from __future__ import annotations

import base64
import json
import logging
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple

import httpx

logger = logging.getLogger("sanjaya")


def _b64url_decode(data: str) -> bytes:
    padding = '=' * (-len(data) % 4)
    return base64.urlsafe_b64decode(data + padding)


def jwt_expiry_unverified(jwt_token: str) -> Optional[float]:
    """Extract exp (epoch seconds) from JWT without verifying signature."""
    try:
        parts = jwt_token.split(".")
        if len(parts) < 2:
            return None
        payload = json.loads(_b64url_decode(parts[1]).decode("utf-8"))
        exp = payload.get("exp")
        if isinstance(exp, (int, float)):
            return float(exp)
        return None
    except Exception:
        return None


@dataclass
class SanjayaAuth:
    access_token: str
    exp_epoch: Optional[float]


class SanjayaAPI:
    def __init__(self, base_url: str, timeout_s: float = 30.0, debug_http: bool = False):
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self.debug_http = debug_http
        self._auth: Optional[SanjayaAuth] = None

        self._username = os.environ.get("SANJAYA_USERNAME", "").strip()
        self._password = os.environ.get("SANJAYA_PASSWORD", "").strip()
        self._source = os.environ.get("SANJAYA_SOURCE", "string").strip()

        if not self._username or not self._password:
            logger.warning(
                "SANJAYA_USERNAME / SANJAYA_PASSWORD not set. "
                "Calls that require auth will fail until you export them."
            )

    async def login(self) -> SanjayaAuth:
        if not self._username or not self._password:
            raise RuntimeError("Missing SANJAYA_USERNAME or SANJAYA_PASSWORD env vars")

        payload = {
            "source": self._source,
            "name": self._username,
            "password": self._password,
        }
        data = await self._request_json(
            method="POST",
            path="/api/v1/master_fm/user/login",
            json_body=payload,
            headers={},
            params=None,
            auth_required=False,
        )

        token = data.get("access_token")
        if not token:
            raise RuntimeError("Login succeeded but access_token missing in response")

        exp = jwt_expiry_unverified(token)
        self._auth = SanjayaAuth(access_token=token, exp_epoch=exp)
        return self._auth

    def _token_valid(self) -> bool:
        if not self._auth:
            return False
        if not self._auth.exp_epoch:
            # If server didn't provide exp, assume short validity and refresh often
            return True
        # refresh 60s early
        return time.time() < (self._auth.exp_epoch - 60)

    async def ensure_token(self) -> str:
        if self._token_valid():
            return self._auth.access_token  # type: ignore[union-attr]
        auth = await self.login()
        return auth.access_token

    async def get_clients(self) -> List[Dict[str, Any]]:
        """Fetch all clients from the API.
        
        Returns:
            List of client dictionaries, each containing:
            - fm_client_name: The client name (e.g., "10.27-gg")
            - display_name: Display name (e.g., "10.27")
            - fm_client_id: The client ID (e.g., 197)
            - fm_client_country: Country name
            - fm_client_image: Base64 encoded image
            - fm_client_logo_url: Logo URL
            - archival_state: Boolean
            - updated_by: Username
            - timezone: Timezone string
        """
        token = await self.ensure_token()
        headers = {"x-user-token": token}
        
        return await self._request_json(
            method="GET",
            path="/api/v1/master_fm/clients",
            params=None,
            headers=headers,
            json_body=None,
            auth_required=True,
        )

    async def get_client_by_id(self, fm_client_id: int) -> Dict[str, Any]:
        """Fetch client details by client ID.
        
        Args:
            fm_client_id: The client ID (e.g., 214)
            
        Returns:
            Client dictionary containing:
            - fm_client_name: The client name (e.g., "Timezone-admin")
            - display_name: Display name (e.g., "Timezone")
            - fm_client_id: The client ID
            - fm_client_country: Country name
            - fm_client_image: Base64 encoded image
            - fm_client_logo_url: Logo URL
            - fm_fleet_names: Array of fleet names for this client
            - other_info: Dictionary with fm_tag and compatible_sherpa_tags
            - updated_by: Username
            - timezone: Timezone string
        """
        token = await self.ensure_token()
        headers = {"x-user-token": token}
        
        return await self._request_json(
            method="GET",
            path=f"/api/v1/master_fm/clients/{fm_client_id}",
            params=None,
            headers=headers,
            json_body=None,
            auth_required=True,
        )

    async def get_sherpas_by_client_id(self, client_id: int) -> List[Dict[str, Any]]:
        """Fetch all sherpas for a given client ID.
        
        Args:
            client_id: The client ID (e.g., 214)
            
        Returns:
            List of sherpa dictionaries, each containing:
            - sherpa_name: The sherpa name (e.g., "tug-51-ceat-nagpur-05")
            - sherpa_hwid: Hardware ID
            - fleet_name: The fleet name (e.g., "CEAT-Nagpur-North-Plant")
        """
        token = await self.ensure_token()
        headers = {"x-user-token": token}
        
        return await self._request_json(
            method="GET",
            path=f"/api/v1/master_fm/sherpas/{client_id}",
            params=None,
            headers=headers,
            json_body=None,
            auth_required=True,
        )

    async def get_sherpas_by_fleet_id(self, fleet_id: int) -> List[Dict[str, Any]]:
        """Fetch all sherpas for a given fleet ID.
        
        Args:
            fleet_id: The fleet ID (e.g., 21)
            
        Returns:
            List of sherpa dictionaries, each containing sherpa information
        """
        token = await self.ensure_token()
        headers = {"x-user-token": token}
        
        return await self._request_json(
            method="GET",
            path=f"/api/v1/master_fm/sherpas/{fleet_id}",
            params=None,
            headers=headers,
            json_body=None,
            auth_required=True,
        )

    async def get_sherpa_names_for_fleet(
        self, client_name: str, fleet_name: str
    ) -> Optional[List[str]]:
        """Resolve client_name + fleet_name to list of sherpa names for basic_analytics.
        Same logic as MCP _resolve_sherpa_names_for_fleet so DAG and chat get same API response.
        """
        all_clients = await self.get_clients()
        client_id = None
        for c in all_clients:
            if isinstance(c, dict) and (c.get("fm_client_name") or "").lower() == client_name.lower():
                client_id = c.get("fm_client_id")
                break
        if not client_id:
            for c in all_clients:
                if isinstance(c, dict):
                    c_name = (c.get("fm_client_name") or "").lower()
                    if client_name.lower() in c_name or c_name in client_name.lower():
                        client_id = c.get("fm_client_id")
                        break
        if not client_id:
            return None
        all_sherpas = await self.get_sherpas_by_client_id(client_id)
        matching = [
            s
            for s in all_sherpas
            if isinstance(s, dict) and (s.get("fleet_name") or "").lower() == fleet_name.lower()
        ]
        names = [s.get("sherpa_name") for s in matching if s.get("sherpa_name")]
        return names if names else None

    async def basic_analytics(
        self,
        fm_client_name: str,
        start_time: str,
        end_time: str,
        timezone: str,
        fleet_name: str | List[str],
        status: List[str],
        sherpa_name: Optional[str] | List[str] = None,
    ) -> Dict[str, Any]:
        params: Dict[str, Any] = {
            "start_time": start_time,
            "end_time": end_time,
            "timezone": timezone,
            "fleet_name": fleet_name,
            "status": status,
        }
        # API requires sherpa_name parameter (even if empty for "all sherpas")
        # API accepts sherpa_name as:
        # - A string (single sherpa)
        # - A list of strings (multiple sherpas)
        # - Empty string "" (all sherpas - API requires the parameter)
        if sherpa_name is None:
            # None means we want all sherpas - send empty string (API requires the parameter)
            params["sherpa_name"] = ""
        elif isinstance(sherpa_name, str) and sherpa_name.lower() in ("null", "none", ""):
            # Handle string "null"/"none" from NLU - treat as empty (all sherpas)
            params["sherpa_name"] = ""
        elif isinstance(sherpa_name, list):
            # List of sherpa names - pass as is
            params["sherpa_name"] = sherpa_name
        else:
            # Single sherpa name as string
            params["sherpa_name"] = sherpa_name.strip() if sherpa_name else ""

        token = await self.ensure_token()
        headers = {"x-user-token": token}

        return await self._request_json(
            method="GET",
            path=f"/api/v1/master_fm/analytics/basic_analytics/{fm_client_name}",
            params=params,
            headers=headers,
            json_body=None,
            auth_required=True,
        )

    async def route_analytics(
        self,
        fm_client_name: str,
        start_time: str,
        end_time: str,
        timezone: str,
        fleet_name: str | List[str],
        status: List[str],
        sherpa_name: Optional[str] | List[str] = None,
    ) -> Dict[str, Any]:
        """Fetch route analytics including takt time data.
        
        Args:
            fm_client_name: Client name (e.g., "ceat-nagpur")
            start_time: Start time string (e.g., "2026-01-23 00:00:00")
            end_time: End time string (e.g., "2026-01-23 23:59:59")
            timezone: Timezone (e.g., "Asia/Kolkata")
            fleet_name: Fleet name or list of fleet names
            status: List of statuses (e.g., ["succeeded", "failed", "cancelled"])
            sherpa_name: Optional sherpa name or list of sherpa names
            
        Returns:
            Dictionary containing route analytics data including avg_takt_per_sherpa
        """
        params: Dict[str, Any] = {
            "start_time": start_time,
            "end_time": end_time,
            "timezone": timezone,
            "fleet_name": fleet_name,
            "status": status,
        }
        # API requires sherpa_name parameter (even if empty for "all sherpas")
        # API accepts sherpa_name as:
        # - A string (single sherpa)
        # - A list of strings (multiple sherpas)
        # - Empty string "" (all sherpas - API requires the parameter)
        if sherpa_name is None:
            # None means we want all sherpas - send empty string (API requires the parameter)
            params["sherpa_name"] = ""
        elif isinstance(sherpa_name, str) and sherpa_name.lower() in ("null", "none", ""):
            # Handle string "null"/"none" from NLU - treat as empty (all sherpas)
            params["sherpa_name"] = ""
        elif isinstance(sherpa_name, list):
            # List of sherpa names - pass as is
            params["sherpa_name"] = sherpa_name
        else:
            # Single sherpa name as string
            params["sherpa_name"] = sherpa_name.strip() if sherpa_name else ""

        token = await self.ensure_token()
        headers = {"x-user-token": token}

        return await self._request_json(
            method="GET",
            path=f"/api/v1/master_fm/analytics/route_analytics/{fm_client_name}",
            params=params,
            headers=headers,
            json_body=None,
            auth_required=True,
        )

    async def _request_json(
        self,
        method: str,
        path: str,
        params: Optional[Dict[str, Any]],
        headers: Dict[str, str],
        json_body: Optional[Dict[str, Any]],
        auth_required: bool,
    ) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"

        async with httpx.AsyncClient(timeout=self.timeout_s) as client:
            req = client.build_request(method=method, url=url, params=params, headers=headers, json=json_body)
            if self.debug_http:
                logger.info("HTTP %s %s", method, req.url)
                if json_body is not None:
                    logger.info("HTTP JSON BODY: %s", json.dumps(json_body, ensure_ascii=False))
                # Avoid logging tokens
                safe_headers = {k: ("<redacted>" if k.lower() in {"authorization", "x-user-token"} else v) for k, v in req.headers.items()}
                logger.info("HTTP HEADERS: %s", safe_headers)

            resp = await client.send(req)
            if self.debug_http:
                logger.info("=" * 80)
                logger.info("API REQUEST:")
                logger.info("  Method: %s", method)
                logger.info("  URL: %s", req.url)
                if params:
                    logger.info("  Query Params: %s", json.dumps(params, indent=2, ensure_ascii=False))
                if json_body is not None:
                    logger.info("  Request Body: %s", json.dumps(json_body, indent=2, ensure_ascii=False))
                logger.info("  Status: %s", resp.status_code)
                logger.info("  Response Headers: %s", dict(resp.headers))
                
                # Try to parse and pretty-print JSON response
                try:
                    response_json = resp.json()
                    logger.info("  Response JSON:")
                    logger.info("%s", json.dumps(response_json, indent=2, ensure_ascii=False))
                except Exception:
                    logger.info("  Response Text (not JSON): %s", resp.text[:1000])
                logger.info("=" * 80)

            # If 401 and auth_required: retry once after re-login
            if resp.status_code == 401 and auth_required:
                logger.warning("401 Unauthorized. Refreshing token and retrying once...")
                await self.login()
                token = await self.ensure_token()
                headers2 = dict(headers)
                headers2["x-user-token"] = token
                req2 = client.build_request(method=method, url=url, params=params, headers=headers2, json=json_body)
                resp = await client.send(req2)
                if self.debug_http:
                    logger.info("=" * 80)
                    logger.info("API REQUEST (RETRY after 401):")
                    logger.info("  Method: %s", method)
                    logger.info("  URL: %s", req2.url)
                    logger.info("  Status: %s", resp.status_code)
                    try:
                        response_json = resp.json()
                        logger.info("  Response JSON:")
                        logger.info("%s", json.dumps(response_json, indent=2, ensure_ascii=False))
                    except Exception:
                        logger.info("  Response Text (not JSON): %s", resp.text[:1000])
                    logger.info("=" * 80)
            
            # If 422 and missing sherpa_name: API validation issue
            # Try with a placeholder value (some APIs accept "*" or "all" for "any")
            if resp.status_code == 422 and auth_required and method == "GET":
                try:
                    error_json = resp.json()
                    error_detail = error_json.get("detail", [])
                    if isinstance(error_detail, list):
                        for err in error_detail:
                            if isinstance(err, dict) and err.get("loc") == ["query", "sherpa_name"]:
                                logger.warning("422 error: sherpa_name required. API may have inconsistent validation.")
                                # Don't retry automatically - let the error propagate with clear message
                                break
                except Exception:
                    pass

            # Provide better error messages for 4xx/5xx responses
            if not resp.is_success:
                error_detail = ""
                try:
                    error_json = resp.json()
                    error_detail = f" API error: {error_json}"
                except Exception:
                    error_detail = f" Response: {resp.text[:500]}"
                
                # Log the error detail before raising
                # 500 errors are often expected (e.g., non-existent fleet_ids during search)
                # Log them at debug level to reduce noise
                if resp.status_code == 500:
                    logger.debug(f"HTTP {resp.status_code} error{error_detail}")
                else:
                    logger.error(f"HTTP {resp.status_code} error{error_detail}")
                resp.raise_for_status()  # This will raise the HTTPError

            try:
                return resp.json()
            except Exception as e:
                raise RuntimeError(f"Response was not valid JSON: {e}. Raw: {resp.text[:500]}")
