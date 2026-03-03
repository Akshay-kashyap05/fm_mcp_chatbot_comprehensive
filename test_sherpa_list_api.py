#!/usr/bin/env python3
"""Test script to verify API accepts sherpa_name as a list."""

import asyncio
import os
import sys
from datetime import datetime
from dotenv import load_dotenv

# Add src to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from src.sanjaya_client import SanjayaAPI

load_dotenv()

BASE_URL = "https://sanjaya.atimotors.com"


async def test_single_sherpa():
    """Test API call with a single sherpa name (string)."""
    print("\n" + "="*60)
    print("TEST 1: Single sherpa name (string)")
    print("="*60)
    
    client = SanjayaAPI(BASE_URL, debug_http=True)
    
    # Get current time range for today
    now = datetime.now()
    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    end_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        result = await client.basic_analytics(
            fm_client_name="ceat-nagpur",
            start_time=start_time,
            end_time=end_time,
            timezone="Asia/Kolkata",
            fleet_name="CEAT-Nagpur-North-Plant",
            status=["succeeded", "failed", "cancelled"],
            sherpa_name="tug-107-ceat-nagpur-12",  # Single sherpa as string
        )
        print(f"✅ SUCCESS: Got response with total_trips={result.get('total_trips')}")
        print(f"Response keys: {list(result.keys())[:5]}...")
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False


async def test_multiple_sherpas():
    """Test API call with multiple sherpa names (list)."""
    print("\n" + "="*60)
    print("TEST 2: Multiple sherpa names (list)")
    print("="*60)
    
    client = SanjayaAPI(BASE_URL, debug_http=True)
    
    # Get current time range for today
    now = datetime.now()
    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    end_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        result = await client.basic_analytics(
            fm_client_name="ceat-nagpur",
            start_time=start_time,
            end_time=end_time,
            timezone="Asia/Kolkata",
            fleet_name="CEAT-Nagpur-North-Plant",
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=["tug-107-ceat-nagpur-12", "tug-110-ceat-nagpur-14"],  # List of sherpas
        )
        print(f"✅ SUCCESS: Got response with total_trips={result.get('total_trips')}")
        print(f"Response keys: {list(result.keys())[:5]}...")
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False


async def test_empty_sherpa():
    """Test API call with empty sherpa_name (all sherpas)."""
    print("\n" + "="*60)
    print("TEST 3: Empty sherpa_name (all sherpas)")
    print("="*60)
    
    client = SanjayaAPI(BASE_URL, debug_http=True)
    
    # Get current time range for today
    now = datetime.now()
    start_time = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
    end_time = now.strftime("%Y-%m-%d %H:%M:%S")
    
    try:
        result = await client.basic_analytics(
            fm_client_name="ceat-nagpur",
            start_time=start_time,
            end_time=end_time,
            timezone="Asia/Kolkata",
            fleet_name="CEAT-Nagpur-North-Plant",
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=None,  # None -> empty string for all sherpas
        )
        print(f"✅ SUCCESS: Got response with total_trips={result.get('total_trips')}")
        print(f"Response keys: {list(result.keys())[:5]}...")
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False


async def test_fetch_sherpas():
    """Test fetching sherpas by fleet_id."""
    print("\n" + "="*60)
    print("TEST 4: Fetch sherpas by fleet_id")
    print("="*60)
    
    client = SanjayaAPI(BASE_URL, debug_http=True)
    
    fleet_id = int(os.environ.get("SANJAYA_DEFAULT_FLEET_ID", "21"))
    
    try:
        sherpas = await client.get_sherpas_by_fleet_id(fleet_id)
        print(f"✅ SUCCESS: Got {len(sherpas)} sherpas")
        
        # Filter by fleet name
        fleet_name = os.environ.get("SANJAYA_DEFAULT_FLEET", "CEAT-Nagpur-North-Plant")
        matching = [s for s in sherpas if isinstance(s, dict) and s.get("fleet_name") == fleet_name]
        print(f"Found {len(matching)} sherpas matching fleet_name: {fleet_name}")
        
        if matching:
            print("\nFirst few sherpas:")
            for s in matching[:3]:
                print(f"  - {s.get('sherpa_name')}")
        
        return True, matching
    except Exception as e:
        print(f"❌ FAILED: {e}")
        return False, []


async def test_with_fetched_sherpas():
    """Test API call using sherpas fetched from fleet_id."""
    print("\n" + "="*60)
    print("TEST 5: API call with fetched sherpas (list)")
    print("="*60)
    
    client = SanjayaAPI(BASE_URL, debug_http=True)
    
    # First fetch sherpas
    fleet_id = int(os.environ.get("SANJAYA_DEFAULT_FLEET_ID", "21"))
    fleet_name = os.environ.get("SANJAYA_DEFAULT_FLEET", "CEAT-Nagpur-North-Plant")
    
    try:
        all_sherpas = await client.get_sherpas_by_fleet_id(fleet_id)
        matching_sherpas = [
            s for s in all_sherpas 
            if isinstance(s, dict) and s.get("fleet_name") == fleet_name
        ]
        sherpa_names = [s.get("sherpa_name") for s in matching_sherpas if s.get("sherpa_name")]
        
        print(f"Found {len(sherpa_names)} sherpas for fleet {fleet_name}")
        
        if not sherpa_names:
            print("❌ No sherpas found, skipping test")
            return False
        
        # Get current time range for today
        now = datetime.now()
        start_time = now.replace(hour=0, minute=0, second=0, microsecond=0).strftime("%Y-%m-%d %H:%M:%S")
        end_time = now.strftime("%Y-%m-%d %H:%M:%S")
        
        # Test with all fetched sherpas as a list
        result = await client.basic_analytics(
            fm_client_name="ceat-nagpur",
            start_time=start_time,
            end_time=end_time,
            timezone="Asia/Kolkata",
            fleet_name=fleet_name,
            status=["succeeded", "failed", "cancelled"],
            sherpa_name=sherpa_names,  # List of all sherpas
        )
        print(f"✅ SUCCESS: Got response with total_trips={result.get('total_trips')}")
        print(f"Response keys: {list(result.keys())[:5]}...")
        return True
    except Exception as e:
        print(f"❌ FAILED: {e}")
        import traceback
        traceback.print_exc()
        return False


async def main():
    print("Testing Sanjaya API with different sherpa_name formats")
    print("="*60)
    
    # Check environment variables
    if not os.environ.get("SANJAYA_USERNAME") or not os.environ.get("SANJAYA_PASSWORD"):
        print("❌ ERROR: SANJAYA_USERNAME and SANJAYA_PASSWORD must be set")
        return
    
    results = []
    
    # Run tests
    results.append(("Single sherpa (string)", await test_single_sherpa()))
    results.append(("Multiple sherpas (list)", await test_multiple_sherpas()))
    results.append(("Empty sherpa (all)", await test_empty_sherpa()))
    
    success, matching = await test_fetch_sherpas()
    results.append(("Fetch sherpas", success))
    
    if matching:
        results.append(("API with fetched sherpas", await test_with_fetched_sherpas()))
    
    # Summary
    print("\n" + "="*60)
    print("TEST SUMMARY")
    print("="*60)
    for name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status}: {name}")
    
    all_passed = all(r for _, r in results)
    if all_passed:
        print("\n🎉 All tests passed!")
    else:
        print("\n⚠️  Some tests failed")


if __name__ == "__main__":
    asyncio.run(main())

