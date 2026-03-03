"""TTL-based caching for API responses."""

from __future__ import annotations

import asyncio
import logging
import time
from typing import Any, Dict, Optional, TypeVar

logger = logging.getLogger("cache")

T = TypeVar("T")


class TTLCache:
    """Thread-safe TTL cache for async operations.
    
    Example:
        cache = TTLCache(ttl_seconds=300)  # 5 minutes
        value = await cache.get_or_set("key", fetch_function)
    """
    
    def __init__(self, ttl_seconds: float = 300.0, max_size: Optional[int] = None):
        """Initialize TTL cache.
        
        Args:
            ttl_seconds: Time-to-live in seconds (default: 5 minutes)
            max_size: Maximum number of entries (None = unlimited)
        """
        self.ttl_seconds = ttl_seconds
        self.max_size = max_size
        self._cache: Dict[str, tuple[float, T]] = {}
        self._lock = asyncio.Lock()
    
    async def get(self, key: str) -> Optional[T]:
        """Get value from cache if it exists and hasn't expired.
        
        Args:
            key: Cache key
            
        Returns:
            Cached value or None if not found/expired
        """
        async with self._lock:
            if key not in self._cache:
                return None
            
            timestamp, value = self._cache[key]
            if time.time() - timestamp > self.ttl_seconds:
                # Expired, remove it
                del self._cache[key]
                logger.debug(f"Cache expired for key: {key}")
                return None
            
            return value
    
    async def set(self, key: str, value: T) -> None:
        """Set value in cache.
        
        Args:
            key: Cache key
            value: Value to cache
        """
        async with self._lock:
            # Check max size
            if self.max_size and len(self._cache) >= self.max_size:
                # Remove oldest entry (simple FIFO)
                oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k][0])
                del self._cache[oldest_key]
                logger.debug(f"Cache full, evicted key: {oldest_key}")
            
            self._cache[key] = (time.time(), value)
            logger.debug(f"Cached value for key: {key}")
    
    async def get_or_set(self, key: str, fetch_func: callable, *args, **kwargs) -> T:
        """Get value from cache, or fetch and cache it if not present/expired.
        
        Args:
            key: Cache key
            fetch_func: Async function to fetch the value if not cached
            *args, **kwargs: Arguments to pass to fetch_func
            
        Returns:
            Cached or freshly fetched value
        """
        # Try to get from cache
        cached = await self.get(key)
        if cached is not None:
            logger.debug(f"Cache hit for key: {key}")
            return cached
        
        # Cache miss, fetch the value
        logger.debug(f"Cache miss for key: {key}, fetching...")
        value = await fetch_func(*args, **kwargs)
        await self.set(key, value)
        return value
    
    async def clear(self) -> None:
        """Clear all cached entries."""
        async with self._lock:
            count = len(self._cache)
            self._cache.clear()
            logger.info(f"Cleared cache ({count} entries)")
    
    async def invalidate(self, key: str) -> None:
        """Remove a specific key from cache.
        
        Args:
            key: Cache key to remove
        """
        async with self._lock:
            if key in self._cache:
                del self._cache[key]
                logger.debug(f"Invalidated cache key: {key}")
    
    def size(self) -> int:
        """Get current cache size."""
        return len(self._cache)

