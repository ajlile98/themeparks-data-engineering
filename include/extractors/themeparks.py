"""
Themeparks API Client

A simple async client to query data from the themeparks.wiki API.
API Documentation: https://api.themeparks.wiki/docs

Usage:
    # Async context (recommended for multiple parks)
    async with ThemeparksClient() as client:
        results = await client.get_multiple_parks_live(park_ids)

    # Simple sync usage (one-off scripts)
    import asyncio
    asyncio.run(main())
"""

import httpx
import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class ThemeParksClient:
    """
    Async client for the Themeparks API.
    
    Designed for efficient parallel data collection across multiple parks.
    """
    
    BASE_URL = "https://api.themeparks.wiki/v1"
    
    def __init__(self, timeout: float = 30.0):
        self._client: httpx.AsyncClient | None = None
        self._timeout = timeout
    
    async def __aenter__(self):
        self._client = httpx.AsyncClient(timeout=self._timeout)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None
    
    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError("Client not initialized. Use 'async with ThemeparksClient() as client:'")
        return self._client
    
    # -------------------------------------------------------------------------
    # API Methods
    # -------------------------------------------------------------------------
    
    async def get_destinations(self) -> dict:
        """Get all available destinations (theme park resorts)."""
        response = await self.client.get(f"{self.BASE_URL}/destinations")
        response.raise_for_status()
        return response.json()
    
    async def get_destination(self, destination_id: str) -> dict:
        """Get details for a specific destination."""
        response = await self.client.get(f"{self.BASE_URL}/destinations/{destination_id}")
        response.raise_for_status()
        return response.json()
    
    async def get_entity(self, entity_id: str) -> dict:
        """Get details for a specific entity (park, attraction, etc.)."""
        response = await self.client.get(f"{self.BASE_URL}/entity/{entity_id}")
        response.raise_for_status()
        return response.json()
    
    async def get_entity_children(self, entity_id: str) -> dict:
        """Get children of an entity (e.g., attractions within a park)."""
        response = await self.client.get(f"{self.BASE_URL}/entity/{entity_id}/children")
        response.raise_for_status()
        return response.json()
    
    async def get_entity_live(self, entity_id: str) -> dict:
        """Get live data for an entity (wait times, show times, etc.)."""
        response = await self.client.get(f"{self.BASE_URL}/entity/{entity_id}/live")
        response.raise_for_status()
        return response.json()
    
    async def get_entity_schedule(
        self, 
        entity_id: str, 
        year: Optional[int] = None, 
        month: Optional[int] = None
    ) -> dict:
        """Get schedule/calendar data for an entity."""
        url = f"{self.BASE_URL}/entity/{entity_id}/schedule"
        params = {}
        if year:
            params["year"] = year
        if month:
            params["month"] = month
        response = await self.client.get(url, params=params)
        response.raise_for_status()
        return response.json()
    
    # -------------------------------------------------------------------------
    # Batch Methods (the real power of async)
    # -------------------------------------------------------------------------
    
    async def get_multiple_parks_live(self, park_ids: list[str]) -> dict[str, dict]:
        """
        Fetch live data for multiple parks in parallel.
        
        Returns:
            Dict mapping park_id -> live_data (or error info)
        """
        logger.debug(f"Fetching live data for {len(park_ids)} parks in parallel")
        tasks = [self.get_entity_live(park_id) for park_id in park_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        output = {}
        errors = 0
        for park_id, result in zip(park_ids, results):
            if isinstance(result, Exception):
                logger.error(f"Failed to fetch park {park_id}: {result}")
                output[park_id] = {"error": str(result), "success": False}
                errors += 1
            else:
                result["success"] = True
                output[park_id] = result
        
        if errors > 0:
            logger.warning(f"Failed to fetch {errors}/{len(park_ids)} parks")
        else:
            logger.debug(f"Successfully fetched all {len(park_ids)} parks")
        
        return output
    
    async def get_all_park_ids(self) -> list[str]:
        """Fetch all park IDs from all destinations."""
        logger.debug("Fetching all park IDs from destinations")
        destinations = await self.get_destinations()
        park_ids = []
        for dest in destinations.get("destinations", []):
            for park in dest.get("parks", []):
                if park_id := park.get("id"):
                    park_ids.append(park_id)
        logger.info(f"Found {len(park_ids)} total parks across all destinations")
        return park_ids


# -----------------------------------------------------------------------------
# Convenience functions for simple sync usage
# -----------------------------------------------------------------------------

async def fetch_parks_live(park_ids: list[str]) -> dict[str, dict]:
    """
    One-liner to fetch multiple parks.
    
    Usage:
        results = asyncio.run(fetch_parks_live(["park-id-1", "park-id-2"]))
    """
    async with ThemeparksClient() as client:
        return await client.get_multiple_parks_live(park_ids)


async def fetch_all_parks_live() -> dict[str, dict]:
    """
    Fetch live data for ALL parks.
    
    Usage:
        results = asyncio.run(fetch_all_parks_live())
    """
    async with ThemeparksClient() as client:
        park_ids = await client.get_all_park_ids()
        logger.info(f"Fetching live data for {len(park_ids)} parks...")
        return await client.get_multiple_parks_live(park_ids)


# -----------------------------------------------------------------------------
# Demo
# -----------------------------------------------------------------------------

async def demo():
    """Demo: Fetch and display theme park data."""
    import time
    
    print("=" * 60)
    print("THEMEPARKS API - Async Demo")
    print("=" * 60)
    
    async with ThemeParksClient() as client:
        # Get all destinations
        print("\nFetching all destinations...")
        destinations = await client.get_destinations()
        print(f"Found {len(destinations.get('destinations', []))} destinations\n")
        
        # Get Disney World park IDs
        disney_parks = {
            "Magic Kingdom": "75ea578a-adc8-4116-a54d-dccb60765ef9",
            "EPCOT": "47f90d2c-e191-4239-a466-5892ef59a88b",
            "Hollywood Studios": "288747d1-8b4f-4a64-867e-ea7c9571a440",
            "Animal Kingdom": "1c84a229-8862-4648-9c71-378ddd2c7693",
        }
        
        print("Fetching ALL Disney World parks in parallel...")
        start = time.perf_counter()
        
        results = await client.get_multiple_parks_live(list(disney_parks.values()))
        
        elapsed = time.perf_counter() - start
        print(f"Fetched {len(results)} parks in {elapsed:.2f}s\n")
        
        # Show results
        for park_name, park_id in disney_parks.items():
            data = results.get(park_id, {})
            if data.get("success"):
                live_items = data.get("liveData", [])
                operating = sum(1 for item in live_items if item.get("status") == "OPERATING")
                print(f"  {park_name}: {operating} operating / {len(live_items)} total")
            else:
                print(f"  {park_name}: Error - {data.get('error')}")
    
    print("\n" + "=" * 60)
    print("Demo complete!")
    print("=" * 60)


def main():
    """Entry point for sync execution."""
    asyncio.run(demo())


if __name__ == "__main__":
    main()
