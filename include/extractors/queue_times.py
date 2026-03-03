"""
Queue Times API Client

Async client for the queue-times.com API.
API: https://queue-times.com

Endpoints used:
    GET /parks.json                        — all companies and their parks
    GET /parks/{park_id}/queue_times.json  — live wait times for a single park

Usage:
    # Async context (recommended for multiple parks)
    async with QueueTimesClient() as client:
        results = await client.get_multiple_parks_queue_times(park_ids)

    # Sync single-park (for mapped Airflow tasks)
    client = QueueTimesClient()
    data = client.get_park_queue_times_sync(park_id)

Key differences from the old themeparks.wiki API:
    - Park/ride IDs are integers (not UUIDs)
    - Only rides are tracked; no shows, restaurants, etc.
    - Rides are grouped by land within each park
    - A single wait_time int replaces the nested queue struct
    - Geographic data (lat/long/timezone) is included in the parks list
    - No separate entity-children endpoint: ride catalog is embedded in queue_times
"""

import httpx
import asyncio
import logging

logger = logging.getLogger(__name__)


class QueueTimesClient:
    """
    Async client for the queue-times.com API.

    Designed for efficient parallel data collection across multiple parks.
    max_connections caps concurrent sockets to avoid overwhelming the upstream
    API — httpx queues excess requests automatically at the transport layer.
    """

    BASE_URL = "https://queue-times.com"

    def __init__(self, timeout: float = 30.0, max_connections: int = 10):
        self._client: httpx.AsyncClient | None = None
        self._timeout = timeout
        self._max_connections = max_connections

    async def __aenter__(self):
        self._client = httpx.AsyncClient(
            timeout=self._timeout,
            limits=httpx.Limits(max_connections=self._max_connections),
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise RuntimeError(
                "Client not initialized. Use 'async with QueueTimesClient() as client:'"
            )
        return self._client

    # -------------------------------------------------------------------------
    # API Methods
    # -------------------------------------------------------------------------

    async def get_parks(self) -> list[dict]:
        """
        Get all companies and their parks.

        Response shape:
            [
                {
                    "id": 2,
                    "name": "Walt Disney Attractions",
                    "parks": [
                        {
                            "id": 6,
                            "name": "Disney Magic Kingdom",
                            "country": "United States",
                            "continent": "North America",
                            "latitude": "28.417663",
                            "longitude": "-81.581212",
                            "timezone": "America/New_York"
                        },
                        ...
                    ]
                },
                ...
            ]
        """
        response = await self.client.get(f"{self.BASE_URL}/parks.json")
        response.raise_for_status()
        return response.json()

    async def get_park_queue_times(self, park_id: int) -> dict:
        """
        Get live wait times for a single park.

        Response shape:
            {
                "lands": [
                    {
                        "id": 56,
                        "name": "Adventureland",
                        "rides": [
                            {
                                "id": 134,
                                "name": "Jungle Cruise",
                                "is_open": true,
                                "wait_time": 50,
                                "last_updated": "2026-03-03T15:00:24.000Z"
                            },
                            ...
                        ]
                    },
                    ...
                ],
                "rides": [...]  # top-level rides not belonging to any land
            }
        """
        response = await self.client.get(
            f"{self.BASE_URL}/parks/{park_id}/queue_times.json"
        )
        response.raise_for_status()
        return response.json()

    def get_park_queue_times_sync(self, park_id: int) -> dict:
        """
        Synchronous single-call variant — no event loop overhead.
        Use this when fetching one park in a mapped Airflow task
        (Airflow workers provide the parallelism; async buys nothing here).
        """
        with httpx.Client(timeout=self._timeout) as client:
            response = client.get(
                f"{self.BASE_URL}/parks/{park_id}/queue_times.json"
            )
            response.raise_for_status()
            return response.json()

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def extract_rides(park_id: int, queue_data: dict) -> list[dict]:
        """
        Flatten rides from a queue_times response into a list of dicts.

        Rides within lands get land_id and land_name populated.
        Top-level rides (outside any land) get land_id=None, land_name=None.

        Args:
            park_id:    Integer park ID — added to every ride record.
            queue_data: Response dict from get_park_queue_times().

        Returns:
            List of ride dicts with keys:
                park_id, land_id, land_name, id, name,
                is_open, wait_time, last_updated
        """
        rides: list[dict] = []

        for land in queue_data.get("lands", []):
            land_id = land.get("id")
            land_name = land.get("name")
            for ride in land.get("rides", []):
                rides.append({
                    "park_id":    park_id,
                    "land_id":    land_id,
                    "land_name":  land_name,
                    "id":         ride.get("id"),
                    "name":       ride.get("name"),
                    "is_open":    ride.get("is_open"),
                    "wait_time":  ride.get("wait_time"),
                    "last_updated": ride.get("last_updated"),
                })

        # Some parks surface top-level rides not nested inside any land
        for ride in queue_data.get("rides", []):
            rides.append({
                "park_id":    park_id,
                "land_id":    None,
                "land_name":  None,
                "id":         ride.get("id"),
                "name":       ride.get("name"),
                "is_open":    ride.get("is_open"),
                "wait_time":  ride.get("wait_time"),
                "last_updated": ride.get("last_updated"),
            })

        return rides

    # -------------------------------------------------------------------------
    # Batch Methods (the real power of async)
    # -------------------------------------------------------------------------

    async def get_multiple_parks_queue_times(
        self, park_ids: list[int]
    ) -> dict[int, dict]:
        """
        Fetch queue times for multiple parks in parallel.

        Returns:
            Dict mapping park_id -> queue_times data (or error info).
            Error entries have shape: {"error": str, "success": False}
        """
        logger.debug(f"Fetching queue times for {len(park_ids)} parks in parallel")
        tasks = [self.get_park_queue_times(pid) for pid in park_ids]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        output: dict[int, dict] = {}
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

    async def get_all_park_ids(self) -> list[int]:
        """Fetch all park IDs from all companies."""
        logger.debug("Fetching all park IDs")
        companies = await self.get_parks()
        park_ids: list[int] = []
        for company in companies:
            for park in company.get("parks", []):
                if pid := park.get("id"):
                    park_ids.append(pid)
        logger.info(f"Found {len(park_ids)} total parks across all companies")
        return park_ids
