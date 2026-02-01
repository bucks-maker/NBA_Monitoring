"""Base API client with retry logic and rate limiting."""
import asyncio
import logging
from typing import Optional, Dict, Any
import httpx
from tenacity import (
    retry,
    stop_after_attempt,
    wait_exponential,
    retry_if_exception_type,
    before_sleep_log,
)

logger = logging.getLogger(__name__)


class RateLimitError(Exception):
    """Rate limit error from API."""
    pass


class BaseAPIClient:
    """Base API client with retry logic."""

    def __init__(
        self,
        base_url: str,
        timeout: int = 30,
        max_retries: int = 5,
        initial_wait: float = 1.0,
        max_wait: float = 60.0,
    ):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout
        self.max_retries = max_retries
        self.initial_wait = initial_wait
        self.max_wait = max_wait
        self.client: Optional[httpx.AsyncClient] = None
        self._request_count = 0
        self._last_request_time = 0

    async def __aenter__(self):
        self.client = httpx.AsyncClient(
            timeout=httpx.Timeout(self.timeout),
            headers={
                "User-Agent": "PolymarketCollector/1.0",
                "Accept": "application/json",
            },
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            await self.client.aclose()

    @retry(
        retry=(
            retry_if_exception_type(httpx.HTTPError) |
            retry_if_exception_type(RateLimitError)
        ),
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, min=1, max=60),
        before_sleep=before_sleep_log(logger, logging.WARNING),
    )
    async def _request(
        self,
        method: str,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        data: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Make HTTP request with retry logic."""
        if not self.client:
            raise RuntimeError("Client not initialized. Use async context manager.")

        url = f"{self.base_url}/{endpoint.lstrip('/')}"

        # Simple rate limiting - wait between requests
        await asyncio.sleep(0.5)  # Basic delay between requests

        try:
            response = await self.client.request(
                method=method,
                url=url,
                params=params,
                json=data,
                **kwargs,
            )

            # Check for rate limiting
            if response.status_code == 429:
                # Get retry-after header if available
                retry_after = response.headers.get("Retry-After", "60")
                try:
                    wait_time = int(retry_after)
                except ValueError:
                    wait_time = 60

                logger.warning(f"Rate limited. Waiting {wait_time} seconds...")
                await asyncio.sleep(wait_time)
                raise RateLimitError(f"Rate limited. Retry after {wait_time}s")

            # Check for server errors (5xx)
            if 500 <= response.status_code < 600:
                logger.warning(f"Server error {response.status_code}. Retrying...")
                raise httpx.HTTPError(f"Server error: {response.status_code}")

            response.raise_for_status()

            # Increment request counter
            self._request_count += 1

            # Return JSON response
            return response.json()

        except httpx.JSONDecodeError:
            logger.error(f"Failed to decode JSON from {url}")
            return {}
        except httpx.HTTPError as e:
            logger.error(f"HTTP error for {url}: {e}")
            raise

    async def get(
        self,
        endpoint: str,
        params: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Make GET request."""
        return await self._request("GET", endpoint, params=params, **kwargs)

    async def post(
        self,
        endpoint: str,
        data: Optional[Dict[str, Any]] = None,
        **kwargs,
    ) -> Dict[str, Any]:
        """Make POST request."""
        return await self._request("POST", endpoint, data=data, **kwargs)