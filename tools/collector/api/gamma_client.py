"""Gamma API client for profile search."""
import logging
from typing import Optional, List
from difflib import SequenceMatcher

from .base import BaseAPIClient
from ..models import Profile, SearchResponse

logger = logging.getLogger(__name__)


class GammaAPIClient(BaseAPIClient):
    """Client for Gamma API (profile search)."""

    def __init__(self, **kwargs):
        super().__init__(
            base_url="https://gamma-api.polymarket.com",
            **kwargs
        )

    async def search_profiles(self, query: str) -> List[Profile]:
        """Search for user profiles by handle/name."""
        logger.info(f"Searching for profile: {query}")

        response = await self.get(
            "/public-search",
            params={
                "q": query,
                "search_profiles": "true",
            },
        )

        # Parse response
        search_response = SearchResponse(**response)
        profiles = search_response.profiles

        logger.info(f"Found {len(profiles)} profiles for query: {query}")
        return profiles

    async def resolve_handle_to_wallet(self, handle: str) -> Optional[str]:
        """Resolve a handle to proxy wallet address."""
        # Clean up handle (remove @ if present)
        handle = handle.lstrip("@")

        profiles = await self.search_profiles(handle)

        if not profiles:
            logger.warning(f"No profiles found for handle: {handle}")
            return None

        # Find best matching profile
        best_profile = self._find_best_matching_profile(handle, profiles)

        if best_profile:
            logger.info(f"Resolved handle '{handle}' to wallet: {best_profile.proxyWallet}")
            return best_profile.proxyWallet

        logger.warning(f"Could not find exact match for handle: {handle}")
        return None

    def _find_best_matching_profile(
        self, handle: str, profiles: List[Profile]
    ) -> Optional[Profile]:
        """Find the best matching profile from search results."""
        if not profiles:
            return None

        # First, try exact match on name
        for profile in profiles:
            if profile.name and profile.name.lower() == handle.lower():
                logger.info(f"Found exact match on name: {profile.name}")
                return profile

        # Second, try exact match on pseudonym
        for profile in profiles:
            if profile.pseudonym and profile.pseudonym.lower() == handle.lower():
                logger.info(f"Found exact match on pseudonym: {profile.pseudonym}")
                return profile

        # Third, use similarity matching
        best_match = None
        best_score = 0.0

        for profile in profiles:
            # Calculate similarity scores
            name_score = 0.0
            if profile.name:
                name_score = SequenceMatcher(
                    None, handle.lower(), profile.name.lower()
                ).ratio()

            pseudonym_score = 0.0
            if profile.pseudonym:
                pseudonym_score = SequenceMatcher(
                    None, handle.lower(), profile.pseudonym.lower()
                ).ratio()

            # Check if handle is contained in bio
            bio_contains = 0.0
            if profile.bio and handle.lower() in profile.bio.lower():
                bio_contains = 0.5  # Bonus for bio mention

            # Get the best score for this profile
            profile_score = max(name_score, pseudonym_score) + bio_contains

            if profile_score > best_score:
                best_score = profile_score
                best_match = profile

        if best_match and best_score > 0.7:  # Threshold for similarity
            logger.info(
                f"Found best match with score {best_score:.2f}: "
                f"{best_match.name or best_match.pseudonym}"
            )
            return best_match

        # If no good match, return the first one with a warning
        if profiles:
            logger.warning(
                f"No good match found (best score: {best_score:.2f}). "
                f"Using first result: {profiles[0].name or profiles[0].pseudonym}"
            )
            return profiles[0]

        return None