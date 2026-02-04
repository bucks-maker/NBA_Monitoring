"""Tests for UserDataCollector."""
import pytest
from pathlib import Path
from polymarket_collector.collectors.user_collector import UserDataCollector


class TestUserDataCollector:
    """Test suite for UserDataCollector."""

    def test_extract_handle_from_url(self, tmp_path):
        """Test handle extraction from various URL formats."""
        collector = UserDataCollector(tmp_path)

        # Test standard URL
        url1 = "https://polymarket.com/@gabagool22"
        assert collector.extract_handle_from_url(url1) == "gabagool22"

        # Test URL with query params
        url2 = "https://polymarket.com/@gabagool22?tab=activity"
        assert collector.extract_handle_from_url(url2) == "gabagool22"

        # Test URL with trailing slash
        url3 = "https://polymarket.com/@testuser/"
        assert collector.extract_handle_from_url(url3) == "testuser"

        # Test URL with additional path
        url4 = "https://polymarket.com/@trader123/portfolio"
        assert collector.extract_handle_from_url(url4) == "trader123"

        # Test invalid URL
        url5 = "https://polymarket.com/markets"
        assert collector.extract_handle_from_url(url5) is None

        # Test empty string
        assert collector.extract_handle_from_url("") is None

    def test_collector_initialization(self, tmp_path):
        """Test collector initialization."""
        collector = UserDataCollector(tmp_path)
        assert collector.output_dir == tmp_path
        assert collector.storage is not None
        assert collector.metadata["total_trades"] == 0
        assert collector.metadata["total_activities"] == 0

    @pytest.mark.asyncio
    async def test_resolve_user_with_invalid_input(self, tmp_path):
        """Test user resolution with invalid input."""
        collector = UserDataCollector(tmp_path)

        # Should raise error when neither profile_url nor handle is provided
        with pytest.raises(ValueError, match="Either profile_url or handle must be provided"):
            await collector.resolve_user(profile_url=None, handle=None)

        # Should raise error when invalid URL is provided
        with pytest.raises(ValueError, match="Could not extract handle"):
            await collector.resolve_user(profile_url="https://polymarket.com/invalid", handle=None)