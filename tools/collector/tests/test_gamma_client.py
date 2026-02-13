"""Tests for GammaAPIClient."""
import pytest
from polymarket_collector.api.gamma_client import GammaAPIClient
from polymarket_collector.models import Profile


class TestGammaAPIClient:
    """Test suite for GammaAPIClient."""

    def test_find_best_matching_profile_exact_name(self):
        """Test exact name matching."""
        client = GammaAPIClient()

        profiles = [
            Profile(name="gabagool22", proxyWallet="0xabc123"),
            Profile(name="gabagool", proxyWallet="0xdef456"),
        ]

        result = client._find_best_matching_profile("gabagool22", profiles)
        assert result is not None
        assert result.proxyWallet == "0xabc123"

    def test_find_best_matching_profile_exact_pseudonym(self):
        """Test exact pseudonym matching."""
        client = GammaAPIClient()

        profiles = [
            Profile(name="User1", pseudonym="trader123", proxyWallet="0xabc123"),
            Profile(name="User2", pseudonym="trader456", proxyWallet="0xdef456"),
        ]

        result = client._find_best_matching_profile("trader123", profiles)
        assert result is not None
        assert result.proxyWallet == "0xabc123"

    def test_find_best_matching_profile_similarity(self):
        """Test similarity-based matching."""
        client = GammaAPIClient()

        profiles = [
            Profile(name="gabagoolster", proxyWallet="0xabc123"),
            Profile(name="completelydifferent", proxyWallet="0xdef456"),
        ]

        # Should match the more similar one
        result = client._find_best_matching_profile("gabagool", profiles)
        assert result is not None
        assert result.proxyWallet == "0xabc123"

    def test_find_best_matching_profile_bio_bonus(self):
        """Test bio mention bonus."""
        client = GammaAPIClient()

        profiles = [
            Profile(name="User1", bio="I am gabagool22", proxyWallet="0xabc123"),
            Profile(name="User2", bio="Random bio", proxyWallet="0xdef456"),
        ]

        result = client._find_best_matching_profile("gabagool22", profiles)
        assert result is not None
        assert result.proxyWallet == "0xabc123"

    def test_find_best_matching_profile_empty_list(self):
        """Test with empty profile list."""
        client = GammaAPIClient()
        result = client._find_best_matching_profile("test", [])
        assert result is None

    def test_find_best_matching_profile_low_similarity(self):
        """Test with low similarity scores."""
        client = GammaAPIClient()

        profiles = [
            Profile(name="zzz", proxyWallet="0xabc123"),
            Profile(name="xxx", proxyWallet="0xdef456"),
        ]

        # Should still return first result even with low score
        result = client._find_best_matching_profile("gabagool22", profiles)
        assert result is not None  # Returns first as fallback