"""Pydantic models for Polymarket API responses."""
from datetime import datetime
from typing import Optional, List, Dict, Any
from decimal import Decimal
from pydantic import BaseModel, Field


class Profile(BaseModel):
    """User profile from Gamma API."""
    name: Optional[str] = None
    pseudonym: Optional[str] = None
    bio: Optional[str] = None
    proxyWallet: str = Field(..., alias="proxy_wallet")
    # Additional fields from API
    extra_fields: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        populate_by_name = True
        extra = "allow"


class SearchResponse(BaseModel):
    """Search response from Gamma API."""
    profiles: List[Profile] = Field(default_factory=list)

    class Config:
        extra = "allow"


class Trade(BaseModel):
    """Trade data from Data API."""
    timestamp: datetime
    transaction_hash: str = Field(alias="transactionHash")
    condition_id: str = Field(alias="conditionId")
    slug: Optional[str] = None
    event_slug: Optional[str] = Field(None, alias="eventSlug")
    outcome: Optional[str] = None
    outcome_index: Optional[int] = Field(None, alias="outcomeIndex")
    side: str  # BUY or SELL
    size: Decimal
    price: Decimal
    usdc_size: Optional[Decimal] = Field(None, alias="usdcSize")
    proxy_wallet: Optional[str] = Field(None, alias="proxyWallet")
    # Additional fields
    extra_fields: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        populate_by_name = True
        extra = "allow"


class Activity(BaseModel):
    """Activity data from Data API."""
    timestamp: datetime
    type: str  # TRADE, SPLIT, MERGE, REDEEM, REWARD, CONVERSION
    condition_id: Optional[str] = Field(None, alias="conditionId")
    transaction_hash: Optional[str] = Field(None, alias="transactionHash")
    side: Optional[str] = None
    size: Optional[Decimal] = None
    usdc_size: Optional[Decimal] = Field(None, alias="usdcSize")
    # Additional fields
    extra_fields: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        populate_by_name = True
        extra = "allow"


class Position(BaseModel):
    """Open position data from Data API."""
    condition_id: str = Field(alias="conditionId")
    slug: Optional[str] = None
    event_slug: Optional[str] = Field(None, alias="eventSlug")
    outcome: Optional[str] = None
    outcome_index: Optional[int] = Field(None, alias="outcomeIndex")
    size: Decimal
    average_price: Optional[Decimal] = Field(None, alias="averagePrice")
    usdc_value: Optional[Decimal] = Field(None, alias="usdcValue")
    unrealized_pnl: Optional[Decimal] = Field(None, alias="unrealizedPnl")
    # Additional fields
    extra_fields: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        populate_by_name = True
        extra = "allow"


class ClosedPosition(BaseModel):
    """Closed position data from Data API."""
    condition_id: str = Field(alias="conditionId")
    slug: Optional[str] = None
    event_slug: Optional[str] = Field(None, alias="eventSlug")
    outcome: Optional[str] = None
    outcome_index: Optional[int] = Field(None, alias="outcomeIndex")
    size: Optional[Decimal] = None  # Optional - not always present in API
    average_price: Optional[Decimal] = Field(None, alias="averagePrice")
    total_bought: Optional[Decimal] = Field(None, alias="totalBought")  # Alternative to size
    realized_pnl: Optional[Decimal] = Field(None, alias="realizedPnl")
    close_timestamp: Optional[datetime] = Field(None, alias="closeTimestamp")
    timestamp: Optional[int] = None  # Unix timestamp
    # Additional fields
    extra_fields: Dict[str, Any] = Field(default_factory=dict)

    class Config:
        populate_by_name = True
        extra = "allow"