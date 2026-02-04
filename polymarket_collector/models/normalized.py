"""Normalized data models for storage and analysis."""
from datetime import datetime
from typing import Optional
from decimal import Decimal
from pydantic import BaseModel, Field


class NormalizedTrade(BaseModel):
    """Normalized trade data for storage."""
    timestamp: datetime
    transaction_hash: str
    condition_id: str
    slug: Optional[str] = None
    event_slug: Optional[str] = None
    outcome: Optional[str] = None
    outcome_index: Optional[int] = None
    side: str  # BUY or SELL
    size: float  # Convert from Decimal for storage
    price: float
    usdc_size: Optional[float] = None
    proxy_wallet: str


class NormalizedActivity(BaseModel):
    """Normalized activity data for storage."""
    timestamp: datetime
    type: str
    condition_id: Optional[str] = None
    transaction_hash: Optional[str] = None
    side: Optional[str] = None
    size: Optional[float] = None
    usdc_size: Optional[float] = None
    proxy_wallet: str


class NormalizedPosition(BaseModel):
    """Normalized position data for storage."""
    condition_id: str
    slug: Optional[str] = None
    event_slug: Optional[str] = None
    outcome: Optional[str] = None
    outcome_index: Optional[int] = None
    size: float
    average_price: Optional[float] = None
    usdc_value: Optional[float] = None
    unrealized_pnl: Optional[float] = None
    realized_pnl: Optional[float] = None
    is_closed: bool = False
    close_timestamp: Optional[datetime] = None
    proxy_wallet: str