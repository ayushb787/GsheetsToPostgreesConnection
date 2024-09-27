"""
Author: Ayush Bhandari
Email: ayushbhandariofficial@gmail.com
"""
from pydantic import BaseModel
from typing import Optional
from datetime import datetime
import pytz

# Define the UTC timezone
UTC = pytz.utc


# Function to get current time in UTC
def current_time_utc() -> datetime:
    return datetime.now(UTC)


# Base schema with common fields
class ItemBase(BaseModel):
    first_name: str
    last_name: str
    status: str
    region: str
    sales_rep: str
    follow_up: Optional[str] = None
    notes: Optional[str] = None


# Schema for creating new records
class ItemCreate(ItemBase):
    id: int
    last_updated: datetime = current_time_utc()  # Automatically set to current UTC time


# Schema for updating existing records
class ItemUpdate(BaseModel):
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    status: Optional[str] = None
    region: Optional[str] = None
    sales_rep: Optional[str] = None
    follow_up: Optional[str] = None
    notes: Optional[str] = None
    last_updated: datetime = current_time_utc()  # Automatically set to current UTC time


# Schema for returning data (response model)
class Item(ItemBase):
    id: int
    last_updated: datetime

    class Config:
        orm_mode = True  # This allows parsing SQLAlchemy models directly
