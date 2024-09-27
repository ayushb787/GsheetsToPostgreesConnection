"""
Author: Ayush Bhandari
Email: ayushbhandariofficial@gmail.com
"""
from pydantic import BaseModel
from datetime import datetime

class DataRecord(BaseModel):
    id: int
    first_name: str
    last_name: str
    status: str
    region: str
    sales_rep: str
    follow_up: str
    notes: str
    last_updated: datetime
