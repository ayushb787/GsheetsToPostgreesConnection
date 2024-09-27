from fastapi import APIRouter, HTTPException
from src.datamodels.postgres_curd_model import ItemCreate, ItemUpdate, Item
from src.utils.postgres_curd import fetch_postgres_data, insert_postgres_record, update_postgres_record, \
    delete_postgres_record, upsert_postgres_record
from src.datamodels.model import DataRecord
from loguru import logger
from typing import List

router = APIRouter()


# Fetch all records
@router.get("/records", response_model=List[Item])
def get_records():
    try:
        records = fetch_postgres_data()
        if records.empty:
            return []
        return records.to_dict(orient="records")
    except Exception as e:
        logger.error(f"Error fetching records: {e}")
        raise HTTPException(status_code=500, detail="Error fetching records")


# Create a new record (Insert)
@router.post("/records", response_model=Item)
def create_record(record: ItemCreate):
    try:
        data_record = DataRecord(**record.dict())
        success = insert_postgres_record(data_record)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to insert record")
        return record
    except Exception as e:
        logger.error(f"Error inserting record: {e}")
        raise HTTPException(status_code=500, detail="Error inserting record")


# Update an existing record
@router.put("/records/{record_id}", response_model=Item)
def update_record(record_id: int, record: ItemUpdate):
    try:
        existing_record_df = fetch_postgres_data()
        if existing_record_df.empty or record_id not in existing_record_df['id'].values:
            raise HTTPException(status_code=404, detail="Record not found")

        # Ensure last_updated is part of the update
        record_data = record.dict(exclude_unset=True)
        record_data['id'] = record_id
        data_record = DataRecord(**record_data)
        success = update_postgres_record(data_record)

        if not success:
            raise HTTPException(status_code=500, detail="Failed to update record")
        return record_data
    except Exception as e:
        logger.error(f"Error updating record: {e}")
        raise HTTPException(status_code=500, detail="Error updating record")


# Delete a record
@router.delete("/records/{record_id}", response_model=dict)
def delete_record(record_id: int):
    try:
        success = delete_postgres_record(record_id)
        if not success:
            raise HTTPException(status_code=500, detail="Failed to delete record")
        return {"message": f"Record with id {record_id} deleted successfully"}
    except Exception as e:
        logger.error(f"Error deleting record: {e}")
        raise HTTPException(status_code=500, detail="Error deleting record")
