"""
Author: Ayush Bhandari
Email: ayushbhandariofficial@gmail.com
"""
import asyncio
from loguru import logger
from src.utils.gsheets_curd import fetch_sheets_data, add_row_to_sheets, update_row_in_sheets, \
    delete_row_from_sheets
from src.utils.postgres_curd import fetch_postgres_data,  delete_postgres_record, upsert_postgres_record
from src.datamodels.model import DataRecord

SYNC_INTERVAL = 15


def sync_all():
    try:
        # Step 1: Initial Fetch
        sheets_df = fetch_sheets_data()
        postgres_df = fetch_postgres_data()

        logger.info("Fetched data from Google Sheets and PostgreSQL.")

        # Handle empty DataFrames
        if sheets_df.empty and postgres_df.empty:
            logger.info("No data to sync.")
            return "No data to sync."

        # Ensure that 'id' is the key
        if not sheets_df.empty:
            sheets_df.set_index('id', inplace=True)
        if not postgres_df.empty:
            postgres_df.set_index('id', inplace=True)

        # Get sets of IDs in both Google Sheets and PostgreSQL
        sheets_ids = set(sheets_df.index.tolist()) if not sheets_df.empty else set()
        postgres_ids = set(postgres_df.index.tolist()) if not postgres_df.empty else set()

        # Step 2: Handle Insertions and Updates from Google Sheets to PostgreSQL
        common_ids = sheets_ids & postgres_ids
        sheets_only_ids = sheets_ids - postgres_ids
        postgres_only_ids = postgres_ids - sheets_ids

        for idx in common_ids:
            sheets_row = sheets_df.loc[idx]
            postgres_row = postgres_df.loc[idx]

            # print(sheets_row['last_updated'])
            # print(postgres_row['last_updated'])
            if sheets_row['last_updated'] > postgres_row['last_updated']:
                # Sheets has newer data; update PostgreSQL
                record = DataRecord(
                    id=idx,
                    first_name=sheets_row['first_name'],
                    last_name=sheets_row['last_name'],
                    status=sheets_row['status'],
                    region=sheets_row['region'],
                    sales_rep=sheets_row['sales_rep'],
                    follow_up=sheets_row['follow_up'],
                    notes=sheets_row['notes'],
                    last_updated=sheets_row['last_updated']
                )
                upsert_postgres_record(record)
                logger.info(f"Upserted record ID {record.id} in PostgreSQL from Sheets.")

            elif postgres_row['last_updated'] > sheets_row['last_updated']:
                # PostgreSQL has newer data; update Google Sheets
                record = DataRecord(
                    id=idx,
                    first_name=postgres_row['first_name'],
                    last_name=postgres_row['last_name'],
                    status=postgres_row['status'],
                    region=postgres_row['region'],
                    sales_rep=postgres_row['sales_rep'],
                    follow_up=postgres_row['follow_up'],
                    notes=postgres_row['notes'],
                    last_updated=postgres_row['last_updated']
                )
                update_row_in_sheets(record)
                logger.info(f"Updated record ID {record.id} in Google Sheets from PostgreSQL.")

        # Handle new records from Google Sheets
        for idx in sheets_only_ids:
            sheets_row = sheets_df.loc[idx]
            record = DataRecord(
                id=idx,
                first_name=sheets_row['first_name'],
                last_name=sheets_row['last_name'],
                status=sheets_row['status'],
                region=sheets_row['region'],
                sales_rep=sheets_row['sales_rep'],
                follow_up=sheets_row['follow_up'],
                notes=sheets_row['notes'],
                last_updated=sheets_row['last_updated']
            )
            upsert_postgres_record(record)
            logger.info(f"Inserted new record ID {record.id} into PostgreSQL from Sheets.")

        # Handle new records from PostgreSQL
        for idx in postgres_only_ids:
            postgres_row = postgres_df.loc[idx]
            record = DataRecord(
                id=idx,
                first_name=postgres_row['first_name'],
                last_name=postgres_row['last_name'],
                status=postgres_row['status'],
                region=postgres_row['region'],
                sales_rep=postgres_row['sales_rep'],
                follow_up=postgres_row['follow_up'],
                notes=postgres_row['notes'],
                last_updated=postgres_row['last_updated']
            )
            add_row_to_sheets(record)
            logger.info(f"Added new record ID {record.id} to Google Sheets from PostgreSQL.")

        # Step 3: Re-fetch Data After Insertions/Updates
        sheets_df_updated = fetch_sheets_data()
        postgres_df_updated = fetch_postgres_data()

        logger.info("Re-fetched updated data from Google Sheets and PostgreSQL after insertions/updates.")

        # Ensure 'id' is the key
        if not sheets_df_updated.empty:
            sheets_df_updated.set_index('id', inplace=True)
        if not postgres_df_updated.empty:
            postgres_df_updated.set_index('id', inplace=True)

        # Step 4: Deletions
        # Find records present in PostgreSQL but not in Google Sheets -> delete from PostgreSQL
        deleted_in_sheets = postgres_ids - sheets_ids
        for idx in deleted_in_sheets:
            delete_postgres_record(idx)
            logger.info(f"Deleted record ID {idx} from PostgreSQL.")

        # Find records present in Google Sheets but not in PostgreSQL -> delete from Google Sheets
        deleted_in_postgres = sheets_ids - postgres_ids
        for idx in deleted_in_postgres:
            delete_row_from_sheets(idx)
            logger.info(f"Deleted record ID {idx} from Google Sheets.")

        logger.info("Synchronization complete.")
        return "Synchronization complete."

    except KeyError as e:
        logger.error(f"Synchronization failed: Missing key {e}")
        return f"Synchronization failed: Missing key {e}"
    except Exception as e:
        logger.error(f"Synchronization failed: {e}")
        return f"Synchronization failed: {e}"



async def background_sync_task():
    while True:
        result = sync_all()
        logger.info(result)
        await asyncio.sleep(SYNC_INTERVAL)


def start_background_sync(app):
    asyncio.create_task(background_sync_task())
