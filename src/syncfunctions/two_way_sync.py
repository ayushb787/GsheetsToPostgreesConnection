"""
Author: Ayush Bhandari
Email: ayushbhandariofficial@gmail.com
"""
import os
from datetime import datetime
import pandas as pd
from src.gsheetsconnection.oauth import authenticate_sheets
from src.postgresconnection.postgres_connection import get_postgres_engine
from loguru import logger
from src.utils.gsheets_curd import fetch_google_sheet_data

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RANGE_NAME = 'Sheet1!A1:J26'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


def convert_to_datetime(datetime_str):
    try:
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


# Sync Google Sheets data to PostgreSQL
def sync_to_postgres(data):
    try:
        # Create DataFrame from fetched data
        df = pd.DataFrame(data[1:], columns=data[0])

        df['last_updated'] = df['last_updated'].apply(lambda x: convert_to_datetime(x) if x else None)

        # Ensure correct data types, especially for 'id'
        if 'id' in df.columns:
            df['id'] = pd.to_numeric(df['id'], errors='coerce')

        # Connect to PostgreSQL and sync data
        engine = get_postgres_engine()
        table_name = 'google_sheet_data'
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        return f"Successfully synced {len(df)} rows to PostgreSQL."
    except Exception as e:
        return str(e)


# Sync PostgreSQL data back to Google Sheets
def sync_to_google_sheets():
    try:
        engine = get_postgres_engine()
        query = "SELECT * FROM google_sheet_data"
        df = pd.read_sql(query, engine)

        # Prepare values for Google Sheets
        values = [df.columns.tolist()] + df.values.tolist()

        sheets = authenticate_sheets()

        body = {'values': values}
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME,
            valueInputOption='RAW', body=body
        ).execute()

        return f"Successfully synced {len(df)} rows to Google Sheets."

    except Exception as e:
        return str(e)


# Two-way sync function
def start_sync():
    # Sync data from Google Sheets to PostgreSQL
    data = fetch_google_sheet_data()
    if isinstance(data, str):  # If data is an error message
        return data
    else:
        postgres_sync_result = sync_to_postgres(data)
        logger.info(postgres_sync_result)

    # Sync data from PostgreSQL back to Google Sheets
    google_sheets_sync_result = sync_to_google_sheets()
    logger.info(google_sheets_sync_result)
