import os
from datetime import datetime
import pandas as pd
from googleapiclient.errors import HttpError
from src.datamodels.model import DataRecord
from src.gsheetsconnection.oauth import authenticate_sheets
from src.postgresconnection.postgres_connection import get_postgres_engine
from loguru import logger

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RANGE_NAME = 'Sheet1!A1:J26'
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']


# Fetch data from Google Sheets
def fetch_google_sheet_data():
    try:
        sheets = authenticate_sheets()
        result = sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        if not values:
            raise Exception("No data found in the Google Sheet.")
        return values
    except Exception as e:
        return str(e)


def convert_to_datetime(datetime_str):
    try:
        return datetime.strptime(datetime_str, '%Y-%m-%d %H:%M:%S')
    except ValueError:
        return None


def fetch_sheets_data():
    if not SPREADSHEET_ID:
        logger.error("Error: SPREADSHEET_ID environment variable is not set.")
        return pd.DataFrame()

    try:
        sheets = authenticate_sheets()
        result = sheets.values().get(spreadsheetId=SPREADSHEET_ID, range=RANGE_NAME).execute()
        values = result.get('values', [])
        if not values:
            return pd.DataFrame(columns=[
                'id', 'first_name', 'last_name', 'status',
                'region', 'sales_rep', 'follow_up', 'notes', 'last_updated'
            ])
        headers = values[0]
        data = values[1:]
        df = pd.DataFrame(data, columns=headers)

        for column, dtype in [('id', int), ('first_name', str), ('last_name', str),
                              ('status', str), ('region', str), ('sales_rep', str),
                              ('follow_up', str), ('notes', str), ('last_updated', str)]:
            if column in df.columns:
                df[column] = df[column].astype(dtype, errors='ignore')
            else:
                df[column] = None

        df['last_updated'] = df['last_updated'].apply(convert_to_datetime)

        df.dropna(subset=['id'], inplace=True)

        logger.info(f"Fetched {len(df)} records from Google Sheets:")
        # print(df)

        return df
    except HttpError as error:
        logger.error(f"Error fetching Sheets data: {error}")
        return pd.DataFrame()


def write_sheets_data(df):
    if df.empty:
        logger.error("Warning: Attempting to write an empty DataFrame to Sheets.")
        return False

    try:
        sheets = authenticate_sheets()
        data = [df.columns.tolist()] + df.fillna('').values.tolist()
        body = {
            'values': data
        }
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(f"Wrote data Sheets")
        return True
    except HttpError as error:
        logger.error(f"Error writing Sheets data: {error}")
        return False


def format_data_for_sheets(record: DataRecord):
    """Prepare data for Google Sheets, formatting dates and datetimes as strings."""
    return [
        record.id,
        record.first_name,
        record.last_name,
        record.status,
        record.region,
        record.sales_rep,
        record.follow_up,
        record.notes,
        record.last_updated.strftime('%Y-%m-%d %H:%M:%S') if record.last_updated else None
    ]


def add_row_to_sheets(record: DataRecord):
    if not all([record.id, record.first_name, record.last_name, record.status,
                record.region, record.sales_rep, record.follow_up, record.notes, record.last_updated]):
        logger.error("Incomplete DataRecord provided.")
        return False

    try:
        sheets = authenticate_sheets()
        values = [format_data_for_sheets(record)]
        body = {
            'values': values
        }
        sheets.values().append(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption='RAW',
            insertDataOption='INSERT_ROWS',
            body=body
        ).execute()
        logger.info(f"Inserted records in Sheets with id: {record.id}")
        return True
    except HttpError as error:
        logger.error(f"Error adding row to Sheets: {error}")
        return False


def update_row_in_sheets(record: DataRecord):
    if not all([record.id, record.first_name, record.last_name, record.status,
                record.region, record.sales_rep, record.follow_up, record.notes, record.last_updated]):
        logger.error("Incomplete DataRecord provided.")
        return False

    try:
        df = fetch_sheets_data()
        if df.empty:
            logger.error("Sheets data is empty.")
            return False

        row_indices = df.index[df['id'] == record.id].tolist()
        if not row_indices:
            logger.error(f"Record with id {record.id} not found.")
            return False

        row_number = row_indices[0] + 2
        sheets = authenticate_sheets()
        values = [format_data_for_sheets(record)]
        body = {
            'values': values
        }
        sheets.values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=f'Sheet1!A{row_number}:J{row_number}',
            valueInputOption='RAW',
            body=body
        ).execute()
        logger.info(f"Updated records in Sheets with id: {record.id}")
        return True
    except HttpError as error:
        logger.error(f"Error updating row in Sheets: {error}")
        return False


def delete_row_from_sheets(record_id: int):
    try:
        df = fetch_sheets_data()
        row_indices = df.index[df['id'] == record_id].tolist()
        if not row_indices:
            print(f"Record with ID {record_id} not found in Sheets.")
            return False
        row_number = row_indices[0] + 2
        sheets = authenticate_sheets()
        sheet_metadata = sheets.get(spreadsheetId=SPREADSHEET_ID).execute()
        sheet_id = sheet_metadata['sheets'][0]['properties']['sheetId']
        sheets.batchUpdate(
            spreadsheetId=SPREADSHEET_ID,
            body={
                "requests": [
                    {
                        "deleteDimension": {
                            "range": {
                                "sheetId": sheet_id,
                                "dimension": "ROWS",
                                "startIndex": row_number - 1,
                                "endIndex": row_number
                            }
                        }
                    }
                ]
            }
        ).execute()
        logger.info(f"Deleted row number {row_number} from Sheets.")
        return True
    except HttpError as error:
        logger.error(f"Error deleting row from Sheets: {error}")
        return False
