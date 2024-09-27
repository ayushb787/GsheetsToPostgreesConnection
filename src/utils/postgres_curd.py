"""
Author: Ayush Bhandari
Email: ayushbhandariofficial@gmail.com
"""
from sqlalchemy import create_engine, Table, Column, Integer, String, Float, DateTime, MetaData
from sqlalchemy.orm import sessionmaker
from src.postgresconnection.postgres_connection import get_postgres_engine
from src.datamodels.model import DataRecord
import pandas as pd
from sqlalchemy.dialects.postgresql import insert
from loguru import logger

engine = get_postgres_engine()
metadata = MetaData()

data_table = Table('google_sheet_data', metadata,
                   Column('id', Integer, primary_key=True),
                   Column('first_name', String),
                   Column('last_name', String),
                   Column('status', String),
                   Column('region', String),
                   Column('sales_rep', String),
                   Column('follow_up', String),
                   Column('notes', String),
                   Column('last_updated', DateTime)
                   )

metadata.create_all(engine)

Session = sessionmaker(bind=engine)
session = Session()


def fetch_postgres_data():
    try:
        query = data_table.select()
        result = session.execute(query).fetchall()

        if not result:
            return pd.DataFrame(columns=[
                'id', 'first_name', 'last_name', 'status', 'region',
                'sales_rep', 'follow_up', 'notes', 'last_updated'
            ])

        # Retrieve column names from data_table
        column_names = data_table.columns.keys()

        # Create DataFrame with dynamic column names
        df = pd.DataFrame(result, columns=column_names)
        logger.info(f"Fetched {len(df)} records from postgres")
        return df
    except Exception as e:
        logger.error(f"Error fetching PostgreSQL data: {e}")
        return pd.DataFrame()


def insert_postgres_record(record: DataRecord):
    try:
        query = data_table.insert().values(
            id=record.id,
            first_name=record.first_name,
            last_name=record.last_name,
            status=record.status,
            region=record.region,
            sales_rep=record.sales_rep,
            follow_up=record.follow_up,
            notes=record.notes,
            last_updated=record.last_updated
        )
        session.execute(query)
        session.commit()
        logger.info(f"Inserted record in postgres with id: {record.id}")
        return True
    except Exception as e:
        logger.error(f"Error inserting into PostgreSQL: {e}")
        session.rollback()
        return False


def update_postgres_record(record: DataRecord):
    try:
        query = data_table.update().where(data_table.c.id == record.id).values(
            first_name=record.first_name,
            last_name=record.last_name,
            status=record.status,
            region=record.region,
            sales_rep=record.sales_rep,
            follow_up=record.follow_up,
            notes=record.notes,
            last_updated=record.last_updated
        )
        session.execute(query)
        session.commit()
        logger.info(f"Updated record in postgres with id: {record.id}")
        return True
    except Exception as e:
        logger.error(f"Error updating PostgreSQL: {e}")
        session.rollback()
        return False


def delete_postgres_record(record_id: int):
    try:
        query = data_table.delete().where(data_table.c.id == record_id)
        session.execute(query)
        session.commit()
        logger.info(f"Deleted record from postgres with id: {record_id}")
        return True
    except Exception as e:
        logger.error(f"Error deleting from PostgreSQL: {e}")
        session.rollback()
        return False


def upsert_postgres_record(record: DataRecord):
    try:
        stmt = insert(data_table).values(
            id=record.id,
            first_name=record.first_name,
            last_name=record.last_name,
            status=record.status,
            region=record.region,
            sales_rep=record.sales_rep,
            follow_up=record.follow_up,
            notes=record.notes,
            last_updated=record.last_updated
        )
        stmt = stmt.on_conflict_do_update(
            index_elements=['id'],
            set_={
                'first_name': stmt.excluded.first_name,
                'last_name': stmt.excluded.last_name,
                'status': stmt.excluded.status,
                'region': stmt.excluded.region,
                'sales_rep': stmt.excluded.sales_rep,
                'follow_up': stmt.excluded.follow_up,
                'notes': stmt.excluded.notes,
                'last_updated': stmt.excluded.last_updated
            }
        )
        session.execute(stmt)
        session.commit()
        logger.info(f"Upserted record from postgres with id: {record.id}")
        return True
    except Exception as e:
        logger.error(f"Error upserting into PostgreSQL: {e}")
        session.rollback()
        return False
