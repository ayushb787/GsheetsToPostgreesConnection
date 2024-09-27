from sqlalchemy import create_engine
import os


def get_postgres_engine():
    user = os.getenv('POSTGRES_USER', 'postres')
    password = os.getenv('POSTGRES_PASSWORD', 'admin123')
    host = os.getenv('POSTGRES_HOST', 'localhost')
    port = os.getenv('POSTGRES_PORT', '5432')
    database = os.getenv('POSTGRES_DB', 'superjoin')

    conn_string = f"postgresql://{user}:{password}@{host}:{port}/{database}"
    engine = create_engine(conn_string)
    return engine
