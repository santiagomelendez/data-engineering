import os
from sqlalchemy import create_engine, text

hostname = os.environ.get('DB_HOSTNAME')
database = os.environ.get('DB_DATABASE')
username = os.environ.get('DB_USERNAME')
pwd = os.environ.get('DB_PWD')
port = int(os.environ.get('DATABASE_PORT'))


db_params = {
    'host': hostname,
    'user': username,
    'password': pwd,
    'database': database,
    'port': port,
}

db_url = f'postgresql+psycopg2://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}'


engine = create_engine(db_url)


def execute_query(query):
    with engine.begin() as conn:
        conn.execute(text(query))