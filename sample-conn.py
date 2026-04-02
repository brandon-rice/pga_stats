import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

def get_connection(use_cloud=False):
    if use_cloud:
        conn = psycopg2.connect(os.getenv("NEON_DATABASE_URL"))
    else:
        conn = psycopg2.connect(
            host=os.getenv("LOCAL_DB_HOST"),
            dbname=os.getenv("LOCAL_DB_NAME"),
            user=os.getenv("LOCAL_DB_USER"),
            password=os.getenv("LOCAL_DB_PASSWORD")
        )
    return conn