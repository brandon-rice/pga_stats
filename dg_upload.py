# This will be used to upload golf ranking data to db 

import os
import pandas as pd
import re
import numpy as np
from datetime import datetime

# Need to re-do postgres creds 
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

# Load environment variables from .env file
load_dotenv()

# Base folder path
DATAGOLF_CSV_PATH = os.getenv("DATAGOLF_CSV_PATH")

# Database configuration from environment variables
db_config = {
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT', '5432')
}
schema = os.getenv('DB_SCHEMA')

# Test the connection
try:
    conn = psycopg2.connect(**db_config)
    conn.close()
    print("✅ Database connection successful!")
    print(f"   Connected to: {db_config['database']} on {db_config['host']}")
except Exception as e:
    print(f"❌ Connection failed: {e}")


create_table_query = """
CREATE TABLE IF NOT EXISTS datagolf_ranks (
    id SERIAL PRIMARY KEY,
    player_name VARCHAR(100), 
    primary_tour VARCHAR(100),
    dg_rank INT,
    owgr_rank INT,
    dg_index NUMERIC(10,3),
    refresh_date TIMESTAMP
     
);
"""
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

cur.execute(create_table_query)
conn.commit()

# Commit changes
conn.commit()
print("Table created successfully!")


# Read CSV file into a pandas DataFrame
df = pd.read_csv(DATAGOLF_CSV_PATH, index_col=False)

# Add refresh date column
df['refresh_date'] = datetime.now()

# Drop unwanted columns
columns_to_drop = ['dg_change', 'owgr_change','dg_points_rank', 'dg_points_change']
df = df.drop(columns=columns_to_drop, errors='ignore')

df['owgr_rank'] = pd.to_numeric(df['owgr_rank'], errors='coerce')
df['dg_rank'] = pd.to_numeric(df['dg_rank'], errors='coerce')


df = df.where(pd.notnull(df), None)
df['owgr_rank'] = df['owgr_rank'].fillna(9999)
numeric_columns = ['owgr_rank', 'dg_rank', 'dg_index']
for col in numeric_columns:
    if col in df.columns:
        df[col] = df[col].where(pd.notna(df[col]), None)


print(df)

# ...existing code...

# Convert "Last, First" to "First Last"
df['player_name'] = df['player_name'].apply(
    lambda x: ' '.join([part.strip() for part in x.split(',')[::-1]]) if ',' in x else x
)

# ...existing code...


# table the data is being inserted into
table_name = 'datagolf_ranks'

cur.execute(f"TRUNCATE TABLE {schema}.{table_name} RESTART IDENTITY")
conn.commit()
print("Table truncated successfully!")

for _, row in df.iterrows():
    columns = ', '.join(f'"{col}"' for col in df.columns)  # quoting for safety
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"
    print(row)  # Debug: See the values being inserted
    cur.execute(insert_sql, tuple(row))

conn.commit()
cur.close()
conn.close()
print("Data inserted successfully!")