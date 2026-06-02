# import necessary libraries
import os
import pandas as pd
import re
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os
load_dotenv()  
pd.set_option('display.max_rows', None)


''' To upload SG files do the following: 
1. go to pga.com site and go to stats site, then to the SG:Total 
2. Save the file as SG_[tourneyID]_[year].csv 
3. Save the file to the /Users/brandon/Desktop/Golf_Data/upload_staging/data_for_upload/ folder 

Other info: 
4. the process should create a merged file (this can be deleted at some point) in the folder 

'''

# Base folder path
SG_CSV_PATH = os.getenv("SG_CSV_PATH")
JOIN_FILE_CSV_PATH = os.getenv("JOIN_FILE_CSV_PATH")


# Load the file to join
main_df = pd.read_csv(JOIN_FILE_CSV_PATH)
main_df = main_df[['TourneyID', 'Tournament', 'Week_Of_Season', 'Course', 'Signature_Event', 'Major_Event', 'par']]

# --- Parse ID and Year from filename ---
def parse_id_and_year(filename):
    match = re.match(r'.*_(\d+)_(\d{4})\.csv', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None


# --- Identify Leader Files ---
leader_files = [
    f for f in os.listdir(SG_CSV_PATH)
    if f.startswith('sg') and f.endswith('.csv')
]

# --- Process Each Leader File ---
for leader_file in leader_files:
    tourney_id, year = parse_id_and_year(leader_file)
    if not tourney_id:
        continue

    print(f"Processing SG file: {leader_file} (TourneyID={tourney_id}, year={year})")

    # Load leader file
    leader_path = os.path.join(SG_CSV_PATH, leader_file)
    leader_df = pd.read_csv(leader_path)
    leader_df['TourneyID'] = int(tourney_id)
    leader_df['year'] = year

    # Join metadata (left join on tourneyID)
    enriched_df = leader_df.merge(main_df, on='TourneyID', how='left')

    # Remove blank rows
    enriched_df.dropna(how='all', inplace=True)

    # Trim spaces from column names in join file
    enriched_df.columns = enriched_df.columns.str.strip()

    # Trim spaces from string values in join file
    enriched_df = enriched_df.apply(lambda x: x.str.strip() if x.dtype == "object" else x)



tourney_file_name = enriched_df['Tournament'].unique().tolist()
print(tourney_file_name)

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

# Target table name
table_name = 'sg_data'

enriched_df['MOVEMENT'] = np.nan
enriched_df['PLAYER_ID'] = None


# reorder cols 
enriched_df = enriched_df[['Rank', 'MOVEMENT', 'PLAYER_ID', 'Player','Avg', 'Total SG:T', 'Total SG:T2G'
                           , 'Total SG:P', 'Measured Rounds','TourneyID','year','Tournament'
                           , 'Week_Of_Season' , 'Course','Signature_Event','Major_Event','par']]


df = enriched_df.rename(columns={'Rank': 'Rank', 'MOVEMENT': 'Movement', 
                            'PLAYER_ID': 'PlayerID', 'Player': 'Player', 'Total SG:T': 'Total_SG_T'
                            , 'Total SG:T2G': 'Total_SG_T2G', 'Total SG:P':'Total_SG_P'
                            , 'Measured Rounds':'Measured_Rounds' })

df = df.dropna(subset=df.columns[:7], how='all')

columns_to_clean = ['par', 'Week_Of_Season', 'year','Rank','Measured_Rounds'] # removing Player_ID since its null/blank

df.drop(columns=['TourneyID'], inplace=True)

for col in columns_to_clean:
    df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x))


conn = psycopg2.connect(**db_config)
cur = conn.cursor()
for _, row in df.iterrows():
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"
    
    # DEBUG: print the row and data types
    print("Row data:", row.to_dict())

    try:
        cur.execute(insert_sql, tuple(row))
    except Exception as e:
        print("Error on row:", row.to_dict())
        print("Data types:", [type(x) for x in row])
        raise e  # re-raise to stop execution

    #cur.execute(insert_sql, tuple(row))
        
    conn.commit()
    print(f"Inserted data from {tourney_file_name} into {table_name}")