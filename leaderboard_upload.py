# Import necessary libraries
import os
import pandas as pd
import re
import numpy as np
import psycopg2
from psycopg2.extras import execute_values
from dotenv import load_dotenv
import os

load_dotenv()  # must come first

''' To upload leaderboard files do the following: 
1. go to pga.com site and go to leaderboard site, use the to_par scoring option 
1.5 Copy the text and paste it reg into excel. Then copy that into another sheet with just values. 
2. Save the file as leaderboard_[tourneyID]_[year].csv 
3. Save the file to the /Users/brandon/Desktop/Golf_Data/upload_staging/data_for_upload/ folder 

Other info: 
4. the process should create a merged file (this can be deleted at some point) in the folder 

'''

# Base folder path
LEADERBOARD_CSV_PATH = os.getenv("LEADERBOARD_CSV_PATH")
JOIN_FILE_CSV_PATH = os.getenv("JOIN_FILE_CSV_PATH")


# Load the file to join
main_df = pd.read_csv(JOIN_FILE_CSV_PATH)
main_df = main_df[['TourneyID','Tournament', 'Week_Of_Season', 'Course', 'Signature_Event', 'Major_Event', 'par']]

# --- Parse ID and Year from filename ---
def parse_id_and_year(filename):
    match = re.match(r'.*_(\d+)_(\d{4})\.csv', filename)
    if match:
        return match.group(1), match.group(2)
    return None, None


# --- Identify Leader Files ---
leader_files = [
    f for f in os.listdir(LEADERBOARD_CSV_PATH)
    if f.startswith('leaderboard_') and f.endswith('.csv')
]


# --- Process Each Leader File ---
for leader_file in leader_files:
    tourney_id, year = parse_id_and_year(leader_file)
    if not tourney_id:
        continue

    print(f"Processing leader file: {leader_file} (TourneyID={tourney_id}, year={year})")

    # Load leader file
    leader_path = os.path.join(LEADERBOARD_CSV_PATH, leader_file)
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
table_name = 'Leaderboard_data'


df = enriched_df.rename(columns={'Pos': 'pos', 'MOVEMENT': 'Movement'
                                , 'R1': 'r1' , 'R2': 'r2' , 'R3': 'r3' , 'R4': 'r4' 
                                , 'To Par': 'To_Par', 'PLAYER': 'Player'
                                , 'FedExCup Pts': 'FedExCup_Pts', 'Official Money': 'Official_Money'})
# drop blank rows 
df = df.dropna(subset=['pos'])
print('money col updated')
# update cells to fix format
df['r1'] = df['r1'].replace('^-$', '0', regex=True)
df['r1'] = pd.to_numeric(df['r1'], errors='coerce')
df['r1'] = df['r1'].fillna(0).astype(int) 


df['r2'] = df['r2'].replace('^-$', '0', regex=True)
df['r2'] = pd.to_numeric(df['r2'], errors='coerce')
df['r2'] = df['r2'].fillna(0).astype(int) 

df['r3'] = df['r3'].replace('^-$', '0', regex=True)
df['r3'] = pd.to_numeric(df['r3'], errors='coerce')
df['r3'] = df['r3'].fillna(0).astype(int) 

df['r4'] = df['r4'].replace('^-$', '0', regex=True)
df['r4'] = pd.to_numeric(df['r4'], errors='coerce')
df['r4'] = df['r4'].fillna(0).astype(int) 

df['r4'] = df['r4'].replace('^-$', '0', regex=True)
df['r4'] = pd.to_numeric(df['r4'], errors='coerce')
df['r4'] = df['r4'].fillna(0).astype(int) 

df['To_Par'] = pd.to_numeric(df['To_Par'].replace('E', '0'), errors='coerce').fillna(0).astype(int)

print('Update to cells done')
if df.shape[1] >= 17:
    df.drop(df.columns[16], axis=1, inplace=True)

# remove blank cols 
first_5_cols = df.columns[:5]
df[first_5_cols] = df[first_5_cols].replace(r'^\s*$', np.nan, regex=True)
df.dropna(subset=first_5_cols, how='all', inplace=True)
print('blank cols removed')

df['Official_Money'] = df['Official_Money'].replace('[\$,]', '', regex=True)
df['Official_Money'] = df['Official_Money'].replace(',', '', regex=True)
df['Official_Money'] = pd.to_numeric(df['Official_Money'], errors='coerce')  # Convert to float
df['Official_Money'] = df['Official_Money'].astype(int)


print('money col updated')

columns_to_clean = ['Week_Of_Season', 'year','r1','r2','r3','r4'
                        ,'Official_Money'
                        ,'To_Par']


for col in columns_to_clean:
    df[col] = df[col].apply(lambda x: None if pd.isna(x) else int(x))

    print(df.head())

columns_to_clean = ['FedExCup_Pts']

for col in columns_to_clean:
    df[col] = df[col].apply(lambda x: None if pd.isna(x) else None)

df.drop(columns=['TourneyID'], inplace=True)

conn = psycopg2.connect(**db_config)
cur = conn.cursor()
for _, row in df.iterrows():
    columns = ', '.join(df.columns)
    placeholders = ', '.join(['%s'] * len(row))
    insert_sql = f"INSERT INTO {schema}.{table_name} ({columns}) VALUES ({placeholders})"
    cur.execute(insert_sql, tuple(row))
        
    conn.commit()
    print(f"Inserted data from {tourney_file_name} into {table_name}")