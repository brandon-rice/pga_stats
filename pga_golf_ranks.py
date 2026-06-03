# Golfer Ranks using pga data 

from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np
import os
import re
import sys
from datetime import datetime


# Load environment variables before anything that reads from them
load_dotenv()

class _Tee:
    def __init__(self, *files):
        self._files = files
    def write(self, data):
        for f in self._files:
            f.write(data)
    def flush(self):
        for f in self._files:
            f.flush()

_log_dir = os.getenv('LOG_DIR', os.path.dirname(os.path.abspath(__file__)))
os.makedirs(_log_dir, exist_ok=True)
_log_path = os.path.join(_log_dir, f"pga_golf_ranks_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
_log_file = open(_log_path, 'w', encoding='utf-8')
sys.stdout = _Tee(sys.__stdout__, _log_file)
print(f"Logging output to: {_log_path}")
# Base folder path
PLAYER_LIST_CSV_PATH = os.getenv("PLAYER_LIST_CSV_PATH")

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

# Load environment variables from .env file
load_dotenv()
# Base folder path
PLAYER_LIST_CSV_PATH = os.getenv("PLAYER_LIST_CSV_PATH")

# --------------------------------------------------------------------------------------------------------
# ********************************************************************************************************
# Updated Player List 
tier_df = pd.read_csv(PLAYER_LIST_CSV_PATH) 
# --------------------------------------------------------------------------------------------------------


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

# Create a cursor
conn = psycopg2.connect(**db_config)
cur = conn.cursor()

# Example query
query = ''' select a.player , a.pos , a.to_par 
, a.year , a.week_of_season , a.tournament , a.course
, b.avg , case when b.avg is null then -2 else b.avg end Updated_Avg
, b.total_sg_t , b.total_sg_t2g , b.total_sg_p , measured_rounds 
from pga_stats.Leaderboard_data a 
left join pga_stats.sg_data b
on a.player = b.player 
and a.year = b.year 
and a.week_of_season = b.week_of_season
'''

# Execute the query
cur.execute(query)

# Fetch results
rows = cur.fetchall()

colnames = [desc[0] for desc in cur.description]

# Load into DataFrame
df = pd.DataFrame(rows, columns=colnames)


# Sort data
df = df.sort_values(by=['player', 'year', 'week_of_season'])

# Group by golfer and compute rolling averages
df['SG_last_1'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(1, min_periods=1).mean())
df['SG_last_2'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(2, min_periods=1).mean())
df['SG_last_3'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(3, min_periods=1).mean())
df['SG_last_5'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(5, min_periods=1).mean())
df['SG_last_8'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(8, min_periods=1).mean())
df['SG_last_13'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(13, min_periods=1).mean())
df['SG_last_21'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(21, min_periods=1).mean())
df['SG_last_34'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(34, min_periods=1).mean())
df['SG_last_55'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(55, min_periods=1).mean())
df['SG_last_89'] = df.groupby('player')['updated_avg'].transform(lambda x: x.rolling(89, min_periods=1).mean())


df = df.round(3)
player_view = df[df['player'].str.lower() == 'ben griffin']
latest = df.groupby('player').tail(1).reset_index(drop=True)

# Query to call updated datagolf ranks 

cur = conn.cursor()

# Write your SQL query
query = "SELECT * FROM pga_stats.datagolf_ranks;"

# Load data into a DataFrame
df = pd.read_sql_query(query, conn)

joined_df = pd.merge(latest, df, how='left', left_on='player', right_on='player_name')

# Percentile Ranks
sg_cols = [
    'SG_last_1', 'SG_last_2', 'SG_last_3', 'SG_last_5', 'SG_last_8',
    'SG_last_13', 'SG_last_21', 'SG_last_34', 'SG_last_55', 'SG_last_89', 'dg_index'
]

for col in sg_cols:
    joined_df[f'{col}_percentile'] = joined_df[col].rank(pct=True, ascending=True)

# If you want descending order (best performer = 100th percentile)
joined_df['owgr_rank_percentile'] = joined_df['owgr_rank'].rank(pct=True,ascending=False)
joined_df = joined_df.round(4)

# Pull leaderboard data
cur = conn.cursor()

# Write your SQL query
query = '''select player , pos , to_par , official_money 
, year , tournament , week_of_season , course 
from pga_stats.leaderboard_data ;'''

# Load data into a DataFrame
df = pd.read_sql_query(query, conn)

# Sort data
df = df.sort_values(by=['player', 'year', 'week_of_season'])


# CONFIGURATION: Choose how to handle cuts
CUT_HANDLING = "penalty"  # Options: "penalty", "skip", "field_size"
CUT_PENALTY_POSITION = 80  # Position assigned to cuts when using "penalty" method

# Convert position to numeric, handling string positions like "T5", "T10", etc.
def clean_position(pos):
    if pd.isna(pos):
        return np.nan
    
    # Convert to string and handle tied positions
    pos_str = str(pos).upper().strip()
    
    # Handle cuts based on chosen method
    if pos_str == 'CUT' or pos_str == 'MC':  # MC = Missed Cut
        if CUT_HANDLING == "penalty":
            return CUT_PENALTY_POSITION  # Fixed penalty position
        elif CUT_HANDLING == "skip":
            return np.nan  # Skip cuts entirely - don't count in averages

    # Remove 'T' for tied positions and convert to integer
    if pos_str.startswith('T'):
        return int(pos_str[1:])
    else:
        try:
            return int(pos_str)
        except ValueError:
            return np.nan
        
# Apply position cleaning
df['position_numeric'] = df['pos'].apply(clean_position)

# Sort by player and then by week/year to get chronological order
df_sorted = df.sort_values(['player', 'year', 'week_of_season'], ascending=[True, True, True])


# Group by player and calculate rolling averages
results = []

# Get current date info for filtering last year's data
current_year = df['year'].max()  # Use the most recent year in the data
last_year_cutoff = current_year - 1  # Look at roughly last year of data


for player, group in df_sorted.groupby('player'):
    # Get all tournament entries (including cuts) in chronological order
    all_entries = group.dropna(subset=['pos']).copy()
    
    # Filter for last year's data for cut percentage calculation
    # recent_entries = all_entries[all_entries['year'] >= current_year] # this is the old version that was just pulling current year values but was supposed to be prior year
    recent_entries = all_entries[all_entries['year'] == last_year_cutoff]
    # Calculate cut percentage over the last year
    if len(recent_entries) > 0:
        cuts_made = len(recent_entries[~recent_entries['pos'].str.upper().isin(['CUT', 'MC'])])
        total_tournaments = len(recent_entries)
        cut_percentage = (cuts_made / total_tournaments) * 100
    else:
        cut_percentage = np.nan
        total_tournaments = 0
    
    # Get numeric positions for averaging (this excludes NaN values from cuts if using "skip" method)
    positions = group['position_numeric'].dropna().tolist()
    
    if len(positions) == 0:
        continue

    # 1, 2, 3, 5, 8, 13, 21, 34, 55, 89, 144
    
    recent_positions = recent_entries[recent_entries['position_numeric'].notna()]['position_numeric'].tolist()


    top_5_count = len([pos for pos in recent_positions if pos <= 5])
    top_10_count = len([pos for pos in recent_positions if pos <= 10]) 
    top_20_count = len([pos for pos in recent_positions if pos <= 20])
    # Calculate average for last 3 games


    # Calculate percentages based on tournaments finished (not including cuts)
    num_recent_positions = len(recent_positions)
    if num_recent_positions > 0:
        top_5_percentage = (top_5_count / num_recent_positions) * 100
        top_10_percentage = (top_10_count / num_recent_positions) * 100
        top_20_percentage = (top_20_count / num_recent_positions) * 100
    else:
        top_5_percentage = 0.0
        top_10_percentage = 0.0
        top_20_percentage = 0.0

    last_1_avg = np.mean(positions[-1:]) if len(positions) >= 1 else np.nan
    last_3_avg = np.mean(positions[-3:]) if len(positions) >= 1 else np.nan
    
    # Calculate average for last 5 games
    last_5_avg = np.mean(positions[-5:]) if len(positions) >= 1 else np.nan
    
    # Calculate average for last 10 games
    last_10_avg = np.mean(positions[-10:]) if len(positions) >= 1 else np.nan

    # Count of games with valid positions (excludes cuts if using "skip" method)
    games_with_positions = len(positions)
    
    # Count of all tournament entries
    total_entries = len(all_entries)
    last_5_pos  = positions[-5:]
    last_10_pos = positions[-10:]

    results.append({
        'player': player,
        'total_tournaments': total_entries,
        'games_with_positions': games_with_positions,
        'tournaments_last_year': total_tournaments, #recent_entries = all_entries[all_entries['year'] == last_year_cutoff]
        'cut_percentage_last_year': round(cut_percentage, 1) if not pd.isna(cut_percentage) else None,
        'last_3_avg_position': round(last_3_avg, 2) if not pd.isna(last_3_avg) else None,
        'last_5_avg_position': round(last_5_avg, 2) if not pd.isna(last_5_avg) else None,
        'last_10_avg_position': round(last_10_avg, 2) if not pd.isna(last_10_avg) else None,
        'top_5_count': round(top_5_count, 2),
        'top_10_count': round(top_10_count, 2),
        'top_20_count': round(top_20_count, 2),
        'top_5_percentage_last_year': round(top_5_percentage, 1),
        'top_10_percentage_last_year': round(top_10_percentage, 1),
        'top_20_percentage_last_year': round(top_20_percentage, 1),
         # top finish counts — last 5 starts
        # 'top_3_last_5'                    : sum(1 for p in last_5_pos  if p <= 3), # maybeb delete this 
        'top_5_last_5'                    : sum(1 for p in last_5_pos  if p <= 5),
        'top_10_last_5'                   : sum(1 for p in last_5_pos  if p <= 10),
        'top_20_last_5'                   : sum(1 for p in last_5_pos  if p <= 20),
        # top finish counts — last 10 starts
        # 'top_3_last_10'                   : sum(1 for p in last_10_pos if p <= 3), # maybeb delete this 
        'top_5_last_10'                   : sum(1 for p in last_10_pos if p <= 5),
        'top_10_last_10'                  : sum(1 for p in last_10_pos if p <= 10),
        'top_20_last_10'                  : sum(1 for p in last_10_pos if p <= 20),

    })

# Convert to DataFrame
results_df = pd.DataFrame(results)

# Sort by best average position over last 5 games (lower is better)
results_df = results_df.sort_values('last_5_avg_position')

# Display results
top_n = 20
'''
print(f"Top {top_n} Players by Average Finish Position")
print("=" * 90)
print(f"{'Player':<25} {'Tournaments':<12} {'Cut %':<8} {'Last 3 Avg':<12} {'Last 5 Avg':<12}")
print("-" * 90)
'''
for idx, row in results_df.head(top_n).iterrows():
    #top_5 = f"{row['top_5_percentage']:.2f}" if row['top_5_percentage'] is not None else "N/A"
    last_3 = f"{row['last_3_avg_position']:.2f}" if row['last_3_avg_position'] is not None else "N/A"
    last_5 = f"{row['last_5_avg_position']:.2f}" if row['last_5_avg_position'] is not None else "N/A"
    cut_pct = f"{row['cut_percentage_last_year']:.1f}%" if row['cut_percentage_last_year'] is not None else "N/A"
    tournaments = f"{row['tournaments_last_year']}"
    
    print(f"{row['player']:<25} {tournaments:<12} {cut_pct:<8} {last_3:<12} {last_5:<12}")

filtered_df = results_df

filtered_df['last_3_avg_position_percentile'] = filtered_df['last_3_avg_position'].rank(pct=True,ascending=False)
filtered_df['last_5_avg_position_percentile'] = filtered_df['last_5_avg_position'].rank(pct=True,ascending=False)
filtered_df['last_10_avg_position_percentile'] = filtered_df['last_10_avg_position'].rank(pct=True,ascending=False)
filtered_df['cut_percentage_last_year_percentile'] = filtered_df['cut_percentage_last_year'].rank(pct=True,ascending=True)
filtered_df['top_5_percentage_last_year_percentile'] = filtered_df['top_5_percentage_last_year'].rank(pct=True,ascending=True)
filtered_df['top_10_percentage_last_year_percentile'] = filtered_df['top_10_percentage_last_year'].rank(pct=True,ascending=True)
filtered_df['top_20_percentage_last_year_percentile'] = filtered_df['top_20_percentage_last_year'].rank(pct=True,ascending=True)

#filtered_df['top_3_last_5_percentile'] = filtered_df['top_3_last_5'].rank(pct=True,ascending=True)
filtered_df['top_5_last_5_percentile'] = filtered_df['top_5_last_5'].rank(pct=True,ascending=True)
filtered_df['top_10_last_5_percentile'] = filtered_df['top_10_last_5'].rank(pct=True,ascending=True)
filtered_df['top_20_last_5_percentile'] = filtered_df['top_20_last_5'].rank(pct=True,ascending=True)


#filtered_df['top_3_last_10_percentile'] = filtered_df['top_3_last_10'].rank(pct=True,ascending=True)
filtered_df['top_5_last_10_percentile'] = filtered_df['top_5_last_10'].rank(pct=True,ascending=True)
filtered_df['top_10_last_10_percentile'] = filtered_df['top_10_last_10'].rank(pct=True,ascending=True)
filtered_df['top_20_last_10_percentile'] = filtered_df['top_20_last_10'].rank(pct=True,ascending=True)

merged_dataset = pd.merge(joined_df, filtered_df, on='player', how='left')
merged_dataset = merged_dataset[merged_dataset['tournaments_last_year'] > 4]

metric_col_1 = 'SG_last_1_percentile'
metric_col_2 = 'SG_last_2_percentile'
metric_col_3 = 'SG_last_3_percentile'
metric_col_4 = 'SG_last_5_percentile'
metric_col_5 = 'SG_last_8_percentile'
metric_col_6 = 'SG_last_13_percentile'
metric_col_7 = 'SG_last_21_percentile'
metric_col_8 = 'SG_last_34_percentile'
metric_col_9 = 'SG_last_55_percentile'
metric_col_10 = 'dg_index_percentile'
metric_col_11 = 'owgr_rank_percentile'
metric_col_12 = 'last_3_avg_position_percentile'
metric_col_13 = 'last_5_avg_position_percentile'
metric_col_14 = 'last_10_avg_position_percentile'
metric_col_15 = 'cut_percentage_last_year_percentile'
metric_col_16 = 'top_5_percentage_last_year_percentile'
metric_col_17 = 'top_10_percentage_last_year_percentile'
metric_col_18 = 'top_20_percentage_last_year_percentile'

#metric_col_19 = 'top_3_last_5_percentile' 

metric_col_19 = 'top_5_last_5_percentile' 
metric_col_20 = 'top_10_last_5_percentile' 
metric_col_21 = 'top_20_last_5_percentile' 

#metric_col_22 = 'top_3_last_10_percentile' 
metric_col_22 = 'top_5_last_10_percentile' 
metric_col_23 = 'top_10_last_10_percentile' 
metric_col_24 = 'top_20_last_10_percentile'

# First Version, there was too much weight on most recent events. 
# This is a problem because some events have worse fields and this makes less competitive fields have an advantage. 

weight_1	=	0.75	#	SG_last_1_percentile
weight_2	=	0.75	#	SG_last_2_percentile
weight_3	=	0.75	#	SG_last_3_percentile
weight_4	=	0.75	#	SG_last_5_percentile
weight_5	=	0.5	#	SG_last_8_percentile
weight_6	=	0.5	#	SG_last_13_percentile
weight_7	=	0.5	#	SG_last_21_percentile
weight_8	=	0.25	#	SG_last_34_percentile
weight_9	=	0.25	#	SG_last_55_percentile
weight_10 = 1.5     # dg_index_percentile
weight_11 = 0.5     # owgr_rank_percentile
weight_12 = 0.5     # last_3_avg_position_percentile
weight_13 = 0.75    # last_5_avg_position_percentile
weight_14 = 0.5     # last_10_avg_position_percentile
weight_15 = 0.75    # cut_percentage_last_year_percentile
weight_16 = 0.75    # top_5_percentage_last_year_percentile
weight_17 = 0.5     # top_10_percentage_last_year_percentile
weight_18 = 0.5     # top_20_percentage_last_year_percentile
weight_19 = 1       # top_5_last_5_percentile
weight_20 = 1.25    # top_10_last_5_percentile
weight_21 = 0.5     # top_20_last_5_percentile
weight_22 = 1       # top_5_last_10_percentile
weight_23 = 1.25    # top_10_last_10_percentile
weight_24 = 0.5     # top_20_last_10_percentile

# Calculate total weight for normalization
total_weight = (weight_1 + weight_2 + weight_3 + weight_4 + weight_5 + 
               weight_6 + weight_7 + weight_8 + weight_9 + weight_10 +
               weight_11 + weight_12 + weight_13 + weight_14 + weight_15 + weight_16 + weight_17 + weight_18 +
               weight_19 + weight_20 + weight_21 + weight_22 + weight_23 + weight_24)


# Normalize weights so they sum to 1
norm_weight_1 = weight_1 / total_weight
norm_weight_2 = weight_2 / total_weight
norm_weight_3 = weight_3 / total_weight
norm_weight_4 = weight_4 / total_weight
norm_weight_5 = weight_5 / total_weight
norm_weight_6 = weight_6 / total_weight
norm_weight_7 = weight_7 / total_weight
norm_weight_8 = weight_8 / total_weight
norm_weight_9 = weight_9 / total_weight
norm_weight_10 = weight_10 / total_weight
norm_weight_11 = weight_11 / total_weight
norm_weight_12 = weight_12 / total_weight
norm_weight_13 = weight_13 / total_weight
norm_weight_14 = weight_14 / total_weight
norm_weight_15 = weight_15 / total_weight
norm_weight_16 = weight_16 / total_weight
norm_weight_17 = weight_17 / total_weight
norm_weight_18 = weight_18 / total_weight
norm_weight_19 = weight_19 / total_weight
norm_weight_20 = weight_20 / total_weight
norm_weight_21 = weight_21 / total_weight
norm_weight_22 = weight_22 / total_weight
norm_weight_23 = weight_23 / total_weight
norm_weight_24 = weight_24 / total_weight


# Calculate weighted composite score for each player
weighted_scores = []

for index, row in merged_dataset.iterrows():
    # Get the values for each metric (handle missing values)
    val_1 = row[metric_col_1] if not pd.isna(row[metric_col_1]) else 0
    val_2 = row[metric_col_2] if not pd.isna(row[metric_col_2]) else 0
    val_3 = row[metric_col_3] if not pd.isna(row[metric_col_3]) else 0
    val_4 = row[metric_col_4] if not pd.isna(row[metric_col_4]) else 0
    val_5 = row[metric_col_5] if not pd.isna(row[metric_col_5]) else 0
    val_6 = row[metric_col_6] if not pd.isna(row[metric_col_6]) else 0
    val_7 = row[metric_col_7] if not pd.isna(row[metric_col_7]) else 0
    val_8 = row[metric_col_8] if not pd.isna(row[metric_col_8]) else 0
    val_9 = row[metric_col_9] if not pd.isna(row[metric_col_9]) else 0
    val_10 = row[metric_col_10] if not pd.isna(row[metric_col_10]) else 0
    val_11 = row[metric_col_11] if not pd.isna(row[metric_col_11]) else 0
    val_12 = row[metric_col_12] if not pd.isna(row[metric_col_12]) else 0
    val_13 = row[metric_col_13] if not pd.isna(row[metric_col_13]) else 0
    val_14 = row[metric_col_14] if not pd.isna(row[metric_col_14]) else 0
    val_15 = row[metric_col_15] if not pd.isna(row[metric_col_15]) else 0
    val_16 = row[metric_col_16] if not pd.isna(row[metric_col_16]) else 0
    val_17 = row[metric_col_17] if not pd.isna(row[metric_col_17]) else 0
    val_18 = row[metric_col_18] if not pd.isna(row[metric_col_18]) else 0
    val_19 = row[metric_col_19] if not pd.isna(row[metric_col_19]) else 0
    val_20 = row[metric_col_20] if not pd.isna(row[metric_col_20]) else 0
    val_21 = row[metric_col_21] if not pd.isna(row[metric_col_21]) else 0
    val_22 = row[metric_col_22] if not pd.isna(row[metric_col_22]) else 0
    val_23 = row[metric_col_23] if not pd.isna(row[metric_col_23]) else 0
    val_24 = row[metric_col_24] if not pd.isna(row[metric_col_24]) else 0
    
    # Calculate weighted score
    weighted_score = (val_1 * norm_weight_1 + 
                     val_2 * norm_weight_2 + 
                     val_3 * norm_weight_3 + 
                     val_4 * norm_weight_4 + 
                     val_5 * norm_weight_5 + 
                     val_6 * norm_weight_6 + 
                     val_7 * norm_weight_7 + 
                     val_8 * norm_weight_8 + 
                     val_9 * norm_weight_9 + 
                     val_10 * norm_weight_10 +
                     val_11 * norm_weight_11 +
                     val_12 * norm_weight_12 +
                     val_13 * norm_weight_13 +
                     val_14 * norm_weight_14 +
                     val_15 * norm_weight_15 + 
                     val_16 * norm_weight_16 +
                     val_17 * norm_weight_17 +
                     val_18 * norm_weight_18 +
                     val_19 * norm_weight_19 +
                     val_20 * norm_weight_20 +
                     val_21 * norm_weight_21 +
                     val_22 * norm_weight_22 +
                     val_23 * norm_weight_23 +
                     val_24 * norm_weight_24)
    
    weighted_scores.append(weighted_score)


# Add weighted scores to dataframe
merged_dataset['weighted_composite'] = weighted_scores

# Calculate final percentile rank manually
# Sort the weighted scores and assign ranks
df_sorted = merged_dataset.sort_values('weighted_composite', ascending=False)
df_sorted['rank'] = range(1, len(df_sorted) + 1)

df_sorted['composite_percentile_rank'] = df_sorted['weighted_composite'].rank(pct=True,ascending=True)

df_sorted = df_sorted.round(4)
composite_with_percentile = df_sorted[['player', 'weighted_composite', 'composite_percentile_rank']].copy()
#composite_with_percentile.loc[composite_with_percentile['player'] == 'Ludvig Åberg', 'player'] = 'Ludvig Aberg'

playerlist_metrics = pd.merge(
    tier_df, 
    composite_with_percentile, 
    on='player', 
    how='left'  # Keep all tier players, even if no composite score
)

# STEP 6: Analyze by tier
print("\n=== TOURNAMENT TIER ANALYSIS ===")
print("Best players in each Tier (by composite score):")
print("=" * 70)

for tier_num in sorted(playerlist_metrics['Tier'].unique()):
    tier_players = playerlist_metrics[playerlist_metrics['Tier'] == tier_num].sort_values('weighted_composite', ascending=False)
    
    print(f"\nTIER {tier_num} (Top 5):")
    print(f"{'Rank':<4} {'Player':<25} {'Composite':<12} {'Percentile':<12}")
    print("-" * 55)
    
    for i, (_, player) in enumerate(tier_players.iterrows(), 1):
        comp_score = f"{player['weighted_composite']:.4f}" if not pd.isna(player['weighted_composite']) else "N/A"
        pct_rank = f"{player['composite_percentile_rank']:.4f}%" if not pd.isna(player['composite_percentile_rank']) else "N/A"
        print(f"{i:<4} {player['player']:<25} {comp_score:<12} {pct_rank:<12}")

# STEP 7: Optimal picks for your competition
print("\n=== OPTIMAL PICKS FOR COMPETITION ===")
print("Best player from each tier:")
print("=" * 60)

optimal_picks = []
total_composite = 0

for tier_num in sorted(playerlist_metrics['Tier'].unique()):
    tier_players = playerlist_metrics[playerlist_metrics['Tier'] == tier_num].sort_values('weighted_composite', ascending=False)
    best_player = tier_players.iloc[0]
    
    optimal_picks.append({
        'Tier': tier_num,
        'player': best_player['player'],
        'composite_score': best_player['weighted_composite'],
        'percentile_rank': best_player['composite_percentile_rank']
    })
    
    total_composite += best_player['weighted_composite']
    
    comp_score = f"{best_player['weighted_composite']:.4f}" if not pd.isna(best_player['weighted_composite']) else "N/A"
    pct_rank = f"{best_player['composite_percentile_rank']:.4f}%" if not pd.isna(best_player['composite_percentile_rank']) else "N/A"
    
    print(f"Tier {tier_num}: {best_player['player']:<25} (Score: {comp_score}, Rank: {pct_rank})")

print(f"\nTotal Team Composite Score: {total_composite:.4f}")
print(f"Average Team Composite Score: {total_composite/6:.4f}")

# ── Merge ──────────────────────────────────────────────────────────────────
merged_dataset = pd.merge(joined_df, filtered_df, on='player', how='left')
merged_dataset = merged_dataset[merged_dataset['tournaments_last_year'] > 3]

# ── Composite Score ────────────────────────────────────────────────────────
weights = {
    'SG_last_1_percentile'                   : weight_1,
    'SG_last_2_percentile'                   : weight_2,
    'SG_last_3_percentile'                   : weight_3,
    'SG_last_5_percentile'                   : weight_4,
    'SG_last_8_percentile'                   : weight_5,
    'SG_last_13_percentile'                  : weight_6,
    'SG_last_21_percentile'                  : weight_7,
    'SG_last_34_percentile'                  : weight_8,
    'SG_last_55_percentile'                  : weight_9,
    'dg_index_percentile'                    : weight_10,
    'owgr_rank_percentile'                   : weight_11,
    'last_3_avg_position_percentile'         : weight_12,
    'last_5_avg_position_percentile'         : weight_13,
    'last_10_avg_position_percentile'        : weight_14,
    'cut_percentage_last_year_percentile'    : weight_15,
    'top_5_percentage_last_year_percentile'  : weight_16,
    'top_10_percentage_last_year_percentile' : weight_17,
    'top_20_percentage_last_year_percentile' : weight_18,
    'top_5_last_5_percentile'               : weight_19,
    'top_10_last_5_percentile'              : weight_20,
    'top_20_last_5_percentile'              : weight_21,            
    'top_5_last_10_percentile'              : weight_22,
    'top_10_last_10_percentile'             : weight_23,
    'top_20_last_10_percentile'             : weight_24,
}
total_weight = sum(weights.values())
norm_weights = {col: w / total_weight for col, w in weights.items()}

def _composite(row):
    return sum(
        (row[col] if not pd.isna(row.get(col)) else 0) * nw
        for col, nw in norm_weights.items()
        if col in row.index
    )

merged_dataset['composite_score']           = merged_dataset.apply(_composite, axis=1)
merged_dataset['composite_percentile_rank'] = merged_dataset['composite_score'].rank(pct=True, ascending=True)
merged_dataset = merged_dataset.round(4)

print(f'✅ merged_dataset ready — {len(merged_dataset):,} players')

# ── golfer_profile() definition ────────────────────────────────────────────
def golfer_profile(
    player_name,
    tournament_name = None,
    last_n_finishes = 15,
    min_events      = 5,
):
    """
    Print a full scouting report for a given player.

    Parameters
    ----------
    player_name     : str   — player name (case-insensitive)
    tournament_name : str   — optional event name to pull historical finishes
    last_n_finishes : int   — how many recent results to display (default 10)
    min_events      : int   — minimum events required to show percentile rankings
    """
    name_lower = player_name.strip().lower()
    lb = df[df['player'].str.lower() == name_lower].copy()

    if lb.empty:
        print(f"\n❌  No leaderboard data found for '{player_name}'. Check spelling.\n")
        return

    canonical_name = lb['player'].iloc[0]
    lb = lb.sort_values(['year', 'week_of_season'])

    def _pos_numeric(pos, cut_penalty=80):
        if pd.isna(pos): return np.nan
        s = str(pos).upper().strip()
        if s in ('CUT', 'MC', 'WD', 'DQ'): return cut_penalty
        if s.startswith('T'):
            try: return int(s[1:])
            except ValueError: return np.nan
        try: return int(s)
        except ValueError: return np.nan

    lb['_pos_num'] = lb['pos'].apply(_pos_numeric)

    # ── Header ────────────────────────────────────────────────────────────
    print('\n' + '═' * 65)
    print(f'  🏌️  GOLFER PROFILE: {canonical_name.upper()}')
    print('═' * 65)

    # Want to add more stats here 
    print('datagolf ranking: ' + str(merged_dataset.loc[merged_dataset['player'] == canonical_name]['dg_rank'].iloc[0]))
    print(merged_dataset.loc[merged_dataset['player'] == canonical_name][['dg_rank']].to_string(index=False))


    # ── Section 1: Recent Finishes ────────────────────────────────────────
    print(f'\n📋  LAST {last_n_finishes} FINISHES')
    print('-' * 65)
    recent = lb.tail(last_n_finishes)[['year', 'tournament', 'course', 'pos', 'to_par']].copy()
    recent = recent.iloc[::-1].reset_index(drop=True)
    print(f"  {'#':<4} {'Year':<6} {'Pos':<6} {'To Par':<9} {'Tournament':<30} {'Course'}")
    print('  ' + '-' * 62)
    for i, row in recent.iterrows():
        print(f"  {i+1:<4} {int(row['year']):<6} {str(row['pos']):<6} "
              f"{str(row['to_par']) if not pd.isna(row['to_par']) else 'N/A':<9} "
              f"{str(row['tournament'])[:28] if not pd.isna(row['tournament']) else 'N/A':<30} "
              f"{str(row['course'])[:20] if not pd.isna(row['course']) else 'N/A'}")

    # ── Section 2: Rolling Average Finish ────────────────────────────────
    print('\n📊  AVERAGE FINISH POSITION')
    print('-' * 65)
    valid_pos = lb['_pos_num'].dropna().tolist()

    def _avg(lst, n):
        tail = lst[-n:]
        return round(np.mean(tail), 2) if tail else None
    
    def _top_count(lst, n, threshold):
        tail = lst[-n:]
        return sum(1 for p in tail if p <= threshold)

    last3_avg  = _avg(valid_pos,  3)
    last5_avg  = _avg(valid_pos,  5)
    last10_avg = _avg(valid_pos, 10)

    print(f"  Last  3 starts : {_avg(valid_pos,  3) or 'N/A'}")
    print(f"  Last  5 starts : {_avg(valid_pos,  5) or 'N/A'}")
    print(f"  Last 10 starts : {_avg(valid_pos, 10) or 'N/A'}")

    print(f"  Last  3 starts : avg finish = {last3_avg  or 'N/A'}")
    print()
    print(f"  {'Metric':<25} {'Last 5':>8} {'Last 10':>8}")
    print(f"  {'-'*42}")
    print(f"  {'Avg Finish':<25} {str(last5_avg)  if last5_avg  is not None else 'N/A':>8} {str(last10_avg) if last10_avg is not None else 'N/A':>8}")
    print(f"  {'Top 3 Finishes':<25} {_top_count(valid_pos,  5, 3):>8} {_top_count(valid_pos, 10, 3):>8}")
    print(f"  {'Top 5 Finishes':<25} {_top_count(valid_pos,  5, 5):>8} {_top_count(valid_pos, 10, 5):>8}")
    print(f"  {'Top 10 Finishes':<25} {_top_count(valid_pos,  5, 10):>8} {_top_count(valid_pos, 10, 10):>8}")


    print(f"\n  {'Year':<8} {'Events':<8} {'Cuts Made':<12} {'Cut %':<9} {'Avg Finish':<12} {'Top 5':<7} {'Top 10':<7} {'Top 20'}")
    print('  ' + '-' * 60)
    for year, grp in lb.groupby('year', sort=True):
        total_ev   = len(grp)
        cuts_miss  = grp['pos'].str.upper().isin(['CUT', 'MC']).sum()
        cuts_made  = total_ev - cuts_miss
        cut_pct    = round(cuts_made / total_ev * 100, 1) if total_ev else 0
        num_pos    = grp['_pos_num'].dropna()
        avg_fin    = round(num_pos.mean(), 1) if not num_pos.empty else 'N/A'
        print(f"  {int(year):<8} {total_ev:<8} {cuts_made:<12} {cut_pct:<9} {avg_fin:<12} "
              f"{int((num_pos <= 5).sum()):<7} {int((num_pos <= 10).sum()):<7} {int((num_pos <= 20).sum())}")

    # ── Section 3: Event History ──────────────────────────────────────────
    if tournament_name:
        print(f'\n🏆  HISTORY AT: {tournament_name.upper()}')
        print('-' * 65)
        t_hist = lb[lb['tournament'].str.lower().str.contains(tournament_name.strip().lower(), na=False)]
        t_hist = t_hist.sort_values('year', ascending=False)
        if t_hist.empty:
            print(f"  No results found for '{tournament_name}'.")
        else:
            print(f"  {'Year':<8} {'Pos':<8} {'To Par':<10} {'Course'}")
            print('  ' + '-' * 40)
            for _, row in t_hist.iterrows():
                print(f"  {int(row['year']):<8} {str(row['pos']):<8} "
                      f"{str(row['to_par']) if not pd.isna(row['to_par']) else 'N/A':<10} "
                      f"{str(row['course'])[:25] if not pd.isna(row['course']) else 'N/A'}")
            ev_num = t_hist['_pos_num'].dropna()
            if not ev_num.empty:
                print(f'\n  Avg finish at {tournament_name}: {round(ev_num.mean(), 1)}')

    # ── Section 4: SG & Finish Percentile Rankings ────────────────────────
    print(f'\n📈  STROKES GAINED PERCENTILE RANKINGS  (vs. field with {min_events}+ events)')
    print('-' * 65)

    player_row = merged_dataset[merged_dataset['player'].str.lower() == name_lower]

    if player_row.empty:
        print(f"  ⚠️  '{canonical_name}' not found in merged_dataset (may have < {min_events} events).")
    else:
        row = player_row.iloc[0]

        def _bar(pct, width=20):
            if pd.isna(pct): return '[  N/A  ]'
            filled = int(round(pct * width))
            return '[' + '█' * filled + '░' * (width - filled) + f']  {pct*100:.1f}th'

        sg_cols = {
            'SG Last 1 Event'   : 'SG_last_1_percentile',
            'SG Last 3 Events'  : 'SG_last_3_percentile',
            'SG Last 5 Events'  : 'SG_last_5_percentile',
            'SG Last 8 Events'  : 'SG_last_8_percentile',
            'SG Last 13 Events' : 'SG_last_13_percentile',
            'SG Last 21 Events' : 'SG_last_21_percentile',
            'DG Index'          : 'dg_index_percentile',
            'OWGR Rank'         : 'owgr_rank_percentile',
        }
        finish_cols = {
            'Avg Finish Last 3'       : 'last_3_avg_position_percentile',
            'Avg Finish Last 5'       : 'last_5_avg_position_percentile',
            'Avg Finish Last 10'      : 'last_10_avg_position_percentile',
            'Cut % Last Year'         : 'cut_percentage_last_year_percentile',
            'Top 5% Last Year'        : 'top_5_percentage_last_year_percentile',
            'Top 10% Last Year'       : 'top_10_percentage_last_year_percentile',
            'Top 20% Last Year'       : 'top_20_percentage_last_year_percentile',
            'Top 5 Finishes (Last 5)' : 'top_5_last_5_percentile',
            'Top 10 Finishes (Last 5)': 'top_10_last_5_percentile',
            'Top 20 Finishes (Last 5)': 'top_20_last_5_percentile',
            'Top 5 Finishes (Last 10)': 'top_5_last_10_percentile',
            'Top 10 Finishes(Last 10)': 'top_10_last_10_percentile',
            'Top 20 Finishes(Last 10)': 'top_20_last_10_percentile',
        }

        print(f"\n  {'Metric':<28} Percentile Bar")
        print('  ' + '-' * 55)
        print('  — Strokes Gained —')
        for label, col in sg_cols.items():
            print(f"  {label:<28} {_bar(row.get(col, np.nan))}")
        print('\n  — Finish Position —')
        for label, col in finish_cols.items():
            print(f"  {label:<28} {_bar(row.get(col, np.nan))}")

        # ── Section 5: Composite Summary ──────────────────────────────────
        print('\n🎯  COMPOSITE SCORE SUMMARY')
        print('-' * 65)
        comp_val = row.get('composite_score', np.nan)
        if not pd.isna(comp_val):
            comp_pct = (merged_dataset['composite_score'] <= comp_val).mean()
            print(f'  Composite Score  : {comp_val:.4f}')
            print(f'  Composite Pctile : {comp_pct*100:.1f}th percentile')
        else:
            print('  Composite Score  : N/A')

        tier_val = row.get('tier', row.get('Tier', 'N/A'))
        print(f'  Tier             : {tier_val}')

        all_valid = lb['_pos_num'].dropna()
        total_ev  = len(lb)
        cuts_miss = lb['pos'].str.upper().isin(['CUT','MC']).sum()
        print(f'\n  Career Events    : {total_ev}')
        print(f'  Career Cuts Miss : {cuts_miss}  ({round(cuts_miss/total_ev*100,1) if total_ev else 0}%)')
        print(f'  Career Avg Finish: {round(all_valid.mean(), 1) if not all_valid.empty else "N/A"}')

    print('\n' + '═' * 65 + '\n')

print('✅ golfer_profile() is ready to use')


# Build players list from a specific tier
# ── Tier 1 ────────────────────────────────────────────────────────
tier = 1
players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

for name in players:
    golfer_profile(player_name=name, last_n_finishes=15)


# ── Tier 2 ────────────────────────────────────────────────────────
tier = 2
players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

for name in players:
    golfer_profile(player_name=name, last_n_finishes=15)

# ── Tier 3 ────────────────────────────────────────────────────────
tier = 3
players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

for name in players:
    golfer_profile(player_name=name, last_n_finishes=15)

# ── Tier 4 ────────────────────────────────────────────────────────
tier = 4
players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

for name in players:
    golfer_profile(player_name=name, last_n_finishes=15)

# ── Tier 5 ────────────────────────────────────────────────────────
tier = 5
players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

for name in players:
    golfer_profile(player_name=name, last_n_finishes=15)

# ── Tier 6 ────────────────────────────────────────────────────────
tier = 6
players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

for name in players:
    if merged_dataset[merged_dataset['player'].str.lower() == name.lower()].shape[0] > 0:
        golfer_profile(player_name=name, last_n_finishes=15)

# ------------------------------------------------------------------------------------------------------
# Want to post the golfer metrics to the db 
# ------------------------------------------------------------------------------------------------------

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import numpy as np

# ── Config ─────────────────────────────────────────────────────────────────
TABLE_NAME   = 'combined_data'           # change if desired
SCHEMA       = schema                     # already defined as os.getenv('DB_SCHEMA')
FULL_TABLE   = f'{SCHEMA}.{TABLE_NAME}'
IF_EXISTS    = 'replace'                  # 'replace' drops & recreates | 'append' adds rows

# ── 1. Clean the dataframe before writing ──────────────────────────────────
df_out = merged_dataset.copy()

# Replace NaN/inf with None so psycopg2 writes proper NULLs
df_out = df_out.replace([np.inf, -np.inf], np.nan)

for col in df_out.columns:
    if pd.api.types.is_datetime64_any_dtype(df_out[col]):
        df_out[col] = df_out[col].astype(object).where(df_out[col].notna(), None)

df_out = df_out.where(pd.notnull(df_out), None)


# ── 2. Build CREATE TABLE statement from dtypes ────────────────────────────
def pg_dtype(series):
    if pd.api.types.is_datetime64_any_dtype(series):
        return 'TIMESTAMP'
    elif pd.api.types.is_integer_dtype(series):
        return 'BIGINT'
    elif pd.api.types.is_float_dtype(series):
        return 'DOUBLE PRECISION'
    elif pd.api.types.is_bool_dtype(series):
        return 'BOOLEAN'
    else:
        return 'TEXT'

col_defs = ',\n    '.join(
    f'"{col}" {pg_dtype(df_out[col])}' for col in df_out.columns
)


# ── 3. Write to Postgres ───────────────────────────────────────────────────
conn = psycopg2.connect(**db_config)
cur  = conn.cursor()

try:
    if IF_EXISTS == 'replace':
        cur.execute(f'DROP TABLE IF EXISTS {FULL_TABLE};')
        cur.execute(f'CREATE TABLE {FULL_TABLE} (\n    {col_defs}\n);')
        print(f'✅ Table {FULL_TABLE} (re)created — {len(df_out.columns)} columns')

    elif IF_EXISTS == 'append':
        # Table must already exist with matching columns
        pass

    # Batch insert using execute_values for speed
    cols        = [f'"{c}"' for c in df_out.columns]
    col_str     = ', '.join(cols)
    rows        = [tuple(row) for row in df_out.itertuples(index=False, name=None)]

    execute_values(
        cur,
        f'INSERT INTO {FULL_TABLE} ({col_str}) VALUES %s',
        rows,
        page_size=500
    )

    conn.commit()
    print(f'✅ {len(rows):,} rows written to {FULL_TABLE}')

except Exception as e:
    conn.rollback()
    print(f'❌ Write failed: {e}')
    raise

finally:
    cur.close()
    conn.close()


results = analyze_tournament(df, "The Genesis Invitational")

sys.stdout = sys.__stdout__
_log_file.close()
print(f"Log saved to: {_log_path}")