#!/usr/bin/env python
# coding: utf-8
"""
Strokes Gained analysis / composite player ranking.

Converted from StrokesGainedAnalysisv2.ipynb and refactored into functions
with a main() entry point.

Pipeline:
  1. Pull leaderboard + SG data, build rolling SG averages per player
  2. Pull DataGolf ranks, join to the latest SG row per player
  3. Build finish-position metrics (avg finish, cut %, top-N counts)
  4. Merge, score each player with a weighted composite of percentile ranks
  5. Print tier analysis / optimal picks from the player list CSV
  6. Write the scored dataset back to Postgres

Usage:
  python StrokesGainedAnalysisv2.py
  python StrokesGainedAnalysisv2.py --player "Kurt Kitayama" --tournament "Arnold Palmer Invitational"
  python StrokesGainedAnalysisv2.py --tier 3
  python StrokesGainedAnalysisv2.py --no-write
"""

# Improvements to the script
# Need a way to investigate top rated golfers - DONE
# want to see a print out of top golfers and their assigned tier - DONE
# might need to put a min threshold on the amount of events, current analysis bias towards less events (done must at least 5) - DONE
# want to have a print out of past standings for an event
# want to see the x last finishes for a golfer by search - DONE
# eventually need to track results - success of picks
# need to add how many top 3 , 5 and 10 to part of the stats and less sg

# New March 2026
# Add DG ranks to output
# Want to see a trend/moving average of last X events to show improvment or decline over time
# Add query to pull past event score
# Add code to github
# Remove from jupyter notebook and make it a standalone script - DONE

import argparse
import os
import sys

import numpy as np
import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_values

from player_names import canonical_display_name, normalize_player_name

# ── Configuration ──────────────────────────────────────────────────────────

# How to handle missed cuts when converting finish position to a number.
CUT_HANDLING = "penalty"  # Options: "penalty", "skip", "field_size"
CUT_PENALTY_POSITION = 80  # Position assigned to cuts when using "penalty" method

# Rolling SG windows (fibonacci-ish) built per player.
SG_WINDOWS = [1, 2, 3, 5, 8, 13, 21, 34, 55, 89]

# How many seasons count as "recent" for the form metrics and the event minimum.
# This used to be the previous calendar year alone, which made the current
# season invisible: a player who had just turned pro was judged on a handful of
# starts from last year and dropped from the scored dataset entirely, however
# much he had played this year.
RECENT_SEASONS = 2

# Minimum events played in the recent seasons to be included.
MIN_EVENTS_TIER_ANALYSIS = 4  # tier analysis / optimal picks use > 4
MIN_EVENTS_PROFILE = 3        # golfer profiles / db write use > 3

# Composite score weights.
# First version put too much weight on the most recent events. That is a problem
# because some events have worse fields, which gives less competitive fields an
# advantage.
WEIGHTS = {
    'SG_last_1_percentile'                   : 0.75,
    'SG_last_2_percentile'                   : 0.75,
    'SG_last_3_percentile'                   : 0.75,
    'SG_last_5_percentile'                   : 0.75,
    'SG_last_8_percentile'                   : 0.5,
    'SG_last_13_percentile'                  : 0.5,
    'SG_last_21_percentile'                  : 0.5,
    'SG_last_34_percentile'                  : 0.25,
    'SG_last_55_percentile'                  : 0.25,
    'dg_index_percentile'                    : 0.8,
    'owgr_rank_percentile'                   : 0.5,
    'last_3_avg_position_percentile'         : 0.5,
    'last_5_avg_position_percentile'         : 0.75,
    'last_10_avg_position_percentile'        : 0.5,
    'cut_percentage_last_year_percentile'    : 0.75,
    'top_5_percentage_last_year_percentile'  : 0.75,
    'top_10_percentage_last_year_percentile' : 0.5,
    'top_20_percentage_last_year_percentile' : 0.5,
    'top_5_last_5_percentile'                : 1,
    'top_10_last_5_percentile'               : 1.25,
    'top_20_last_5_percentile'               : 0.5,
    'top_5_last_10_percentile'               : 1,
    'top_10_last_10_percentile'              : 1.25,
    'top_20_last_10_percentile'              : 0.5,
}

# Event importance weights. Bigger events count for more when averaging across
# starts, so a good week in a weak field can't outrank a good week at a major.
EVENT_WEIGHTS = {
    'major'     : 2.0,
    'signature' : 1.5,
    'regular'   : 1.0,
}

# Events whose source flags say 'N' but that deserve signature-level weight.
# Matched case-insensitively on tournament name.
ELEVATED_TO_SIGNATURE = {
    'THE PLAYERS CHAMPIONSHIP',
}

# Destination table for the scored dataset.
TABLE_NAME = 'combined_data'
IF_EXISTS = 'replace'  # 'replace' drops & recreates | 'append' adds rows


def load_config():
    """Load .env settings and return (db_config, schema, player_list_csv_path)."""
    load_dotenv()

    db_config = {
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'port': os.getenv('DB_PORT', '5432')
    }
    return db_config, os.getenv('DB_SCHEMA'), os.getenv("PLAYER_LIST_CSV_PATH")


def connect(db_config):
    """Open a connection, reporting success/failure the same way the other scripts do."""
    try:
        conn = psycopg2.connect(**db_config)
        print("✅ Database connection successful!")
        print(f"   Connected to: {db_config['database']} on {db_config['host']}")
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        raise


# ── Strokes Gained ─────────────────────────────────────────────────────────

# Strokes gained assumed for a leaderboard row with no matching sg_data row.
MISSING_SG_FALLBACK = -2


def _read_query(conn, query):
    """Run a SELECT and return the result as a DataFrame."""
    cur = conn.cursor()
    cur.execute(query)
    rows = cur.fetchall()
    colnames = [desc[0] for desc in cur.description]
    cur.close()

    return pd.DataFrame(rows, columns=colnames)


def fetch_sg_data(conn):
    """Leaderboard rows joined to SG data, one row per player/event.

    The join is done here rather than in SQL because the two tables spell
    amateurs differently: the leaderboard tags them "(a)" and sg_data never
    does. Matching on the raw name left every one of those rows without a
    strokes-gained match, which the old SQL then filled in with
    MISSING_SG_FALLBACK — so the marker did not merely lose data, it invented
    a bad result and fed it into every rolling average. Folding both sides
    through normalize_player_name() first is the same approach
    build_sg_percentiles() already uses for the DataGolf join.
    """
    lb = _read_query(conn, ''' select player , pos , to_par
    , year , week_of_season , tournament , course
    , signature_event , major_event
    from pga_stats.leaderboard_data
    ''')

    sg = _read_query(conn, ''' select player , year , week_of_season
    , avg , total_sg_t , total_sg_t2g , total_sg_p , measured_rounds
    from pga_stats.sg_data
    ''')

    lb['player'] = canonical_display_name(lb['player'])
    lb['_match_name'] = normalize_player_name(lb['player'])

    # One SG row per player/event. Duplicates would fan a leaderboard row out
    # into several rows and silently double-count the player.
    sg['_match_name'] = normalize_player_name(sg['player'])
    sg = sg.drop(columns='player').drop_duplicates(
        subset=['_match_name', 'year', 'week_of_season'], keep='first')

    df = pd.merge(lb, sg, how='left', on=['_match_name', 'year', 'week_of_season'])
    df = df.drop(columns='_match_name')

    # Replaces the old SQL `case when b.avg is null then -2 else b.avg end`.
    df['updated_avg'] = pd.to_numeric(df['avg'], errors='coerce').fillna(
        MISSING_SG_FALLBACK)

    return df


def add_event_weights(df):
    """Attach a per-event importance weight from the major/signature flags."""
    def _is_y(col):
        return df[col].fillna('N').astype(str).str.strip().str.upper().eq('Y')

    major = _is_y('major_event')
    signature = _is_y('signature_event')
    elevated = (df['tournament'].fillna('').astype(str)
                  .str.strip().str.upper().isin(ELEVATED_TO_SIGNATURE))

    weights = pd.Series(EVENT_WEIGHTS['regular'], index=df.index, dtype=float)
    weights[signature | elevated] = EVENT_WEIGHTS['signature']
    weights[major] = EVENT_WEIGHTS['major']  # major wins if both flags are set

    df['event_weight'] = weights
    return df


def add_rolling_sg(df):
    """Add SG_last_N rolling averages per player, chronologically.

    Averages are weighted by event importance, so majors and signature events
    pull the average harder than a regular tour stop does.
    """
    df = add_event_weights(df)
    df = df.sort_values(by=['player', 'year', 'week_of_season'])

    # psycopg2 hands back numeric columns as decimal.Decimal, which won't
    # multiply against a float weight. The old plain .rolling().mean() coerced
    # these for us; now we have to do it ourselves.
    df['updated_avg'] = pd.to_numeric(df['updated_avg'], errors='coerce')

    df['_weighted_sg'] = df['updated_avg'] * df['event_weight']

    for window in SG_WINDOWS:
        weighted_sum = (
            df.groupby('player')['_weighted_sg']
              .transform(lambda x, w=window: x.rolling(w, min_periods=1).sum())
        )
        weight_sum = (
            df.groupby('player')['event_weight']
              .transform(lambda x, w=window: x.rolling(w, min_periods=1).sum())
        )
        df[f'SG_last_{window}'] = weighted_sum / weight_sum

    return df.drop(columns=['_weighted_sg']).round(3)


def fetch_datagolf_ranks(conn):
    """Current DataGolf rankings."""
    return pd.read_sql_query("SELECT * FROM pga_stats.datagolf_ranks;", conn)


def build_sg_percentiles(sg_df, ranks_df):
    """Latest SG row per player joined to DG ranks, with percentile ranks added."""
    latest = sg_df.groupby('player').tail(1).reset_index(drop=True)
    latest['_match_name'] = normalize_player_name(latest['player'])

    # Match on the folded name rather than the raw string. Duplicates are dropped
    # so a repeated DataGolf entry can't fan a player out into several rows.
    ranks = ranks_df.copy()
    ranks['_match_name'] = normalize_player_name(ranks['player_name'])
    ranks = ranks[ranks['_match_name'] != ''].drop_duplicates(
        subset='_match_name', keep='first')

    joined_df = pd.merge(latest, ranks, how='left', on='_match_name')
    joined_df = joined_df.drop(columns='_match_name')

    # Percentile Ranks
    sg_cols = [f'SG_last_{w}' for w in SG_WINDOWS] + ['dg_index']
    for col in sg_cols:
        joined_df[f'{col}_percentile'] = joined_df[col].rank(pct=True, ascending=True)

    # Descending order so the best performer lands in the 100th percentile
    joined_df['owgr_rank_percentile'] = joined_df['owgr_rank'].rank(pct=True, ascending=False)

    return joined_df.round(4)


# ── Finish position metrics ────────────────────────────────────────────────

def fetch_leaderboard(conn):
    """Raw leaderboard results, sorted chronologically per player."""
    query = '''select player , pos , to_par , official_money
    , year , tournament , week_of_season , course
    , signature_event , major_event
    from pga_stats.leaderboard_data ;'''

    df = pd.read_sql_query(query, conn)

    # Same fold as fetch_sg_data, so groupby('player'), the tier-list merge and
    # the golfer_profile lookup all see one identity per player rather than
    # separate careers for the amateur and professional spellings.
    df['player'] = canonical_display_name(df['player'])

    return df.sort_values(by=['player', 'year', 'week_of_season'])


def clean_position(pos):
    """Convert position to numeric, handling string positions like "T5", "T10", etc."""
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


def build_position_metrics(lb_df):
    """Per-player finish metrics: avg finish, cut %, and top-N counts.

    Average finish is weighted by event importance; the cut/top-N rates and
    counts stay unweighted.

    The cut/top-N metrics and the tournaments_last_year count cover the last
    RECENT_SEASONS seasons. Those column names say "last_year" for backwards
    compatibility with the combined_data table — the window is wider than the
    names suggest.
    """
    lb_df['position_numeric'] = lb_df['pos'].apply(clean_position)
    lb_df = add_event_weights(lb_df)

    # Sort by player and then by week/year to get chronological order
    df_sorted = lb_df.sort_values(['player', 'year', 'week_of_season'],
                                  ascending=[True, True, True])

    results = []

    # The recent window spans the current season and the ones before it, so a
    # player's results this year count toward his form and his event minimum.
    current_year = lb_df['year'].max()  # Use the most recent year in the data
    recent_years = range(current_year - RECENT_SEASONS + 1, current_year + 1)

    for player, group in df_sorted.groupby('player'):
        # Get all tournament entries (including cuts) in chronological order
        all_entries = group.dropna(subset=['pos']).copy()

        # Filter to the recent seasons for the cut/top-N calculations
        recent_entries = all_entries[all_entries['year'].isin(recent_years)]

        # Calculate cut percentage over the recent seasons
        if len(recent_entries) > 0:
            cuts_made = len(recent_entries[~recent_entries['pos'].str.upper().isin(['CUT', 'MC'])])
            total_tournaments = len(recent_entries)
            cut_percentage = (cuts_made / total_tournaments) * 100
        else:
            cut_percentage = np.nan
            total_tournaments = 0

        # Numeric positions for averaging (excludes NaN from cuts under "skip").
        # Kept as a filtered frame so the event weights stay row-aligned.
        pos_rows = group[group['position_numeric'].notna()]
        positions = pos_rows['position_numeric'].tolist()
        pos_weights = pos_rows['event_weight'].tolist()

        if len(positions) == 0:
            continue

        recent_positions = recent_entries[recent_entries['position_numeric'].notna()]['position_numeric'].tolist()

        top_5_count = len([pos for pos in recent_positions if pos <= 5])
        top_10_count = len([pos for pos in recent_positions if pos <= 10])
        top_20_count = len([pos for pos in recent_positions if pos <= 20])

        # Percentages based on tournaments finished (not including cuts)
        num_recent_positions = len(recent_positions)
        if num_recent_positions > 0:
            top_5_percentage = (top_5_count / num_recent_positions) * 100
            top_10_percentage = (top_10_count / num_recent_positions) * 100
            top_20_percentage = (top_20_count / num_recent_positions) * 100
        else:
            top_5_percentage = 0.0
            top_10_percentage = 0.0
            top_20_percentage = 0.0

        def weighted_avg_position(n):
            return np.average(positions[-n:], weights=pos_weights[-n:])

        last_3_avg = weighted_avg_position(3)
        last_5_avg = weighted_avg_position(5)
        last_10_avg = weighted_avg_position(10)

        # Top-N counts stay unweighted — they're counts, not averages.
        last_5_pos = positions[-5:]
        last_10_pos = positions[-10:]

        results.append({
            'player': player,
            'total_tournaments': len(all_entries),
            'games_with_positions': len(positions),
            'tournaments_last_year': total_tournaments,
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
            'top_5_last_5'   : sum(1 for p in last_5_pos if p <= 5),
            'top_10_last_5'  : sum(1 for p in last_5_pos if p <= 10),
            'top_20_last_5'  : sum(1 for p in last_5_pos if p <= 20),
            # top finish counts — last 10 starts
            'top_5_last_10'  : sum(1 for p in last_10_pos if p <= 5),
            'top_10_last_10' : sum(1 for p in last_10_pos if p <= 10),
            'top_20_last_10' : sum(1 for p in last_10_pos if p <= 20),
        })

    # Sort by best average position over last 5 games (lower is better)
    return pd.DataFrame(results).sort_values('last_5_avg_position')


def add_position_percentiles(results_df):
    """Percentile-rank the finish metrics. Lower avg finish = better = higher percentile."""
    df = results_df.copy()

    for col in ['last_3_avg_position', 'last_5_avg_position', 'last_10_avg_position']:
        df[f'{col}_percentile'] = df[col].rank(pct=True, ascending=False)

    for col in ['cut_percentage_last_year',
                'top_5_percentage_last_year', 'top_10_percentage_last_year',
                'top_20_percentage_last_year',
                'top_5_last_5', 'top_10_last_5', 'top_20_last_5',
                'top_5_last_10', 'top_10_last_10', 'top_20_last_10']:
        df[f'{col}_percentile'] = df[col].rank(pct=True, ascending=True)

    return df


def print_top_players(results_df, top_n=20):
    """Print the leaders by average finish position over the last 5 starts."""
    for _, row in results_df.head(top_n).iterrows():
        last_3 = f"{row['last_3_avg_position']:.2f}" if row['last_3_avg_position'] is not None else "N/A"
        last_5 = f"{row['last_5_avg_position']:.2f}" if row['last_5_avg_position'] is not None else "N/A"
        cut_pct = f"{row['cut_percentage_last_year']:.1f}%" if row['cut_percentage_last_year'] is not None else "N/A"
        tournaments = f"{row['tournaments_last_year']}"

        print(f"{row['player']:<25} {tournaments:<12} {cut_pct:<8} {last_3:<12} {last_5:<12}")


# ── Composite score ────────────────────────────────────────────────────────

def normalized_weights():
    """Weights scaled so they sum to 1."""
    total_weight = sum(WEIGHTS.values())
    return {col: w / total_weight for col, w in WEIGHTS.items()}


def print_normalized_weights(norm_weights):
    print("Normalized weights (should sum to 1.0):")
    for col, weight in norm_weights.items():
        print(f"{col}: {weight:.3f}")
    print(f"Total: {sum(norm_weights.values()):.3f}")
    print()

    print("Event importance weights (applied when averaging across starts):")
    for event_type, weight in EVENT_WEIGHTS.items():
        print(f"{event_type}: {weight}")
    for tournament in sorted(ELEVATED_TO_SIGNATURE):
        print(f"{tournament} (override): {EVENT_WEIGHTS['signature']}")
    print()


def build_scored_dataset(joined_df, filtered_df, min_events, score_col):
    """Merge SG + finish metrics, filter thin schedules, and score each player."""
    merged = pd.merge(joined_df, filtered_df, on='player', how='left')
    merged = merged[merged['tournaments_last_year'] > min_events]

    norm_weights = normalized_weights()

    def _composite(row):
        return sum(
            (row[col] if not pd.isna(row.get(col)) else 0) * nw
            for col, nw in norm_weights.items()
            if col in row.index
        )

    merged[score_col] = merged.apply(_composite, axis=1)
    merged['composite_percentile_rank'] = merged[score_col].rank(pct=True, ascending=True)

    return merged.round(4)


# ── Tier analysis ──────────────────────────────────────────────────────────

def load_tier_list(player_list_csv_path):
    """Player list with tier assignments for the current event."""
    return pd.read_csv(player_list_csv_path)


def print_tier_analysis(playerlist_metrics):
    """Rank the players in each tier by composite score."""
    print("\n=== TOURNAMENT TIER ANALYSIS ===")
    print("Best players in each Tier (by composite score):")
    print("=" * 70)

    for tier_num in sorted(playerlist_metrics['Tier'].unique()):
        tier_players = playerlist_metrics[playerlist_metrics['Tier'] == tier_num].sort_values(
            'weighted_composite', ascending=False)

        print(f"\nTIER {tier_num} (Top 5):")
        print(f"{'Rank':<4} {'Player':<25} {'Composite':<12} {'Percentile':<12}")
        print("-" * 55)

        for i, (_, player) in enumerate(tier_players.iterrows(), 1):
            comp_score = f"{player['weighted_composite']:.4f}" if not pd.isna(player['weighted_composite']) else "N/A"
            pct_rank = f"{player['composite_percentile_rank']:.4f}%" if not pd.isna(player['composite_percentile_rank']) else "N/A"
            print(f"{i:<4} {player['player']:<25} {comp_score:<12} {pct_rank:<12}")


def print_optimal_picks(playerlist_metrics):
    """Best player from each tier, plus the resulting team score."""
    print("\n=== OPTIMAL PICKS FOR COMPETITION ===")
    print("Best player from each tier:")
    print("=" * 60)

    optimal_picks = []
    total_composite = 0

    for tier_num in sorted(playerlist_metrics['Tier'].unique()):
        tier_players = playerlist_metrics[playerlist_metrics['Tier'] == tier_num].sort_values(
            'weighted_composite', ascending=False)
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
    print(f"Average Team Composite Score: {total_composite/6:.4f}")  # 6 picks per team

    return optimal_picks


# ── Golfer profile ─────────────────────────────────────────────────────────

def golfer_profile(
    player_name,
    lb_df,
    merged_dataset,
    tournament_name=None,
    last_n_finishes=15,
    min_events=5,
):
    """
    Print a full scouting report for a given player.

    Parameters
    ----------
    player_name     : str        — player name (case-insensitive)
    lb_df           : DataFrame  — leaderboard results
    merged_dataset  : DataFrame  — scored dataset with percentile columns
    tournament_name : str        — optional event name to pull historical finishes
    last_n_finishes : int        — how many recent results to display
    min_events      : int        — minimum events required to show percentile rankings
    """
    name_lower = player_name.strip().lower()
    lb = lb_df[lb_df['player'].str.lower() == name_lower].copy()

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
    dg_rank_rows = merged_dataset.loc[merged_dataset['player'] == canonical_name]
    if not dg_rank_rows.empty:
        print('datagolf ranking: ' + str(dg_rank_rows['dg_rank'].iloc[0]))
        print(dg_rank_rows[['dg_rank']].to_string(index=False))

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

    last3_avg = _avg(valid_pos, 3)
    last5_avg = _avg(valid_pos, 5)
    last10_avg = _avg(valid_pos, 10)

    print(f"  Last  3 starts : {last3_avg or 'N/A'}")
    print(f"  Last  5 starts : {last5_avg or 'N/A'}")
    print(f"  Last 10 starts : {last10_avg or 'N/A'}")
    print()
    print(f"  {'Metric':<25} {'Last 5':>8} {'Last 10':>8}")
    print(f"  {'-'*42}")
    print(f"  {'Avg Finish':<25} {str(last5_avg) if last5_avg is not None else 'N/A':>8} {str(last10_avg) if last10_avg is not None else 'N/A':>8}")
    print(f"  {'Top 3 Finishes':<25} {_top_count(valid_pos, 5, 3):>8} {_top_count(valid_pos, 10, 3):>8}")
    print(f"  {'Top 5 Finishes':<25} {_top_count(valid_pos, 5, 5):>8} {_top_count(valid_pos, 10, 5):>8}")
    print(f"  {'Top 10 Finishes':<25} {_top_count(valid_pos, 5, 10):>8} {_top_count(valid_pos, 10, 10):>8}")

    print(f"\n  {'Year':<8} {'Events':<8} {'Cuts Made':<12} {'Cut %':<9} {'Avg Finish':<12} {'Top 5':<7} {'Top 10':<7} {'Top 20'}")
    print('  ' + '-' * 60)
    for year, grp in lb.groupby('year', sort=True):
        total_ev = len(grp)
        cuts_miss = grp['pos'].str.upper().isin(['CUT', 'MC']).sum()
        cuts_made = total_ev - cuts_miss
        cut_pct = round(cuts_made / total_ev * 100, 1) if total_ev else 0
        num_pos = grp['_pos_num'].dropna()
        avg_fin = round(num_pos.mean(), 1) if not num_pos.empty else 'N/A'
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
        total_ev = len(lb)
        cuts_miss = lb['pos'].str.upper().isin(['CUT', 'MC']).sum()
        print(f'\n  Career Events    : {total_ev}')
        print(f'  Career Cuts Miss : {cuts_miss}  ({round(cuts_miss/total_ev*100,1) if total_ev else 0}%)')
        print(f'  Career Avg Finish: {round(all_valid.mean(), 1) if not all_valid.empty else "N/A"}')

    print('\n' + '═' * 65 + '\n')


def profile_tier(tier, tier_df, lb_df, merged_dataset, last_n_finishes=15):
    """Print a profile for every player in the given tier that has scored data."""
    players = tier_df[tier_df['Tier'] == tier]['player'].tolist()

    for name in players:
        if merged_dataset[merged_dataset['player'].str.lower() == name.lower()].shape[0] > 0:
            golfer_profile(name, lb_df, merged_dataset, last_n_finishes=last_n_finishes)
        else:
            print(f"\n⚠️  Skipping '{name}' — no scored data.\n")


# ── Write results back to Postgres ─────────────────────────────────────────

def pg_dtype(series):
    """Map a pandas dtype to a Postgres column type."""
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


def write_to_postgres(merged_dataset, db_config, schema):
    """Write the scored dataset to <schema>.combined_data."""
    full_table = f'{schema}.{TABLE_NAME}'

    # ── 1. Clean the dataframe before writing ──────────────────────────────
    df_out = merged_dataset.copy()

    # Replace NaN/inf with None so psycopg2 writes proper NULLs
    df_out = df_out.replace([np.inf, -np.inf], np.nan)

    for col in df_out.columns:
        if pd.api.types.is_datetime64_any_dtype(df_out[col]):
            df_out[col] = df_out[col].astype(object).where(df_out[col].notna(), None)

    df_out = df_out.where(pd.notnull(df_out), None)

    # ── 2. Build CREATE TABLE statement from dtypes ────────────────────────
    col_defs = ',\n    '.join(
        f'"{col}" {pg_dtype(df_out[col])}' for col in df_out.columns
    )

    # ── 3. Write to Postgres ───────────────────────────────────────────────
    conn = psycopg2.connect(**db_config)
    cur = conn.cursor()

    try:
        if IF_EXISTS == 'replace':
            cur.execute(f'DROP TABLE IF EXISTS {full_table};')
            cur.execute(f'CREATE TABLE {full_table} (\n    {col_defs}\n);')
            print(f'✅ Table {full_table} (re)created — {len(df_out.columns)} columns')

        elif IF_EXISTS == 'append':
            # Table must already exist with matching columns
            pass

        # Batch insert using execute_values for speed
        cols = [f'"{c}"' for c in df_out.columns]
        col_str = ', '.join(cols)
        rows = [tuple(row) for row in df_out.itertuples(index=False, name=None)]

        execute_values(
            cur,
            f'INSERT INTO {full_table} ({col_str}) VALUES %s',
            rows,
            page_size=500
        )

        conn.commit()
        print(f'✅ {len(rows):,} rows written to {full_table}')

    except Exception as e:
        conn.rollback()
        print(f'❌ Write failed: {e}')
        raise

    finally:
        cur.close()
        conn.close()


# ── Entry point ────────────────────────────────────────────────────────────

def parse_args():
    parser = argparse.ArgumentParser(description=__doc__.split('\n')[1])
    parser.add_argument('--player', help='Print a scouting report for this player')
    parser.add_argument('--tournament', help='Event history to include in --player report')
    parser.add_argument('--tier', type=int, help='Print reports for every player in this tier')
    parser.add_argument('--last-n', type=int, default=15,
                        help='Recent finishes to show in a report (default: 15)')
    parser.add_argument('--top-n', type=int, default=20,
                        help='Players to list by avg finish (default: 20)')
    parser.add_argument('--no-write', action='store_true',
                        help=f'Skip writing results to {TABLE_NAME}')
    parser.add_argument('--show-weights', action='store_true',
                        help='Print the normalized composite weights')
    return parser.parse_args()


def main():
    # Output is full of emoji and box-drawing characters. On Windows stdout
    # defaults to cp1252 when redirected to a file or pipe, which raises
    # UnicodeEncodeError, so force UTF-8.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            stream.reconfigure(encoding='utf-8')

    args = parse_args()
    db_config, schema, player_list_csv_path = load_config()

    conn = connect(db_config)
    try:
        # ── Strokes gained ────────────────────────────────────────────────
        sg_df = add_rolling_sg(fetch_sg_data(conn))
        ranks_df = fetch_datagolf_ranks(conn)
        joined_df = build_sg_percentiles(sg_df, ranks_df)

        # ── Finish positions ──────────────────────────────────────────────
        lb_df = fetch_leaderboard(conn)
        results_df = build_position_metrics(lb_df)
        filtered_df = add_position_percentiles(results_df)
    finally:
        conn.close()

    print_top_players(results_df, top_n=args.top_n)

    if args.show_weights:
        print_normalized_weights(normalized_weights())

    # ── Tier analysis uses the stricter event minimum ─────────────────────
    tier_dataset = build_scored_dataset(
        joined_df, filtered_df, MIN_EVENTS_TIER_ANALYSIS, 'weighted_composite')
    composite_with_percentile = tier_dataset[
        ['player', 'weighted_composite', 'composite_percentile_rank']].copy()

    tier_df = load_tier_list(player_list_csv_path)
    playerlist_metrics = pd.merge(
        tier_df,
        composite_with_percentile,
        on='player',
        how='left'  # Keep all tier players, even if no composite score
    )

    print_tier_analysis(playerlist_metrics)
    print_optimal_picks(playerlist_metrics)

    # ── Profiles and the db write use the looser event minimum ────────────
    merged_dataset = build_scored_dataset(
        joined_df, filtered_df, MIN_EVENTS_PROFILE, 'composite_score')
    print(f'✅ merged_dataset ready — {len(merged_dataset):,} players')

    if args.player:
        golfer_profile(
            args.player,
            lb_df,
            merged_dataset,
            tournament_name=args.tournament,
            last_n_finishes=args.last_n,
        )

    if args.tier is not None:
        profile_tier(args.tier, tier_df, lb_df, merged_dataset,
                     last_n_finishes=args.last_n)

    if not args.no_write:
        write_to_postgres(merged_dataset, db_config, schema)


if __name__ == '__main__':
    main()


'''
Model improvement ideas:
- Pull in historical finishing position for the current event; add it as a metric
- are there other metrics that could be pulled in; driving accurary; breaking out SGs
- Fix Ludgid Aberg's name
- Need to create a program that prints out the predicted score for the tie breaker for each round
- analyze_tournament() was called in the notebook but never defined; write it or drop the idea
'''
