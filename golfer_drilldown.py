#!/usr/bin/env python
# coding: utf-8
"""
Golfer drill-down — inspect the numbers behind a player's composite score.

This is the read-only companion to StrokesGainedAnalysisv2.py. That script
rebuilds the whole pipeline and rewrites pga_stats.combined_data; this one only
reads what the last run produced, so it starts in about a second and can be run
as often as you like without touching the database.

For each golfer you get the last N finishes from the leaderboard, average finish
over the last 3/5/10 starts, and the strokes-gained and finish-position
percentiles with the raw value beside each one.

  --tier N        every golfer in that tier: comparison table, then a profile each
  --player NAME   one golfer
  --list          the tier roster with composite scores, nothing else

Usage:
  python golfer_drilldown.py --tier 3
  python golfer_drilldown.py --player "Jackson Koivun"
  python golfer_drilldown.py --player "Ben Griffin" --tournament "Wyndham"
  python golfer_drilldown.py --tier 4 --table-only
  python golfer_drilldown.py --list

Run StrokesGainedAnalysisv2.py first if combined_data is stale — this script
reports how old the scored data is so a stale table can't quietly mislead you.
"""

import argparse
import sys
import warnings

import pandas as pd

# pandas warns that psycopg2 connections are not SQLAlchemy engines on every
# read_sql_query call. The queries work; the warning just buries the output.
warnings.filterwarnings(
    'ignore', message='pandas only supports SQLAlchemy connectable')

from StrokesGainedAnalysisv2 import (
    TABLE_NAME,
    connect,
    fetch_leaderboard,
    golfer_profile,
    load_config,
    load_tier_list,
)

# Columns shown in the tier comparison table: (header, column, width, format).
# Kept narrow enough that the whole table fits an 80-column terminal.
TIER_TABLE_COLUMNS = [
    ('Composite', 'composite_score',      9, '{:.4f}'),
    ('Pctile',    'composite_percentile_rank', 6, '{:.3f}'),
    ('DG',        'dg_rank',              4, '{:.0f}'),
    ('Evts',      'tournaments_last_year', 4, '{:.0f}'),
    ('AvgF5',     'last_5_avg_position',  6, '{:.1f}'),
    ('Cut%',      'cut_percentage_last_year', 5, '{:.0f}'),
    ('T10/10',    'top_10_last_10',       6, '{:.0f}'),
    ('SG5',       'SG_last_5',            6, '{:.2f}'),
]


def fetch_scored_dataset(conn, schema):
    """The scored dataset from the last StrokesGainedAnalysisv2.py run."""
    return pd.read_sql_query(f'SELECT * FROM {schema}.{TABLE_NAME};', conn)


def _fmt(row, column, width, spec):
    """Format one cell, leaving a dash where the metric is missing."""
    value = row.get(column)
    if value is None or pd.isna(value):
        return f'{"-":>{width}}'
    return f'{spec.format(float(value)):>{width}}'


def print_tier_table(tier, players, scored):
    """Side-by-side comparison of every golfer in one tier.

    The per-golfer profiles that follow are detailed but long; this is the view
    that lets you compare a tier at a glance and decide who deserves the closer
    read.
    """
    merged = pd.DataFrame({'player': players}).merge(scored, on='player', how='left')
    merged = merged.sort_values('composite_score', ascending=False, na_position='last')

    header = f"  {'#':<3} {'Player':<24}" + ''.join(
        f' {name:>{width}}' for name, _, width, _ in TIER_TABLE_COLUMNS)

    print(f'\n{"═" * len(header)}')
    print(f'  TIER {tier} — {len(merged)} golfers, best composite first')
    print('═' * len(header))
    print(header)
    print('  ' + '-' * (len(header) - 2))

    for i, (_, row) in enumerate(merged.iterrows(), 1):
        cells = ''.join(
            ' ' + _fmt(row, column, width, spec)
            for _, column, width, spec in TIER_TABLE_COLUMNS)
        print(f'  {i:<3} {str(row["player"])[:24]:<24}{cells}')

    missing = merged[merged['composite_score'].isna()]['player'].tolist()
    if missing:
        print(f'\n  ⚠️  No scored data for: {", ".join(missing)}')
        print('     Either below the event minimum, or the tier-list spelling does '
              'not match the leaderboard.')

    print('\n  AvgF5 = avg finish last 5 starts (lower better) · Cut% = cuts made '
          'over the recent seasons')
    print('  T10/10 = top-10s in last 10 starts · SG5 = strokes gained, last 5 events')


def print_tier_list(tier_df, scored):
    """Every tier and its golfers with composite scores, as a quick roster."""
    merged = tier_df.merge(scored[['player', 'composite_score']], on='player', how='left')

    for tier in sorted(merged['Tier'].unique()):
        group = merged[merged['Tier'] == tier].sort_values(
            'composite_score', ascending=False, na_position='last')
        print(f'\nTIER {tier}')
        for _, row in group.iterrows():
            score = row['composite_score']
            shown = f'{score:.4f}' if not pd.isna(score) else '   —  '
            print(f'  {shown}  {row["player"]}')


def resolve_player(name, scored):
    """Find a player by case-insensitive exact match, then by substring.

    Typing a full name exactly, accents included, is the main friction in a
    tool meant for quick lookups, so a partial name is accepted as long as it
    picks out exactly one golfer.
    """
    exact = scored[scored['player'].str.lower() == name.strip().lower()]
    if not exact.empty:
        return exact['player'].iloc[0]

    partial = scored[scored['player'].str.contains(name.strip(), case=False, na=False)]
    matches = sorted(partial['player'].unique())

    if len(matches) == 1:
        print(f"  (matched '{name}' → {matches[0]})")
        return matches[0]
    if len(matches) > 1:
        print(f"\n❌  '{name}' is ambiguous — matches: {', '.join(matches)}\n")
        return None

    print(f"\n❌  No scored golfer matches '{name}'.")
    print("    Run with --list to see who is available.\n")
    return None


def parse_args():
    parser = argparse.ArgumentParser(
        description='Inspect the numbers behind a golfer\'s composite score.')
    parser.add_argument('--player', help='Drill into one golfer (partial name is fine)')
    parser.add_argument('--tier', type=int, help='Drill into every golfer in this tier')
    parser.add_argument('--tournament',
                        help='Also show this event\'s history in the profile')
    parser.add_argument('--last-n', type=int, default=15,
                        help='Recent finishes to show (default: 15)')
    parser.add_argument('--table-only', action='store_true',
                        help='With --tier, print the comparison table and stop')
    parser.add_argument('--list', action='store_true',
                        help='List every tier and its golfers, then exit')
    return parser.parse_args()


def main():
    # Same reason as the main script: the box-drawing and emoji output breaks on
    # Windows cp1252 when stdout is redirected to a file or a pipe.
    for stream in (sys.stdout, sys.stderr):
        if hasattr(stream, 'reconfigure'):
            stream.reconfigure(encoding='utf-8')

    args = parse_args()

    if not (args.player or args.tier is not None or args.list):
        print('Nothing to do — pass --player NAME, --tier N, or --list.')
        print('Run with --help for the full set of options.')
        return

    db_config, schema, player_list_csv_path = load_config()

    conn = connect(db_config)
    try:
        scored = fetch_scored_dataset(conn, schema)
        lb_df = fetch_leaderboard(conn)
    finally:
        conn.close()

    if scored.empty:
        print(f'\n❌  {schema}.combined_data is empty. '
              'Run StrokesGainedAnalysisv2.py first.\n')
        return

    # The scored table is a snapshot, so say how current it is. Silently
    # profiling last month's numbers would be worse than a slow script.
    as_of = pd.to_datetime(scored['refresh_date'], errors='coerce').max()
    stamp = as_of.date() if not pd.isna(as_of) else 'unknown'
    print(f'   Scored data: {len(scored):,} golfers, DataGolf ranks as of {stamp}')

    tier_df = load_tier_list(player_list_csv_path)

    if args.list:
        print_tier_list(tier_df, scored)
        return

    if args.tier is not None:
        players = tier_df[tier_df['Tier'] == args.tier]['player'].tolist()
        if not players:
            available = ', '.join(str(t) for t in sorted(tier_df['Tier'].unique()))
            print(f'\n❌  No golfers in tier {args.tier}. Tiers present: {available}\n')
            return

        print_tier_table(args.tier, players, scored)

        if not args.table_only:
            for name in players:
                if (scored['player'] == name).any():
                    golfer_profile(name, lb_df, scored,
                                   tournament_name=args.tournament,
                                   last_n_finishes=args.last_n)
                else:
                    print(f"\n⚠️  Skipping '{name}' — no scored data.\n")

    if args.player:
        canonical = resolve_player(args.player, scored)
        if canonical:
            golfer_profile(canonical, lb_df, scored,
                           tournament_name=args.tournament,
                           last_n_finishes=args.last_n)


if __name__ == '__main__':
    main()
