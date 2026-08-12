#!/usr/bin/env python
# coding: utf-8
"""
Shared player-name normalization.

The sources spell the same player differently, so any join or grouping keyed on
a raw name silently splits or drops players:

  - DataGolf strips accents ("Ludvig Aberg", "Thorbjorn Olesen") while the
    leaderboard keeps them ("Ludvig Åberg", "Thorbjørn Olesen")
  - Leaderboard names carry an amateur marker, sometimes after an embedded
    newline ("Luke Clanton (a)", "Luke Clanton\\n(a)")
  - Separators vary ("A.J. Ewart" vs "AJ Ewart", "Sang-hee Lee" vs "Sang Hee Lee")

Folding both sides through normalize_player_name() before matching fixes all of
these. It deliberately does NOT resolve nickname/formal-name pairs such as
"Sam Stevens" vs "Samuel Stevens" — those need a curated alias list, because
fuzzy matching is unsafe here: "Cameron Young" and "Carson Young" are 0.87
similar and are two different players.

Usage:
  from player_names import normalize_player_name, normalize_one

  df['player_key'] = normalize_player_name(df['player'])
  key = normalize_one('Ludvig Åberg (a)')   # -> 'ludvigaberg'

  df['player'] = canonical_display_name(df['player'])   # -> 'Ludvig Åberg'
"""

import re
import unicodedata

import pandas as pd

# Letters that unicodedata NFKD will not decompose into ASCII on its own. Without
# these, "Højgaard" folds to "Hjgaard" and stops matching DataGolf's "Hojgaard".
NAME_CHAR_MAP = {
    'ø': 'o', 'Ø': 'O',
    'æ': 'ae', 'Æ': 'AE',
    'œ': 'oe', 'Œ': 'OE',
    'ð': 'd', 'Ð': 'D',
    'đ': 'd', 'Đ': 'D',
    'ł': 'l', 'Ł': 'L',
    'þ': 'th', 'Þ': 'Th',
    'ß': 'ss',
    'ı': 'i',
}


def _strip_amateur_marker(names):
    """Collapse whitespace runs and drop a trailing "(a)" amateur marker.

    Shared by normalize_player_name() and canonical_display_name() so the two
    cannot disagree about which names are the same player.
    """
    cleaned = names.fillna('').astype(str)
    cleaned = cleaned.str.replace(r'\s+', ' ', regex=True).str.strip()
    return cleaned.str.replace(r'\s*\(a\)$', '', regex=True, flags=re.IGNORECASE)


def canonical_display_name(names):
    """Fold a Series of names to one printable spelling per player.

    Same identity rule as normalize_player_name(), but the result stays
    human-readable: accents and capitalization survive, only the whitespace
    noise and the amateur marker are removed.

    The leaderboard files amateurs as "Luke Clanton (a)", and sometimes with an
    embedded newline as "Luke Clanton\\n(a)", so one player can occupy three
    identities. sg_data carries no "(a)" rows at all, so those leaderboard rows
    find no strokes-gained match and get scored with a fabricated fallback.
    Folding the marker off before any join or groupby merges the split career.
    """
    return _strip_amateur_marker(names)


def normalize_player_name(names):
    """Fold a Series of player names to a common matching key.

    Returns a Series of lowercase, letters-only keys. Missing or empty names
    fold to '', which matches nothing.
    """
    cleaned = _strip_amateur_marker(names)

    for src, dst in NAME_CHAR_MAP.items():
        cleaned = cleaned.str.replace(src, dst, regex=False)

    cleaned = (cleaned.map(lambda s: unicodedata.normalize('NFKD', s))
                      .str.encode('ascii', 'ignore').str.decode('ascii'))

    # Separators vary between the sources, so drop everything that isn't a letter.
    cleaned = cleaned.str.replace(r'[^a-zA-Z]', '', regex=True)

    return cleaned.str.casefold()


def normalize_one(name):
    """Fold a single player name, for callers working with one string at a time."""
    return normalize_player_name(pd.Series([name])).iloc[0]
