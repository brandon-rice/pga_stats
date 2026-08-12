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


def normalize_player_name(names):
    """Fold a Series of player names to a common matching key.

    Returns a Series of lowercase, letters-only keys. Missing or empty names
    fold to '', which matches nothing.
    """
    cleaned = names.fillna('').astype(str)

    # Collapse embedded newlines/runs of whitespace, then drop the "(a)" marker.
    cleaned = cleaned.str.replace(r'\s+', ' ', regex=True).str.strip()
    cleaned = cleaned.str.replace(r'\s*\(a\)$', '', regex=True, flags=re.IGNORECASE)

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
