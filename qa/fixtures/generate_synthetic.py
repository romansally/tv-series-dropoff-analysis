"""Generate synthetic test data for QA/demo purposes.

Creates fabricated episode-level data for all 4 selected shows with
designed rating patterns that exercise the shark-jump detection algorithm.

All data is generated from scratch. No real IMDb data is sampled or used.

Usage:
    python qa/fixtures/generate_synthetic.py
"""

import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config import (
    SHOW_IDS,
    SAMPLE_DIR,
    EPISODES_FILTERED_COLS,
    SHOWS_METADATA_COLS,
    EPISODES_BASICS_COLS,
)

# Fixed seed for deterministic output
RNG = np.random.default_rng(seed=42)

# ─── Show Configurations ─────────────────────────────────────
# Each season tuple: (n_episodes, target_rating, base_votes)
#
# Designed shark-jump outcomes (rolling 3-season avg algorithm):
#   Simpsons:  gradual decline     -> shark-jump at S6
#   SpongeBob: sharp drop after S3 -> shark-jump at S5
#   Family Guy: dip at S3, recovery -> NO shark-jump
#   TWD:       late decline S5-6   -> shark-jump at S5

SHOW_SEASONS = {
    "tt0096697": [  # The Simpsons — 8 seasons, gradual decline
        (8, 8.5, 30000),
        (8, 8.3, 28000),
        (7, 8.0, 25000),
        (7, 7.7, 22000),
        (6, 7.3, 18000),
        (6, 7.0, 15000),
        (5, 6.7, 10000),
        (5, 6.4, 8000),
    ],
    "tt0206512": [  # SpongeBob — 6 seasons, sharp drop after S3
        (8, 8.5, 20000),
        (7, 8.7, 22000),
        (7, 8.8, 25000),
        (6, 6.5, 12000),
        (6, 6.2, 10000),
        (5, 6.0, 8000),
    ],
    "tt0182576": [  # Family Guy — 7 seasons, dip + recovery, NO trigger
        (7, 8.2, 25000),
        (7, 8.5, 27000),
        (6, 7.8, 15000),
        (6, 9.2, 30000),
        (6, 8.3, 22000),
        (5, 8.0, 18000),
        (5, 7.8, 15000),
    ],
    "tt1520211": [  # The Walking Dead — 6 seasons, late decline
        (8, 8.8, 35000),
        (8, 8.5, 32000),
        (7, 8.2, 28000),
        (7, 7.5, 20000),
        (6, 6.5, 12000),
        (6, 6.0, 8000),
    ],
}

# Synthetic show metadata (real titles, simplified other fields)
SHOW_META = {
    "tt0096697": {"start_year": 2000, "end_year": 2008, "genres": "Animation,Comedy"},
    "tt0206512": {
        "start_year": 2001,
        "end_year": 2007,
        "genres": "Animation,Comedy,Family",
    },
    "tt0182576": {"start_year": 2000, "end_year": 2007, "genres": "Animation,Comedy"},
    "tt1520211": {
        "start_year": 2010,
        "end_year": 2016,
        "genres": "Drama,Horror,Thriller",
    },
}


def generate_episodes():
    """Generate all episode rows across all shows and seasons."""
    rows = []
    ep_counter = 1

    for show_tconst, seasons in SHOW_SEASONS.items():
        for season_idx, (n_eps, target_rating, base_votes) in enumerate(
            seasons, start=1
        ):
            # Generate ratings: normal distribution centered on target, clipped to [1,10]
            ratings = RNG.normal(target_rating, 0.3, n_eps)
            ratings = np.clip(np.round(ratings, 1), 1.0, 10.0)

            # Generate votes: uniform in [base-5000, base+5000], floor at 500
            vote_lo = max(500, base_votes - 5000)
            vote_hi = base_votes + 5000
            votes = RNG.integers(vote_lo, vote_hi, n_eps)

            for ep_idx in range(n_eps):
                rows.append(
                    {
                        "episode_tconst": f"tt999{ep_counter:04d}",
                        "show_tconst": show_tconst,
                        "season_num": season_idx,
                        "episode_num": ep_idx + 1,
                        "avg_rating": float(ratings[ep_idx]),
                        "num_votes": int(votes[ep_idx]),
                    }
                )
                ep_counter += 1

    return pd.DataFrame(rows)


def generate_shows_metadata():
    """Generate shows_metadata.csv with real titles, synthetic other fields."""
    rows = []
    for show_tconst, title in SHOW_IDS.items():
        meta = SHOW_META[show_tconst]
        rows.append(
            {
                "show_tconst": show_tconst,
                "primary_title": title,
                "start_year": meta["start_year"],
                "end_year": meta["end_year"],
                "genres": meta["genres"],
            }
        )
    return pd.DataFrame(rows)


def main():
    ep_df = generate_episodes()
    shows_df = generate_shows_metadata()

    # episodes_basics: one row per episode, all tvEpisode
    basics_df = pd.DataFrame(
        {
            "episode_tconst": ep_df["episode_tconst"],
            "title_type": "tvEpisode",
        }
    )

    # Enforce column order
    ep_df = ep_df[EPISODES_FILTERED_COLS]
    shows_df = shows_df[SHOWS_METADATA_COLS]
    basics_df = basics_df[EPISODES_BASICS_COLS]

    # Write to sample directory
    SAMPLE_DIR.mkdir(parents=True, exist_ok=True)
    ep_df.to_csv(SAMPLE_DIR / "episodes_filtered.csv", index=False)
    shows_df.to_csv(SAMPLE_DIR / "shows_metadata.csv", index=False)
    basics_df.to_csv(SAMPLE_DIR / "episodes_basics.csv", index=False)

    # Summary
    print(f"Generated {len(ep_df)} episodes across {len(SHOW_IDS)} shows")
    for tconst, title in SHOW_IDS.items():
        n = (ep_df["show_tconst"] == tconst).sum()
        seasons = ep_df[ep_df["show_tconst"] == tconst]["season_num"].nunique()
        print(f"  {title}: {n} episodes, {seasons} seasons")
    print(f"\nOutput directory: {SAMPLE_DIR}")
    print("Synthetic fixtures generated from scratch. No IMDb data sampled.")


if __name__ == "__main__":
    main()
