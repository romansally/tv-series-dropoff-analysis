"""Subset IMDb TSV data for the 4 selected TV shows.

Usage:
    python pipeline/01_subset_imdb.py           # Process real IMDb TSVs
    python pipeline/01_subset_imdb.py --sample   # Process sample fixtures
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config import (
    SHOW_IDS,
    RAW_DIR,
    OUTPUT_DIR,
    SAMPLE_DIR,
    EPISODES_TSV,
    RATINGS_TSV,
    BASICS_TSV,
    CHUNK_SIZE,
    EPISODES_FILTERED_COLS,
    SHOWS_METADATA_COLS,
    EPISODES_BASICS_COLS,
)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Filter IMDb TSV data for selected TV shows"
    )
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Read from data/sample/ instead of IMDb TSVs",
    )
    return parser.parse_args()


# ─── Default mode: real IMDb TSVs ────────────────────────────


def run_default_mode():
    """Process real IMDb TSV files from RAW_DIR, write cleaned CSVs to OUTPUT_DIR."""
    show_tconst_set = set(SHOW_IDS.keys())

    # ── Step a: Filter title.episode ──────────────────────────
    print("Step a: Reading title.episode...")
    episode_path = RAW_DIR / EPISODES_TSV
    if not episode_path.exists():
        print(f"ERROR: {episode_path} not found.")
        print("Download from https://datasets.imdbws.com/ and place in", RAW_DIR)
        sys.exit(1)

    ep_chunks = []
    total_rows = 0
    for chunk in pd.read_csv(
        episode_path, sep="\t", na_values=["\\N"], chunksize=CHUNK_SIZE, dtype=str
    ):
        total_rows += len(chunk)
        filtered = chunk[chunk["parentTconst"].isin(show_tconst_set)]
        if len(filtered) > 0:
            ep_chunks.append(filtered)

    episodes = pd.concat(ep_chunks, ignore_index=True) if ep_chunks else pd.DataFrame()
    print(f"  Read {total_rows:,} total rows, found {len(episodes)} episodes for selected shows")

    # Rename columns to snake_case
    episodes = episodes.rename(
        columns={
            "tconst": "episode_tconst",
            "parentTconst": "show_tconst",
            "seasonNumber": "season_num",
            "episodeNumber": "episode_num",
        }
    )

    # Drop specials: NULL season_num
    before = len(episodes)
    episodes = episodes.dropna(subset=["season_num"])
    dropped_null_season = before - len(episodes)

    # Cast season_num to int, then drop season_num == 0
    episodes["season_num"] = episodes["season_num"].astype(int)
    before = len(episodes)
    episodes = episodes[episodes["season_num"] != 0]
    dropped_zero_season = before - len(episodes)
    print(
        f"  Dropped {dropped_null_season} NULL seasonNumber, "
        f"{dropped_zero_season} seasonNumber=0"
    )

    # Drop NULL episode_num
    before = len(episodes)
    episodes = episodes.dropna(subset=["episode_num"])
    episodes["episode_num"] = episodes["episode_num"].astype(int)
    print(f"  Dropped {before - len(episodes)} NULL episodeNumber")

    episode_tconst_set = set(episodes["episode_tconst"])
    print(f"  {len(episode_tconst_set)} unique episode tconsts after filtering")

    # ── Step b: Filter title.ratings ──────────────────────────
    print("\nStep b: Reading title.ratings...")
    ratings_path = RAW_DIR / RATINGS_TSV
    if not ratings_path.exists():
        print(f"ERROR: {ratings_path} not found.")
        print("Download from https://datasets.imdbws.com/ and place in", RAW_DIR)
        sys.exit(1)

    ratings = pd.read_csv(
        ratings_path, sep="\t", na_values=["\\N"], dtype={"tconst": str}
    )
    ratings = ratings[ratings["tconst"].isin(episode_tconst_set)]
    ratings = ratings.rename(
        columns={
            "tconst": "episode_tconst",
            "averageRating": "avg_rating",
            "numVotes": "num_votes",
        }
    )
    print(f"  Found {len(ratings)} ratings for selected episodes")

    # ── Step c: INNER JOIN episodes <-> ratings ───────────────
    print("\nStep c: Joining episodes and ratings...")
    joined = episodes.merge(ratings, on="episode_tconst", how="inner")
    print(
        f"  {len(joined)} episodes after inner join "
        f"(dropped {len(episodes) - len(joined)} without ratings)"
    )

    # Drop NULL avg_rating
    before = len(joined)
    joined = joined.dropna(subset=["avg_rating"])
    print(f"  Dropped {before - len(joined)} rows with NULL avg_rating")

    # Drop NULL num_votes
    before = len(joined)
    joined = joined.dropna(subset=["num_votes"])
    print(f"  Dropped {before - len(joined)} rows with NULL num_votes")

    # Log zero-vote episodes (keep them)
    zero_votes = (joined["num_votes"] == 0).sum()
    if zero_votes > 0:
        print(f"  NOTE: {zero_votes} episodes with num_votes=0 (kept)")

    # Cast types
    joined["avg_rating"] = joined["avg_rating"].astype(float)
    joined["num_votes"] = joined["num_votes"].astype(int)

    # ── Step d: Filter title.basics (chunked) ─────────────────
    print("\nStep d: Reading title.basics...")
    basics_path = RAW_DIR / BASICS_TSV
    if not basics_path.exists():
        print(f"ERROR: {basics_path} not found.")
        print("Download from https://datasets.imdbws.com/ and place in", RAW_DIR)
        sys.exit(1)

    # Combined filter: episode tconsts (for titleType) + show tconsts (for metadata)
    combined_tconst_set = episode_tconst_set | show_tconst_set

    basics_chunks = []
    total_rows = 0
    for chunk in pd.read_csv(
        basics_path, sep="\t", na_values=["\\N"], chunksize=CHUNK_SIZE, dtype=str
    ):
        total_rows += len(chunk)
        filtered = chunk[chunk["tconst"].isin(combined_tconst_set)]
        if len(filtered) > 0:
            basics_chunks.append(filtered)

    basics = pd.concat(basics_chunks, ignore_index=True) if basics_chunks else pd.DataFrame()
    print(f"  Read {total_rows:,} total rows, found {len(basics)} matching rows")

    # Split into episode-level and show-level basics
    ep_basics = basics[basics["tconst"].isin(episode_tconst_set)].copy()
    show_basics = basics[basics["tconst"].isin(show_tconst_set)].copy()
    print(
        f"  Episode-level basics: {len(ep_basics)}, "
        f"Show-level basics: {len(show_basics)}"
    )

    # ── Step e: Verify titleType == "tvEpisode" ───────────────
    print("\nStep e: Verifying titleType...")

    # Build lookup: episode_tconst -> titleType
    ep_basics_dedup = ep_basics.drop_duplicates(subset=["tconst"])
    ep_basics_map = dict(zip(ep_basics_dedup["tconst"], ep_basics_dedup["titleType"]))

    # Check each episode in the joined set (split missing-basics vs wrong-titleType logs)
    title_type = joined["episode_tconst"].map(ep_basics_map)

    missing_basics = int(title_type.isna().sum())
    wrong_title_type = int((~title_type.isna() & (title_type != "tvEpisode")).sum())

    # Keep only verified tvEpisode (this also drops missing basics rows)
    joined = joined[title_type == "tvEpisode"].copy()

    if missing_basics:
        print(f"  Dropped {missing_basics} episodes with missing basics row / NULL titleType")
    if wrong_title_type:
        print(f"  Dropped {wrong_title_type} episodes with titleType != tvEpisode")

    # ── Step f: Prepare and write outputs ─────────────────────
    print("\nStep f: Preparing outputs...")

    # episodes_filtered.csv — only episodes that passed all checks
    episodes_out = joined[EPISODES_FILTERED_COLS].copy()

    # episodes_basics.csv — all episodes with valid titleType (superset of filtered)
    valid_ep_basics = ep_basics_dedup[ep_basics_dedup["titleType"] == "tvEpisode"][
        ["tconst", "titleType"]
    ].copy()
    valid_ep_basics = valid_ep_basics.rename(
        columns={"tconst": "episode_tconst", "titleType": "title_type"}
    )
    basics_out = valid_ep_basics[EPISODES_BASICS_COLS]

    # shows_metadata.csv — one row per show
    shows_out = show_basics[
        ["tconst", "primaryTitle", "startYear", "endYear", "genres"]
    ].copy()
    shows_out = shows_out.rename(
        columns={
            "tconst": "show_tconst",
            "primaryTitle": "primary_title",
            "startYear": "start_year",
            "endYear": "end_year",
        }
    )
    shows_out = shows_out.drop_duplicates(subset=["show_tconst"])

    # Key alignment check
    ep_set = set(episodes_out["episode_tconst"])
    basics_set = set(basics_out["episode_tconst"])
    is_subset = ep_set.issubset(basics_set)
    print(
        f"  episodes_filtered: {len(episodes_out)} rows, "
        f"episodes_basics: {len(basics_out)} rows"
    )
    print(f"  Subset check (filtered <= basics): {is_subset}")
    if len(ep_set) != len(basics_set):
        print(
            f"  NOTE: Row counts differ — {len(basics_set) - len(ep_set)} episodes "
            "in basics lack ratings"
        )

    # Check all 4 shows present in metadata
    missing_shows = set(SHOW_IDS.keys()) - set(shows_out["show_tconst"])
    if missing_shows:
        print(f"  WARNING: Missing show metadata for: {missing_shows}")

    # Write outputs
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    episodes_out.to_csv(OUTPUT_DIR / "episodes_filtered.csv", index=False)
    shows_out.to_csv(OUTPUT_DIR / "shows_metadata.csv", index=False)
    basics_out.to_csv(OUTPUT_DIR / "episodes_basics.csv", index=False)

    # Per-show summary
    print("\nFinal episode counts per show:")
    for tconst, title in SHOW_IDS.items():
        count = (episodes_out["show_tconst"] == tconst).sum()
        print(f"  {title}: {count}")

    print(f"\nOutputs written to {OUTPUT_DIR}")


# ─── Sample mode: read fixtures from data/sample/ ────────────


def run_sample_mode():
    """Read sample fixtures, apply validation/cleaning, write to OUTPUT_DIR."""
    print("Running in --sample mode (reading from data/sample/)\n")

    ep_path = SAMPLE_DIR / "episodes_filtered.csv"
    shows_path = SAMPLE_DIR / "shows_metadata.csv"
    basics_path = SAMPLE_DIR / "episodes_basics.csv"

    for p in [ep_path, shows_path, basics_path]:
        if not p.exists():
            print(f"ERROR: {p} not found. Run qa/fixtures/generate_synthetic.py first.")
            sys.exit(1)

    ep_df = pd.read_csv(ep_path)
    shows_df = pd.read_csv(shows_path)
    basics_df = pd.read_csv(basics_path)
    print(
        f"Read {len(ep_df)} episodes, {len(shows_df)} shows, "
        f"{len(basics_df)} basics rows"
    )

    # Validate expected columns
    for name, df, expected in [
        ("episodes_filtered", ep_df, EPISODES_FILTERED_COLS),
        ("shows_metadata", shows_df, SHOWS_METADATA_COLS),
        ("episodes_basics", basics_df, EPISODES_BASICS_COLS),
    ]:
        missing = set(expected) - set(df.columns)
        if missing:
            print(f"ERROR: {name} missing columns: {missing}")
            sys.exit(1)

    # ── Apply cleaning/validation (same rules as default mode) ──

    # Drop NULL/0 season_num
    before = len(ep_df)
    ep_df = ep_df.dropna(subset=["season_num"])
    ep_df["season_num"] = ep_df["season_num"].astype(int)
    ep_df = ep_df[ep_df["season_num"] != 0]
    dropped = before - len(ep_df)
    if dropped:
        print(f"  Dropped {dropped} rows with NULL/0 season_num")

    # Drop NULL episode_num
    before = len(ep_df)
    ep_df = ep_df.dropna(subset=["episode_num"])
    ep_df["episode_num"] = ep_df["episode_num"].astype(int)
    dropped = before - len(ep_df)
    if dropped:
        print(f"  Dropped {dropped} rows with NULL episode_num")

    # Drop NULL avg_rating
    before = len(ep_df)
    ep_df = ep_df.dropna(subset=["avg_rating"])
    dropped = before - len(ep_df)
    if dropped:
        print(f"  Dropped {dropped} rows with NULL avg_rating")

    # Drop NULL num_votes
    before = len(ep_df)
    ep_df = ep_df.dropna(subset=["num_votes"])
    dropped = before - len(ep_df)
    if dropped:
        print(f"  Dropped {dropped} rows with NULL num_votes")

    # Log zero-vote episodes (keep them)
    zero_votes = (ep_df["num_votes"] == 0).sum()
    if zero_votes > 0:
        print(f"  NOTE: {zero_votes} episodes with num_votes=0 (kept)")

    # Cast types
    ep_df["avg_rating"] = ep_df["avg_rating"].astype(float)
    ep_df["num_votes"] = ep_df["num_votes"].astype(int)

    # titleType check — drop episodes not in basics or with wrong title_type
    valid_basics = basics_df[basics_df["title_type"] == "tvEpisode"]
    valid_tconsts = set(valid_basics["episode_tconst"])
    before = len(ep_df)
    ep_df = ep_df[ep_df["episode_tconst"].isin(valid_tconsts)]
    dropped = before - len(ep_df)
    if dropped:
        print(f"  Dropped {dropped} episodes with invalid titleType")

    # Enforce column order
    ep_df = ep_df[EPISODES_FILTERED_COLS]
    shows_df = shows_df[SHOWS_METADATA_COLS]
    basics_df = basics_df[EPISODES_BASICS_COLS]

    # Write to output dir
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    ep_df.to_csv(OUTPUT_DIR / "episodes_filtered.csv", index=False)
    shows_df.to_csv(OUTPUT_DIR / "shows_metadata.csv", index=False)
    basics_df.to_csv(OUTPUT_DIR / "episodes_basics.csv", index=False)

    # Per-show summary
    print("\nFinal episode counts per show:")
    for tconst, title in SHOW_IDS.items():
        count = (ep_df["show_tconst"] == tconst).sum()
        print(f"  {title}: {count}")

    print(f"\nOutputs written to {OUTPUT_DIR}")


def main():
    args = parse_args()
    if args.sample:
        run_sample_mode()
    else:
        run_default_mode()


if __name__ == "__main__":
    main()
