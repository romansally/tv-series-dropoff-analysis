"""Validation entry point for pipeline outputs.

Usage:
    python qa/validate.py            # Validate data/ outputs
    python qa/validate.py --sample   # Validate data/sample/ fixtures
    python qa/validate.py --all      # Run all available checks
"""

import argparse
import sys
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config import (
    SHOW_IDS,
    OUTPUT_DIR,
    SAMPLE_DIR,
    EPISODES_FILTERED_COLS,
    SHOWS_METADATA_COLS,
    EPISODES_BASICS_COLS,
)


def parse_args():
    parser = argparse.ArgumentParser(description="Validate pipeline outputs")
    parser.add_argument(
        "--sample", action="store_true", help="Validate data/sample/ fixtures"
    )
    parser.add_argument(
        "--all", action="store_true", help="Run all available checks"
    )
    return parser.parse_args()


# ─── Check helpers ────────────────────────────────────────────


class CheckRunner:
    """Tracks pass/fail/warn counts across all checks."""

    def __init__(self):
        self.failures = 0
        self.warnings = 0
        self.total = 0

    def check(self, name, passed, detail=""):
        self.total += 1
        status = "PASS" if passed else "FAIL"
        suffix = f" — {detail}" if detail else ""
        print(f"  [{status}] {name}{suffix}")
        if not passed:
            self.failures += 1

    def warn(self, name, ok, detail=""):
        self.total += 1
        status = "PASS" if ok else "WARN"
        suffix = f" — {detail}" if detail else ""
        print(f"  [{status}] {name}{suffix}")
        if not ok:
            self.warnings += 1


# ─── Validation logic ────────────────────────────────────────


def validate(data_dir: Path, is_sample: bool) -> int:
    """Run all Phase 1 checks. Returns number of failures (0 = success)."""
    runner = CheckRunner()

    mode_label = "sample fixtures" if is_sample else "pipeline outputs"
    print(f"Validating {mode_label} in {data_dir}\n")

    # ── Load files ────────────────────────────────────────────
    ep_path = data_dir / "episodes_filtered.csv"
    shows_path = data_dir / "shows_metadata.csv"
    basics_path = data_dir / "episodes_basics.csv"

    for p in [ep_path, shows_path, basics_path]:
        if not p.exists():
            print(f"  [FAIL] File not found: {p}")
            return 1

    ep_df = pd.read_csv(ep_path)
    shows_df = pd.read_csv(shows_path)
    basics_df = pd.read_csv(basics_path)

    # ── Check 1: Schema — expected columns present ────────────
    print("Check 1: Schema validation")
    for label, df, expected_cols in [
        ("episodes_filtered", ep_df, EPISODES_FILTERED_COLS),
        ("shows_metadata", shows_df, SHOWS_METADATA_COLS),
        ("episodes_basics", basics_df, EPISODES_BASICS_COLS),
    ]:
        missing = set(expected_cols) - set(df.columns)
        runner.check(
            f"{label} columns",
            len(missing) == 0,
            f"all {len(expected_cols)} present" if not missing else f"missing {missing}",
        )

    # Dtype castability
    try:
        ep_df["season_num"].astype(int)
        ep_df["episode_num"].astype(int)
        ep_df["avg_rating"].astype(float)
        ep_df["num_votes"].astype(int)
        runner.check("episodes_filtered dtypes", True, "all castable")
    except (ValueError, TypeError) as e:
        runner.check("episodes_filtered dtypes", False, str(e))

    # ── Check 2: No duplicate episode_tconst in episodes_filtered
    print("\nCheck 2: No duplicate episode_tconst in episodes_filtered")
    dup_count = ep_df["episode_tconst"].duplicated().sum()
    runner.check(
        "unique episode_tconst",
        dup_count == 0,
        f"{len(ep_df)} unique" if dup_count == 0 else f"{dup_count} duplicates",
    )

    # ── Check 3: No NULL/0 season_num, no NULL episode_num ────
    print("\nCheck 3: No NULL/0 season_num, no NULL episode_num")
    null_season = ep_df["season_num"].isna().sum()
    zero_season = (ep_df["season_num"] == 0).sum()
    null_episode = ep_df["episode_num"].isna().sum()
    runner.check(
        "season_num valid",
        null_season == 0 and zero_season == 0,
        "no NULL or 0"
        if (null_season == 0 and zero_season == 0)
        else f"{null_season} NULL, {zero_season} zero",
    )
    runner.check(
        "episode_num valid",
        null_episode == 0,
        "no NULL" if null_episode == 0 else f"{null_episode} NULL",
    )

    # ── Check 4: avg_rating in [1.0, 10.0], num_votes >= 0 ───
    print("\nCheck 4: Rating and vote ranges")
    rating_min = ep_df["avg_rating"].min()
    rating_max = ep_df["avg_rating"].max()
    rating_null = ep_df["avg_rating"].isna().sum()
    runner.check(
        "avg_rating range [1.0, 10.0]",
        rating_null == 0 and rating_min >= 1.0 and rating_max <= 10.0,
        f"range [{rating_min}, {rating_max}], {rating_null} NULL",
    )

    votes_null = ep_df["num_votes"].isna().sum()
    votes_min = ep_df["num_votes"].min()
    runner.check(
        "num_votes non-NULL and >= 0",
        votes_null == 0 and votes_min >= 0,
        f"min={votes_min}, {votes_null} NULL",
    )

    # ── Check 5: All show_tconst values in SHOW_IDS ──────────
    print("\nCheck 5: show_tconst membership")
    ep_shows = set(ep_df["show_tconst"].unique())
    valid_shows = set(SHOW_IDS.keys())
    unexpected = ep_shows - valid_shows
    runner.check(
        "all show_tconst in SHOW_IDS",
        len(unexpected) == 0,
        f"{len(ep_shows)} shows, all valid"
        if not unexpected
        else f"unexpected: {unexpected}",
    )

    # ── Check 6: episodes_basics title_type == "tvEpisode" ────
    print("\nCheck 6: title_type in episodes_basics")
    non_tv = basics_df[basics_df["title_type"] != "tvEpisode"]
    runner.check(
        "all title_type == tvEpisode",
        len(non_tv) == 0,
        f"{len(basics_df)} rows, all tvEpisode"
        if len(non_tv) == 0
        else f"{len(non_tv)} non-tvEpisode rows",
    )

    # ── Check 7: No duplicate episode_tconst in episodes_basics
    print("\nCheck 7: No duplicate episode_tconst in episodes_basics")
    basics_dup = basics_df["episode_tconst"].duplicated().sum()
    runner.check(
        "unique episode_tconst in basics",
        basics_dup == 0,
        f"{len(basics_df)} unique" if basics_dup == 0 else f"{basics_dup} duplicates",
    )

    # ── Check 8: Key alignment (filtered <= basics) ───────────
    print("\nCheck 8: Key alignment")
    ep_set = set(ep_df["episode_tconst"])
    basics_set = set(basics_df["episode_tconst"])
    is_subset = ep_set.issubset(basics_set)
    runner.check(
        "episodes_filtered.episode_tconst subset of episodes_basics",
        is_subset,
        "subset confirmed" if is_subset else f"{len(ep_set - basics_set)} missing from basics",
    )

    # ── Check 9: Row-count parity (WARN only) ────────────────
    print("\nCheck 9: Row-count parity")
    counts_match = len(ep_df) == len(basics_df)
    runner.warn(
        "row counts equal",
        counts_match,
        f"both {len(ep_df)}"
        if counts_match
        else (
            f"episodes_filtered={len(ep_df)}, episodes_basics={len(basics_df)} "
            f"(subset holds: {is_subset})"
        ),
    )

    # ── Check 10: shows_metadata completeness ─────────────────
    print("\nCheck 10: shows_metadata completeness")
    runner.check(
        "exactly 4 rows",
        len(shows_df) == 4,
        f"{len(shows_df)} rows",
    )
    meta_shows = set(shows_df["show_tconst"])
    runner.check(
        "show_tconst matches SHOW_IDS",
        meta_shows == valid_shows,
        "all 4 shows present"
        if meta_shows == valid_shows
        else f"mismatch: expected {valid_shows}, got {meta_shows}",
    )

    # ── Check 11: Fabrication check (--sample only) ───────────
    if is_sample:
        print("\nCheck 11: Fabrication check (sample only)")
        non_synthetic = ep_df[~ep_df["episode_tconst"].str.startswith("tt999")]
        runner.check(
            "all episode_tconst start with tt999",
            len(non_synthetic) == 0,
            f"{len(ep_df)} synthetic IDs"
            if len(non_synthetic) == 0
            else f"{len(non_synthetic)} non-synthetic IDs found",
        )

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'='*50}")
    print(
        f"Results: {runner.total} checks — "
        f"{runner.total - runner.failures - runner.warnings} passed, "
        f"{runner.failures} failed, {runner.warnings} warnings"
    )

    if runner.failures == 0:
        print("Phase 1 validation: ALL CHECKS PASSED")
    else:
        print(f"Phase 1 validation: {runner.failures} FAILURE(S)")

    return runner.failures


def main():
    args = parse_args()
    data_dir = SAMPLE_DIR if args.sample else OUTPUT_DIR
    failures = validate(data_dir, is_sample=args.sample)
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
