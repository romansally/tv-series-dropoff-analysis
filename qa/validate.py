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


# ─── Phase 1 validation ────────────────────────────────────────


def validate_phase1(data_dir: Path, is_sample: bool, runner: CheckRunner) -> bool:
    """Run Phase 1 checks. Returns False if critical files missing."""
    print("── Phase 1 Checks ──\n")

    # ── Load files ────────────────────────────────────────────
    ep_path = data_dir / "episodes_filtered.csv"
    shows_path = data_dir / "shows_metadata.csv"
    basics_path = data_dir / "episodes_basics.csv"

    for p in [ep_path, shows_path, basics_path]:
        if not p.exists():
            print(f"  [FAIL] File not found: {p}")
            runner.failures += 1
            return False

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

    return True


# ─── Phase 2 validation ────────────────────────────────────────


PHASE2_KPI_COLS = [
    "show_tconst", "season_num", "episode_count", "season_total_votes",
    "weighted_rating", "mean_rating", "rating_stddev", "pct_high_rated",
    "series_avg", "rolling_3_season_avg", "season_rank_best", "catalog_value_index",
]


def validate_phase2(data_dir: Path, is_sample: bool, runner: CheckRunner):
    """Run Phase 2 checks on SQL outputs in data/."""
    print("\n── Phase 2 Checks ──\n")

    valid_shows = set(SHOW_IDS.keys())

    # ── Load Phase 2 files ───────────────────────────────────
    kpi_path = OUTPUT_DIR / "agg_season_kpis.csv"
    shark_path = OUTPUT_DIR / "shark_jump_results.csv"
    dur_path = OUTPUT_DIR / "durability_index.csv"

    for p in [kpi_path, shark_path, dur_path]:
        if not p.exists():
            print(f"  [FAIL] File not found: {p}")
            runner.failures += 1
            return

    kpi_df = pd.read_csv(kpi_path)
    shark_df = pd.read_csv(shark_path)
    dur_df = pd.read_csv(dur_path)

    # ── Check 12: agg_season_kpis schema ─────────────────────
    print("Check 12: agg_season_kpis schema")
    missing_cols = set(PHASE2_KPI_COLS) - set(kpi_df.columns)
    runner.check(
        "expected columns present",
        len(missing_cols) == 0,
        f"all {len(PHASE2_KPI_COLS)} present"
        if not missing_cols
        else f"missing {missing_cols}",
    )

    # Dtype checks
    try:
        kpi_df["season_num"].astype(int)
        kpi_df["episode_count"].astype(int)
        kpi_df["weighted_rating"].astype(float)
        kpi_df["catalog_value_index"].astype(float)
        runner.check("kpi dtypes", True, "all castable")
    except (ValueError, TypeError) as e:
        runner.check("kpi dtypes", False, str(e))

    # ── Check 13: Unique on (show_tconst, season_num) ────────
    print("\nCheck 13: agg_season_kpis uniqueness")
    kpi_dup = kpi_df.duplicated(subset=["show_tconst", "season_num"]).sum()
    runner.check(
        "unique (show_tconst, season_num)",
        kpi_dup == 0,
        f"{len(kpi_df)} rows, all unique" if kpi_dup == 0 else f"{kpi_dup} duplicates",
    )

    # ── Check 14: show_tconst in SHOW_IDS ────────────────────
    print("\nCheck 14: agg_season_kpis show_tconst membership")
    kpi_shows = set(kpi_df["show_tconst"].unique())
    unexpected = kpi_shows - valid_shows
    runner.check(
        "all show_tconst in SHOW_IDS",
        len(unexpected) == 0,
        f"{len(kpi_shows)} shows, all valid"
        if not unexpected
        else f"unexpected: {unexpected}",
    )

    # ── Check 15: season_num > 0, no NULLs; episode_count > 0
    print("\nCheck 15: agg_season_kpis season/episode ranges")
    sn_null = kpi_df["season_num"].isna().sum()
    sn_min = kpi_df["season_num"].min() if sn_null == 0 else -1
    runner.check(
        "season_num > 0, no NULLs",
        sn_null == 0 and sn_min > 0,
        f"min={sn_min}, {sn_null} NULL",
    )
    ec_null = kpi_df["episode_count"].isna().sum()
    ec_min = kpi_df["episode_count"].min() if ec_null == 0 else -1
    runner.check(
        "episode_count > 0",
        ec_null == 0 and ec_min > 0,
        f"min={ec_min}",
    )

    # ── Check 16: weighted_rating in [1, 10], CVI > 0 ────────
    print("\nCheck 16: agg_season_kpis value ranges")
    wr_min = kpi_df["weighted_rating"].min()
    wr_max = kpi_df["weighted_rating"].max()
    runner.check(
        "weighted_rating in [1.0, 10.0]",
        wr_min >= 1.0 and wr_max <= 10.0,
        f"range [{wr_min:.4f}, {wr_max:.4f}]",
    )
    cvi_min = kpi_df["catalog_value_index"].min()
    runner.check(
        "catalog_value_index > 0",
        cvi_min > 0,
        f"min={cvi_min:.4f}",
    )

    # ── Check 17: shark_jump_results schema and grain ─────────
    print("\nCheck 17: shark_jump_results validation")
    shark_expected_cols = {"show_tconst", "shark_jump_season"}
    shark_missing = shark_expected_cols - set(shark_df.columns)
    runner.check(
        "expected columns",
        len(shark_missing) == 0,
        f"columns: {list(shark_df.columns)}" if not shark_missing else f"missing {shark_missing}",
    )

    shark_shows = set(shark_df["show_tconst"].unique())
    runner.check(
        "exactly 1 row per show",
        len(shark_df) == len(shark_shows) and shark_shows == valid_shows,
        f"{len(shark_df)} rows, {len(shark_shows)} shows"
        if shark_shows == valid_shows
        else f"show mismatch: expected {valid_shows}, got {shark_shows}",
    )

    # shark_jump_season is NULL or int >= 3
    for _, row in shark_df.iterrows():
        sj = row["shark_jump_season"]
        title = SHOW_IDS.get(row["show_tconst"], row["show_tconst"])
        if pd.isna(sj):
            runner.check(f"shark_jump_season {title}", True, "NULL (no shark-jump)")
        else:
            runner.check(
                f"shark_jump_season {title}",
                int(sj) >= 3,
                f"season {int(sj)}" if int(sj) >= 3 else f"season {int(sj)} < 3 (invalid)",
            )

    # ── Check 18: durability_index schema and grain ───────────
    print("\nCheck 18: durability_index validation")
    dur_expected_cols = {"show_tconst", "durability_index"}
    dur_missing = dur_expected_cols - set(dur_df.columns)
    runner.check(
        "expected columns",
        len(dur_missing) == 0,
        f"columns: {list(dur_df.columns)}" if not dur_missing else f"missing {dur_missing}",
    )

    dur_shows = set(dur_df["show_tconst"].unique())
    runner.check(
        "exactly 1 row per show",
        len(dur_df) == len(dur_shows) and dur_shows == valid_shows,
        f"{len(dur_df)} rows, {len(dur_shows)} shows"
        if dur_shows == valid_shows
        else f"show mismatch: expected {valid_shows}, got {dur_shows}",
    )

    # durability_index is int >= 0
    for _, row in dur_df.iterrows():
        di = row["durability_index"]
        title = SHOW_IDS.get(row["show_tconst"], row["show_tconst"])
        runner.check(
            f"durability_index {title}",
            not pd.isna(di) and int(di) >= 0,
            f"{int(di)} seasons" if not pd.isna(di) else "NULL",
        )

    # ── Check 19: Off-by-one verification (--sample only) ────
    if is_sample:
        print("\nCheck 19: Off-by-one verification (sample only)")
        # At least one show must trigger shark-jump
        triggered = shark_df[shark_df["shark_jump_season"].notna()]
        runner.check(
            "at least one show triggers shark-jump",
            len(triggered) > 0,
            f"{len(triggered)} shows triggered",
        )

        # For each triggered show, verify the shark_jump_season value
        for _, row in triggered.iterrows():
            tconst = row["show_tconst"]
            sj_season = int(row["shark_jump_season"])
            title = SHOW_IDS.get(tconst, tconst)

            show_kpis = kpi_df[kpi_df["show_tconst"] == tconst].sort_values("season_num")

            # Find the first season where rolling_3_season_avg < series_avg
            # AND the next season also has rolling_3_season_avg < series_avg
            below_seasons = []
            for _, krow in show_kpis.iterrows():
                if krow["rolling_3_season_avg"] < krow["series_avg"]:
                    below_seasons.append(int(krow["season_num"]))

            # Find first consecutive pair
            expected_sj = None
            for i in range(len(below_seasons) - 1):
                # Check if these are consecutive in the KPI table
                s1 = below_seasons[i]
                s2 = below_seasons[i + 1]
                # They must be consecutive season_nums in the data
                all_seasons = sorted(show_kpis["season_num"].tolist())
                idx1 = all_seasons.index(s1)
                if idx1 + 1 < len(all_seasons) and all_seasons[idx1 + 1] == s2:
                    expected_sj = s1
                    break

            runner.check(
                f"off-by-one {title}",
                expected_sj is not None and sj_season == expected_sj,
                f"shark_jump_season={sj_season}, expected={expected_sj}",
            )

    # ── Check 20: Grain check ────────────────────────────────
    print("\nCheck 20: Grain check")
    runner.check(
        "shark_jump_results row count",
        len(shark_df) == len(valid_shows),
        f"{len(shark_df)} rows (expected {len(valid_shows)})",
    )
    runner.check(
        "durability_index row count",
        len(dur_df) == len(valid_shows),
        f"{len(dur_df)} rows (expected {len(valid_shows)})",
    )

    # ── Check 21: Cross-file consistency ─────────────────────
    print("\nCheck 21: Cross-file consistency")
    kpi_show_set = set(kpi_df["show_tconst"].unique())
    shark_show_set = set(shark_df["show_tconst"].unique())
    dur_show_set = set(dur_df["show_tconst"].unique())
    all_equal = kpi_show_set == shark_show_set == dur_show_set == valid_shows
    runner.check(
        "show_tconst sets identical across all outputs",
        all_equal,
        "all 3 files + SHOW_IDS match"
        if all_equal
        else f"kpi={kpi_show_set}, shark={shark_show_set}, dur={dur_show_set}",
    )


# ─── Main validation orchestrator ───────────────────────────────


def validate(data_dir: Path, is_sample: bool, run_all: bool) -> int:
    """Run validation checks. Returns number of failures (0 = success)."""
    runner = CheckRunner()

    mode_label = "sample fixtures" if is_sample else "pipeline outputs"
    print(f"Validating {mode_label} in {data_dir}\n")

    # Always run Phase 1
    phase1_ok = validate_phase1(data_dir, is_sample, runner)

    # Run Phase 2 if --all or Phase 2 outputs exist
    if run_all:
        if phase1_ok:
            validate_phase2(data_dir, is_sample, runner)
        else:
            print("\n── Phase 2 Checks ── SKIPPED (Phase 1 files missing)")

    # ── Summary ───────────────────────────────────────────────
    print(f"\n{'='*50}")
    phases_run = "Phase 1" + (" + Phase 2" if run_all else "")
    print(
        f"Results ({phases_run}): {runner.total} checks — "
        f"{runner.total - runner.failures - runner.warnings} passed, "
        f"{runner.failures} failed, {runner.warnings} warnings"
    )

    if runner.failures == 0:
        print("Validation: ALL CHECKS PASSED")
    else:
        print(f"Validation: {runner.failures} FAILURE(S)")

    return runner.failures


def main():
    args = parse_args()
    data_dir = SAMPLE_DIR if args.sample else OUTPUT_DIR
    failures = validate(data_dir, is_sample=args.sample, run_all=args.all)
    sys.exit(1 if failures > 0 else 0)


if __name__ == "__main__":
    main()
