"""Execute DuckDB SQL pipeline: schema, KPIs, shark-jump, durability.

Usage:
    python pipeline/02_run_sql.py            # Process data/ CSVs
    python pipeline/02_run_sql.py --sample   # Process data/sample/ CSVs
"""

import argparse
import sys
from pathlib import Path

import duckdb
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.config import SHOW_IDS, OUTPUT_DIR, SAMPLE_DIR, DIM_SHOW_PATH

SQL_DIR = PROJECT_ROOT / "sql"


def parse_args():
    parser = argparse.ArgumentParser(description="Run DuckDB SQL pipeline")
    parser.add_argument(
        "--sample",
        action="store_true",
        help="Use data/sample/ CSVs as input",
    )
    return parser.parse_args()


def resolve_paths(is_sample: bool) -> dict:
    """Return placeholder -> resolved path mapping."""
    if is_sample:
        data_dir = SAMPLE_DIR
    else:
        data_dir = OUTPUT_DIR

    paths = {
        "__EPISODES_CSV__": str(data_dir / "episodes_filtered.csv"),
        "__SHOWS_CSV__": str(data_dir / "shows_metadata.csv"),
        "__CATEGORY_CSV__": str(DIM_SHOW_PATH),
    }
    return paths


def read_sql(filename: str, path_map: dict) -> str:
    """Read a SQL file and replace placeholder tokens with resolved paths."""
    sql_path = SQL_DIR / filename
    sql_text = sql_path.read_text()
    for token, resolved in path_map.items():
        sql_text = sql_text.replace(token, resolved)
    return sql_text


def main():
    args = parse_args()
    mode = "sample" if args.sample else "full"
    path_map = resolve_paths(args.sample)

    print(f"=== DuckDB SQL Pipeline (mode: {mode}) ===\n")
    print("Resolved CSV paths:")
    for token, path in path_map.items():
        print(f"  {token} -> {path}")
    print()

    # Verify input files exist
    for token, path in path_map.items():
        if not Path(path).exists():
            print(f"ERROR: {path} not found (token: {token})")
            sys.exit(1)

    con = duckdb.connect(":memory:")

    # ── 01: Schema ───────────────────────────────────────────────
    print("Running sql/01_schema.sql ...")
    schema_sql = read_sql("01_schema.sql", path_map)
    con.execute(schema_sql)

    dim_show_count = con.execute("SELECT COUNT(*) FROM dim_show").fetchone()[0]
    fact_ep_count = con.execute("SELECT COUNT(*) FROM fact_episode").fetchone()[0]
    print(f"  dim_show: {dim_show_count} rows")
    print(f"  fact_episode: {fact_ep_count} rows\n")

    # ── 02: Season KPIs ──────────────────────────────────────────
    print("Running sql/02_season_kpis.sql ...")
    kpi_sql = read_sql("02_season_kpis.sql", path_map)
    con.execute(kpi_sql)

    kpi_df = con.execute("SELECT * FROM agg_season_kpis").fetchdf()
    print(f"  agg_season_kpis: {len(kpi_df)} rows\n")

    # ── 03: Shark-jump detection ─────────────────────────────────
    print("Running sql/03_shark_jump.sql ...")
    shark_sql = read_sql("03_shark_jump.sql", path_map)
    shark_df = con.execute(shark_sql).fetchdf()
    print(f"  shark_jump_results: {len(shark_df)} rows\n")

    # ── 04: Durability index ─────────────────────────────────────
    print("Running sql/04_durability.sql ...")
    dur_sql = read_sql("04_durability.sql", path_map)
    dur_df = con.execute(dur_sql).fetchdf()
    print(f"  durability_index: {len(dur_df)} rows\n")

    # ── Export CSVs ──────────────────────────────────────────────
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    kpi_df.to_csv(OUTPUT_DIR / "agg_season_kpis.csv", index=False)
    shark_df.to_csv(OUTPUT_DIR / "shark_jump_results.csv", index=False)
    dur_df.to_csv(OUTPUT_DIR / "durability_index.csv", index=False)

    print(f"Exported CSVs to {OUTPUT_DIR}:")
    print(f"  agg_season_kpis.csv    ({len(kpi_df)} rows)")
    print(f"  shark_jump_results.csv ({len(shark_df)} rows)")
    print(f"  durability_index.csv   ({len(dur_df)} rows)")

    # ── Summary ──────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("Summary:")
    print(f"  Shows processed: {dim_show_count}")

    seasons_per_show = kpi_df.groupby("show_tconst")["season_num"].count()
    for tconst, title in SHOW_IDS.items():
        n_seasons = seasons_per_show.get(tconst, 0)
        print(f"  {title}: {n_seasons} seasons")

    print("\nShark-jump results:")
    for _, row in shark_df.iterrows():
        title = SHOW_IDS.get(row["show_tconst"], row["show_tconst"])
        sj = row["shark_jump_season"]
        if pd.isna(sj):
            print(f"  {title}: No shark-jump detected")
        else:
            print(f"  {title}: Season {int(sj)}")

    print("\nDurability index:")
    for _, row in dur_df.iterrows():
        title = SHOW_IDS.get(row["show_tconst"], row["show_tconst"])
        print(f"  {title}: {int(row['durability_index'])} seasons above avg")

    con.close()
    print(f"\nDone.")


if __name__ == "__main__":
    main()
