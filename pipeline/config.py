"""Configuration for the TV Series Drop-off Analysis pipeline."""

import os
from pathlib import Path

# Project root directory
PROJECT_ROOT = Path(__file__).resolve().parent.parent

# ─── Show IDs (tconst -> title) ──────────────────────────────
SHOW_IDS = {
    "tt0096697": "The Simpsons",
    "tt0206512": "SpongeBob SquarePants",
    "tt0182576": "Family Guy",
    "tt1520211": "The Walking Dead",
}

# ─── Directories ─────────────────────────────────────────────
RAW_DIR = Path(os.environ.get("IMDB_RAW_DIR", str(PROJECT_ROOT / "data" / "raw")))
OUTPUT_DIR = PROJECT_ROOT / "data"
SAMPLE_DIR = PROJECT_ROOT / "data" / "sample"
DIM_SHOW_PATH = PROJECT_ROOT / "docs" / "dim_show_category.csv"

# ─── IMDb TSV Filenames ──────────────────────────────────────
EPISODES_TSV = "title.episode.tsv.gz"
RATINGS_TSV = "title.ratings.tsv.gz"
BASICS_TSV = "title.basics.tsv.gz"

# ─── Chunked Read Size ───────────────────────────────────────
CHUNK_SIZE = 500_000

# ─── Output CSV Column Schemas ───────────────────────────────
EPISODES_FILTERED_COLS = [
    "episode_tconst",
    "show_tconst",
    "season_num",
    "episode_num",
    "avg_rating",
    "num_votes",
]

SHOWS_METADATA_COLS = [
    "show_tconst",
    "primary_title",
    "start_year",
    "end_year",
    "genres",
]

EPISODES_BASICS_COLS = [
    "episode_tconst",
    "title_type",
]

DIM_SHOW_CATEGORY_COLS = [
    "show_tconst",
    "title",
    "category",
]
