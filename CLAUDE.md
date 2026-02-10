# CLAUDE.md — TV Series Drop-off Analysis ("Jump the Shark")

## Project Overview
Episode-level analytics model on IMDb data that detects where a TV franchise's
quality trend structurally breaks ("jumps the shark"), and recommends which seasons
maximize catalog value for licensing and promotion. Analyzes 4 shows across 2 formats.
Deliverable is a Tableau Public dashboard framed as streaming platform decision support.

This is a portfolio/resume project. MVP timeframe: 10–15 hours.
Repo name: tv-series-dropoff-analysis
Full project plan: docs/PROJECT_CONTEXT.md — read it before starting any phase.
Where CLAUDE.md and PROJECT_CONTEXT.md conflict, CLAUDE.md takes precedence.

## Session Start Checklist
Every new Claude Code session should begin with:
1. Summarize this CLAUDE.md in 8 bullets.
2. List the current phase and the next 1–3 concrete tasks.
3. Confirm what files you will create/edit and what commands you will run.
4. Stop and ask if any step requires assumptions not covered in CLAUDE.md or
   PROJECT_CONTEXT.md.

## Selected Shows
- The Simpsons: tt0096697
- SpongeBob SquarePants: tt0206512
- Family Guy: tt0182576
- The Walking Dead: tt1520211
IMPORTANT: Verify these tconst IDs against imdb.com before running the pipeline.

## Tech Stack
- Python 3.12 (pandas, numpy) — data cleaning, subsetting, pipeline scripts (.py only, not notebooks)
- DuckDB — local analytical SQL (CTEs, window functions, season-level KPIs)
- Tableau Public — dashboard deliverable (reads exported CSVs from pipeline)
- Excel/Google Sheets — QA reconciliation workbook

## Key Formulas (non-negotiable definitions)
- **Weighted rating:** SUM(avg_rating × num_votes) / SUM(num_votes) per season
- **Shark-jump detection:** See "Shark-Jump Algorithm" section below for exact logic.
  Keep the rule simple. Do not invent a more complex definition.
- **Catalog Value Index:** weighted_rating × LOG10(1 + season_total_votes).
  Engagement-weighted quality proxy. LOG dampens outlier vote counts.
- **Durability Index:** Count of seasons where rolling_3_season_avg >= series_avg.
  Measures sustained above-average quality using smoothed trend, not raw season noise.
- **Series avg override (approved):** series_avg = AVG(weighted_rating) OVER (PARTITION BY show_tconst), i.e., unweighted average of season-level weighted ratings. Applies to shark-jump detection AND Durability Index. Overrides step 2 of the Shark-Jump Algorithm (which specified episode-level weighted avg). Approved during Phase 2 planning.
- **Logging policy:** Pipeline + QA scripts must print only deterministic operational info (row counts, schema checks, computed outputs). No interpretive commentary, causal explanations, or "this aligns with X era" statements. Interpretation belongs in README (with correlation-not-causation language).

## Shark-Jump Algorithm (exact specification)
1. For each season of a show, compute season_weighted_avg using the weighted
   rating formula: SUM(avg_rating × num_votes) / SUM(num_votes) across all
   episodes in that season.
2. Compute series_weighted_avg across ALL episodes of the show (same formula).
3. For each season S (where S >= 3), compute rolling_3_season_avg as the
   average of season_weighted_avg for seasons [S-2, S-1, S].
4. Define below(S) = True when rolling_3_season_avg(S) < series_weighted_avg.
5. Find the earliest season S such that below(S) == True AND below(S+1) == True.
6. Shark-jump season = S (the first season in the consecutive below-average run).
7. If no such consecutive pair exists, the show has NOT jumped the shark.
Do not modify this algorithm without explicit approval.

## Episode Inclusion Rules
- Use title.episode to select episodes where parentTconst matches one of the
  4 selected show tconsts.
- Exclude specials: drop rows where seasonNumber is NULL, "\N", or 0.
- Drop rows with missing or "\N" seasonNumber or episodeNumber after parsing.
- Treat all "\N" values in IMDb TSVs as NULL during parsing.
- After filtering, verify no unexpected titleType values remain (should be
  "tvEpisode" only).

## Non-Negotiable Rules
- NO ML, recommendation systems, or sentiment analysis. Analytics only.
- NO scraping. IMDb public TSV datasets only (https://datasets.imdbws.com/).
- NO hardcoded show names in dashboard titles or annotations. Use a show selector filter.
- NO loading full IMDb TSV files into SQL or Tableau. Filter in Python FIRST — read
  in chunks or filter on read, keep only episodes for the 4 selected shows. Export
  filtered data to small CSVs before any SQL or Tableau work.
- NO fuzzy title matching anywhere. All joins use tconst (IMDb stable identifier).
- NO causal claims about WHY shows declined. Stick to WHEN and HOW MUCH. You may note
  correlations ("SpongeBob's decline coincides with the creator's departure") but frame
  them as observations, not conclusions.
- NO fabricated financial or revenue data. No dollar amounts, no made-up CPM/impressions/
  cost figures. Business value comes from decision-support framing on real data only.
- Never delete or weaken tests/QA checks to make them pass. Fix the code, not the test.
  If a test is genuinely wrong, explain why and propose a better test.
- Never hardcode secrets, file paths, or API keys. Use .env or config variables.
- Ask clarifying questions rather than making assumptions.
- Push back if you think my approach has problems — explain why and suggest alternatives.
- Use Plan Mode (Shift+Tab) before adding dependencies, changing the data model, or
  refactoring across multiple files.
- If I ask for something not in the project plan, warn me about scope creep.
- README must include the required IMDb attribution statement and a link to
  IMDb's conditions of use (https://www.imdb.com/conditions).

## Git Data Policy
- Raw IMDb TSV/TSV.GZ files: NEVER committed. Always .gitignored.
- Full derived CSVs from real IMDb data: NEVER committed. Always .gitignored.
  The /data directory is .gitignored (except data/sample/).
- data/sample/: Contains small SYNTHETIC test data generated by a script in
  qa/fixtures/ — NOT real IMDb-derived rows. Used so someone can clone the
  repo and run QA/tests without downloading anything. COMMITTED to git.
- expected_outputs/: Small "golden" reference outputs generated from the
  synthetic test data. Used for QA comparison. COMMITTED to git.
- qa/fixtures/: Script(s) that generate synthetic test data. COMMITTED.
- The pipeline script that processes real IMDb data outputs to /data (gitignored).
  The README explains how to download IMDb TSVs and run the pipeline.

## File Organization
- pipeline/         — Python .py scripts (ETL, subsetting, data cleaning)
- sql/              — SQL query files (3 files: schema, KPIs, shark-jump detection)
- data/             — .gitignored (real IMDb-derived outputs go here)
- data/sample/      — synthetic test data for demo/QA (committed)
- expected_outputs/ — reference outputs from synthetic data for QA (committed)
- qa/               — validation scripts (qa/validate.py is the entry point)
- qa/fixtures/      — scripts to generate synthetic test data
- excel/            — QA reconciliation workbook
- screenshots/      — Tableau dashboard screenshots (2–3 images)
- docs/             — PROJECT_CONTEXT.md (full project plan — source of truth)
- tableau_link.txt  — Tableau Public URL
- README.md         — business context, metrics, data sources, IMDb attribution,
                      limitations, instructions to reproduce
- CLAUDE.md         — this file

## Business Framing (presentation layer — does not change pipeline)
- Use business language for metrics: "Catalog Value Index"
  (weighted_rating × LOG10(1 + season_total_votes)), "Licensing Priority Rank"
  (season rank), vote count as "engagement proxy."
- Tile 8: "Top N seasons to license" with adjustable Tableau parameter N.
- README frames analysis as streaming platform licensing decision support.
- Do NOT claim dollar amounts or fabricate financial data. Decision-support only.
- Optional v2 enhancements (NOT for MVP): "Renewal Risk Index" composite
  (decline + volatility + engagement drop), "Marginal Value Curve" showing
  diminishing returns of licensing additional seasons.

## Commands
These commands are defined as the project is built. Not all exist at the start.

### Available now:
- (none yet — project is in setup phase)

### To be created in Phase 1 (Python pipeline):
- Run pipeline on full data: python pipeline/01_subset_imdb.py
- Run pipeline on sample: python pipeline/01_subset_imdb.py --sample
- Generate synthetic test data: python qa/fixtures/generate_synthetic.py

### To be created in Phase 2 (SQL):
- Run SQL queries: python pipeline/02_run_sql.py (or manual DuckDB CLI)

### To be created alongside Phase 1–2 (QA):
- Run QA validation: python qa/validate.py
- Run all checks: python qa/validate.py --all
  (This is the "definition of done" command. It validates: episode counts,
  weighted rating spot-checks, no duplicate tconsts, shark-jump sanity,
  vote count reasonableness.)

## Definition of Done
A phase is NOT done until qa/validate.py passes with zero errors.
Do not tell me something is complete unless QA checks pass.

### QA Checks (from project spec):
1. Episode count pivot: show × season COUNT must match SQL agg_season_kpis.episode_count
2. Weighted rating spot-check: manual calc within ±0.01 of SQL output
3. No duplicate episode tconsts (all values unique)
4. Shark-jump season is not Season 1 or 2 unless genuinely justified
5. Vote count spot-check: 2–3 episodes verified against IMDb website

## Phase Order
1. **Python pipeline:** Download + filter IMDb TSVs → clean CSVs + synthetic sample data
2. **SQL (DuckDB):** Build star schema + season KPIs + shark-jump detection
3. **QA:** Excel/Sheets reconciliation workbook + qa/validate.py
4. **Tableau:** Dashboard (8 tiles per project plan) with business framing
5. **README + screenshots + publish**

Build one phase at a time. Do not start the next phase until the current one passes QA.

## Important
- The full project plan is in docs/PROJECT_CONTEXT.md — it is the source of truth
  for the data model, SQL query skeletons, Tableau tile plan, QA checklist, scope
  traps, and interview demo script.
- When in doubt, refer to PROJECT_CONTEXT.md, not your own assumptions.
- Where CLAUDE.md and PROJECT_CONTEXT.md conflict, CLAUDE.md takes precedence
  (CLAUDE.md reflects later decisions made during planning).
