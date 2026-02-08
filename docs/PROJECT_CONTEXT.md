# PROJECT CONTEXT BLOCK: TV Series Drop-off Analysis ("Jump the Shark")
## Full project spec — source of truth for data model, SQL, Tableau tiles, QA

> **NOTE:** Where this document and CLAUDE.md conflict, CLAUDE.md takes precedence.
> CLAUDE.md reflects later planning decisions including: repo rename to
> `tv-series-dropoff-analysis`, DuckDB (not SQLite), .py scripts (not notebooks),
> business framing additions, and git data policy.
>
> This document has been updated to reflect: DuckDB as confirmed tool, updated
> Catalog Value Index formula (engagement-weighted), explicit Durability Index
> definition, shark-jump SQL off-by-one fix, expanded Q2 SQL with all schema KPIs,
> Python subsetting roadmap, episode inclusion rules, and tightened IMDb licensing
> wording.

---

## 1. PROJECT OVERVIEW

### Recruiter-Friendly Title
**TV Series Drop-off Analysis: Detecting "Jump-the-Shark" Inflection Points in Long-Running Franchises**

### 1-Sentence Summary
Builds an episode-level analytics model on IMDb data to detect where a franchise's quality trend structurally breaks ("jumps the shark"), and recommends which seasons maximize catalog value for licensing and promotion.

### Core Business Question
"When does a long-running TV franchise stop being high-value, and which specific seasons should a streaming platform license or promote?"

### Why This Project Was Chosen (over alternatives)
This project was selected through a bias-minimized evaluation process comparing it against a "Streaming Content Strategy: TV Golden Age" genre-trend project. This project won because:
- **Unique business signal:** Episode-level franchise lifecycle analysis with a named detection metric is distinctive — not a standard Kaggle tutorial.
- **Stronger data grain:** Episode → Season → Show relational modeling with SQL window functions, vs. the alternative's flat show-level aggregation.
- **Deliverable hook is answerable:** The alternative's headline ("Golden Age of Simpsons") was unanswerable with its own recommended show-level dataset. This project's IMDb TSVs provide episode-level data that actually supports season-level analysis.
- **Better interview narrative:** "I found The Simpsons jumped the shark at Season X" is specific and memorable vs. "Drama rose, Comedy fell."
- **Best extensibility:** The episode-level IMDb pipeline can later expand to more shows, more formats, or even become the genre-level analysis the alternative proposed — but built on richer data.

---

## 2. SELECTED SHOWS (4 for MVP)

| # | Show | Format | Why Selected | Expected Pattern |
|---|---|---|---|---|
| 1 | **The Simpsons** | Animation | Anchor case — longest-running, most famous decline | Gradual long-term decline (classic "shark jump") |
| 2 | **SpongeBob SquarePants** | Animation (kids/family) | Creator Stephen Hillenburg left after Season 3/movie; sharp quality drop | Sharp inflection point after creator departure — strong "event-driven decline" story |
| 3 | **Family Guy** | Animation | Cancelled, brought back; has dip + recovery patterns | Complex non-linear lifecycle — proves model handles recoveries, not just declines |
| 4 | **The Walking Dead** | Live-action | Most well-documented quality decline in recent live-action TV | Cross-format proof — shows the model isn't animation-specific; famous S7-8 drop-off |

### Why These 4 Specifically
- **4 distinct lifecycle shapes:** long decline, sharp inflection, decline-recovery, cross-format decline.
- **Analytical diversity:** 3 animation + 1 live-action proves format-agnostic framework.
- **All have enough seasons** for rolling-average detection to work (Simpsons 35+, SpongeBob 14+, Family Guy 22+, TWD 11 seasons).
- **All are culturally well-known** — interviewers and recruiters will immediately understand the stories.

### Shows Reserved for v2 Expansion
South Park, Rick & Morty, Adventure Time, Regular Show, Grey's Anatomy, Supernatural. These can be added later by simply appending their `show_tconst` IDs to the config list and rerunning the pipeline.

---

## 3. NON-NEGOTIABLE CONSTRAINTS

- **MVP time:** 10–15 hours total (1–2 days). Hard cap.
- **Budget:** $0. Mac + free tools only, end-to-end.
- **Primary deliverable:** Tableau Public dashboard (publishable link + screenshots).
- **Required stack (all must appear in MVP):**
  - SQL (CTEs + window functions)
  - Python (pandas/numpy)
  - Tableau Public
  - Excel/Google Sheets QA (pivots/reconciliation/sanity checks)
- **Business-centric framing:** Every viz must answer "what decision would a team make from this?"
- **NO heavy ML / recommendation systems.** Analytics only (segmentation, cohorts, lifecycle/drop-off, rolling averages, ranking).
- **NO ToS-risk scraping.** Use IMDb public TSV datasets only.
- **NO fuzzy title matching.** Use stable IMDb `tconst` identifiers throughout.

---

## 4. DATASET PLAN

### Primary Source: IMDb Public TSV Datasets
- **URL:** https://datasets.imdbws.com/
- **License:** Used under IMDb's non-commercial dataset terms (https://www.imdb.com/conditions).
  Do not commit raw or derived IMDb data to public repos. README must include IMDb attribution.
- **No API key needed.** Direct download of .tsv.gz files.

### Files Needed

| File | Key Columns | Purpose |
|---|---|---|
| `title.basics.tsv.gz` | tconst, titleType, primaryTitle, startYear, endYear, runtimeMinutes, genres | Show metadata |
| `title.episode.tsv.gz` | tconst, parentTconst, seasonNumber, episodeNumber | Episode-to-show mapping |
| `title.ratings.tsv.gz` | tconst, averageRating, numVotes | Ratings for each episode |

### Manual Enrichment Table (You Create)
`dim_show_category` — 4 rows:

| show_tconst | title | category |
|---|---|---|
| (Simpsons tconst) | The Simpsons | adult_animation |
| (SpongeBob tconst) | SpongeBob SquarePants | kids_animation |
| (Family Guy tconst) | Family Guy | adult_animation |
| (TWD tconst) | The Walking Dead | live_action |

### Join Strategy
- All joins use `tconst` (IMDb's stable unique identifier). No fuzzy title matching anywhere.
- `title.episode.parentTconst` → links episodes to their parent show's `tconst`.
- `title.ratings.tconst` → links ratings to episodes via episode `tconst`.

### Data Size Warning
- Raw IMDb TSV files are large (title.basics ~800MB, title.episode ~250MB, title.ratings ~25MB).
- **MUST filter in Python immediately on load** — read in chunks or filter on read, keep only episodes belonging to the 4 selected shows. Export filtered data to small CSVs (<1MB total) before loading into SQL.

### Python Subsetting Order (exact steps)
1. Read `title.episode.tsv.gz` in chunks → keep rows where `parentTconst` is in SHOW_IDS.
2. Collect the set of episode `tconst` values from the filtered episodes.
3. Read `title.ratings.tsv.gz` → keep rows where `tconst` is in the episode tconst set.
4. Read `title.basics.tsv.gz` in chunks → keep rows matching show-level tconsts (for dim_show)
   and optionally episode-level tconsts (for runtime if needed later).
5. Treat all `\N` values as NULL during parsing (IMDb uses `\N` for missing data).
6. Exclude specials: drop rows where seasonNumber is NULL or 0.
7. Drop rows with missing seasonNumber or episodeNumber after parsing.
8. Verify remaining rows have titleType = "tvEpisode" only.
9. Export clean CSVs to /data (gitignored) and generate synthetic sample to data/sample/ (committed).

### IMDb tconst IDs for Selected Shows
These need to be looked up on IMDb, but the expected IDs are:
- The Simpsons: `tt0096697`
- SpongeBob SquarePants: `tt0206512`
- Family Guy: `tt0182576`
- The Walking Dead: `tt1520211`

**IMPORTANT:** Verify these tconst IDs before running the pipeline. Search each show on imdb.com and confirm the ID in the URL.

---

## 5. DATA MODEL (Star Schema)

```
dim_show (grain: 1 row per show)
├── show_tconst (PK)
├── title
├── start_year
├── end_year
├── genres
└── show_category (from enrichment: adult_animation / kids_animation / live_action)

fact_episode (grain: 1 row per episode)
├── episode_tconst (PK)
├── show_tconst (FK → dim_show)
├── season_num
├── episode_num
├── avg_rating
└── num_votes

agg_season_kpis (grain: 1 row per show-season)
├── show_tconst (FK → dim_show)
├── season_num
├── episode_count
├── season_total_votes [SUM(num_votes) for the season]
├── weighted_rating  [SUM(avg_rating × num_votes) / SUM(num_votes)]
├── mean_rating
├── rating_stddev
├── pct_high_rated   [% episodes above show's overall avg]
├── season_rank_best [RANK() window by weighted_rating per show]
├── rolling_3_season_avg
├── shark_jump_flag  (0/1)
└── catalog_value_index [weighted_rating × LOG10(1 + season_total_votes)]
```

---

## 6. KEY METRIC DEFINITIONS

| Metric | Definition | Purpose |
|---|---|---|
| **Weighted Rating** | `SUM(avg_rating × num_votes) / SUM(num_votes)` per season | More reliable than simple average — weights by vote confidence |
| **Rolling 3-Season Avg** | `AVG(weighted_rating) OVER (PARTITION BY show ORDER BY season ROWS BETWEEN 2 PRECEDING AND CURRENT ROW)` | Smooths noise; detects trend, not single-season dips |
| **Series Avg** | `AVG(weighted_rating) OVER (PARTITION BY show)` | Baseline for comparison |
| **Shark-Jump Season** | First season where rolling 3-season avg drops below series avg for **2 consecutive seasons** | Simple, defensible, explainable rule. "Two consecutive below-average seasons = structural decline, not a blip." |
| **Catalog Value Index** | `weighted_rating × LOG10(1 + total_votes_in_season)` per season | Engagement-weighted quality proxy for licensing value. LOG dampens outlier vote counts so high-volume shows don't dwarf everything. |
| **Durability Index** | Count of seasons where rolling_3_season_avg >= series_avg | Measures how long a franchise sustains above-average quality using smoothed trend (not raw season noise) |

### Shark-Jump Rule — Design Decisions
- **Why rolling 3-season avg, not single season?** Reduces false positives from one-off bad seasons.
- **Why 2 consecutive seasons below avg?** Distinguishes structural decline from a temporary dip. Family Guy's cancellation/return makes this important.
- **What if a show never triggers?** Label it "No shark-jump detected" — that's a valid, interesting finding.
- **What about SpongeBob's creator departure?** The data should show this naturally. If the shark-jump season aligns with Hillenburg's departure (after Season 3 / 2004 movie), that's a strong story — but frame it as correlation, not causation.

---

## 7. SQL DELIVERABLES (3 Must-Have Queries)

### Q1: Build Episode Fact Table (CTE + JOIN)
```sql
WITH ep AS (
  SELECT
    e.tconst              AS episode_tconst,
    e.parentTconst        AS show_tconst,
    CAST(e.seasonNumber AS INT)  AS season_num,
    CAST(e.episodeNumber AS INT) AS episode_num
  FROM title_episode e
  WHERE e.seasonNumber IS NOT NULL
    AND e.episodeNumber IS NOT NULL
),
rt AS (
  SELECT tconst,
    CAST(averageRating AS REAL) AS avg_rating,
    CAST(numVotes AS INT)       AS num_votes
  FROM title_ratings
)
SELECT ep.*, rt.avg_rating, rt.num_votes
FROM ep
JOIN rt ON rt.tconst = ep.episode_tconst;
```

### Q2: Season KPIs with Window Ranking (CTE + RANK)
```sql
WITH season_base AS (
  SELECT
    show_tconst, season_num,
    COUNT(*)                                              AS episodes,
    SUM(avg_rating * num_votes) * 1.0
      / NULLIF(SUM(num_votes), 0)                        AS weighted_rating,
    AVG(avg_rating)                                       AS mean_rating,
    STDDEV(avg_rating)                                    AS rating_stddev,
    SUM(num_votes)                                        AS season_total_votes
  FROM fact_episode
  GROUP BY show_tconst, season_num
),
with_series_avg AS (
  SELECT *,
    AVG(weighted_rating) OVER (PARTITION BY show_tconst)  AS series_avg,
    AVG(weighted_rating) OVER (
      PARTITION BY show_tconst ORDER BY season_num
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    )                                                     AS rolling_3_season_avg
  FROM season_base
),
with_pct AS (
  SELECT s.*,
    -- pct_high_rated: % of episodes in this season above the show's overall avg
    (SELECT COUNT(*) FROM fact_episode e2
     WHERE e2.show_tconst = s.show_tconst
       AND e2.season_num = s.season_num
       AND e2.avg_rating >= s.series_avg) * 100.0
    / NULLIF(s.episodes, 0)                               AS pct_high_rated
  FROM with_series_avg s
),
ranked AS (
  SELECT *,
    RANK() OVER (PARTITION BY show_tconst
                 ORDER BY weighted_rating DESC)            AS season_rank_best,
    weighted_rating * LOG10(1 + season_total_votes)        AS catalog_value_index
  FROM with_pct
)
SELECT * FROM ranked;
```

### Q3: Shark-Jump Detection (CTE + Rolling Window + LAG)
```sql
WITH season_kpis AS (
  SELECT show_tconst, season_num,
    SUM(avg_rating * num_votes) * 1.0
      / NULLIF(SUM(num_votes), 0) AS weighted_rating
  FROM fact_episode
  GROUP BY show_tconst, season_num
),
with_roll AS (
  SELECT *,
    AVG(weighted_rating) OVER (
      PARTITION BY show_tconst ORDER BY season_num
      ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
    ) AS rolling_3_season_avg,
    AVG(weighted_rating) OVER (PARTITION BY show_tconst) AS series_avg
  FROM season_kpis
),
flagged AS (
  SELECT *,
    CASE WHEN rolling_3_season_avg < series_avg THEN 1 ELSE 0 END AS below_avg_flag,
    LAG(CASE WHEN rolling_3_season_avg < series_avg THEN 1 ELSE 0 END)
      OVER (PARTITION BY show_tconst ORDER BY season_num) AS prev_below
  FROM with_roll
)
SELECT show_tconst,
  MIN(season_num - 1) AS shark_jump_season
FROM flagged
WHERE below_avg_flag = 1 AND prev_below = 1
GROUP BY show_tconst;
```

---

## 8. TABLEAU PUBLIC DASHBOARD PLAN (8 Tiles)

### Story Arc
"Which shows stay great the longest?" → "Where do they break?" → "What seasons are worth licensing?"

### Critical Design Decisions
- **Show selector filter from Day 1** — do NOT hardcode for a single show. Title the dashboard "Franchise Shark-Jump Analysis" not "Simpsons Analysis."
- **All calculations partition by show_tconst** — so adding shows later requires zero dashboard rework.
- **Parameterized pipeline** — adding shows in v2 is just adding IDs to a Python config list and rerunning.

### Tile Layout

| Tile | Type | Content |
|---|---|---|
| 1 | KPI cards + show selector dropdown | Series avg rating, best season #, shark-jump season #, total episodes |
| 2 | Line chart | Weighted rating by season per show; shark-jump season marked with annotation |
| 3 | Dual-line overlay | Rolling 3-season avg vs. series avg — visual proof of where the break happens |
| 4 | Horizontal bar chart | Season ranking (best → worst) per show by weighted rating |
| 5 | Box plot or histogram | Episode rating distribution within selected season |
| 6 | Variance tile | Rating standard deviation by season (consistency/volatility measure) |
| 7 | Cross-show comparison bar chart | Durability index: # of seasons above threshold, per show |
| 8 | Action table | "What to License/Promote" — top N seasons with episode count + catalog value index |

---

## 9. MVP PLAN (10–15 Hours)

| Step | Task | Est. Hours |
|---|---|---|
| 1 | Verify IMDb tconst IDs for 4 shows | 0.25 |
| 2 | Download IMDb TSVs; Python script to filter to 4 shows + their episodes; export clean CSVs | 1.5 |
| 3 | Create dim_show_category enrichment table (4 rows, manual) | 0.25 |
| 4 | Load CSVs into DuckDB; create star schema tables | 1.0 |
| 5 | Write 3 SQL queries (fact build, season KPIs, shark-jump detection) | 2.0 |
| 6 | Export SQL outputs to CSV for Tableau + Excel QA | 0.5 |
| 7 | Excel QA: pivot checks, weighted-rating spot-check, duplicate audit, shark-jump sanity | 1.0 |
| 8 | Build Tableau Public dashboard (8 tiles + story flow + show selector filter) | 4.0 |
| 9 | README + screenshots + publish to Tableau Public + push to GitHub | 1.5 |
| **Total** | | **~12 hours** |
| Buffer | Polish, edge cases, formatting | 2–3 hours |

### GitHub Folder Structure
```
/tv-series-dropoff-analysis
├── /pipeline          (Python .py pipeline scripts — ETL, subsetting)
├── /sql               (3 query .sql files)
├── /data              (.gitignored except data/sample/)
│   └── /sample        (synthetic test data for demo/QA — committed, NOT IMDb-derived)
├── /expected_outputs  (small reference outputs for QA comparison — committed)
├── /qa                (qa/validate.py — validation entry point)
├── /excel             (QA workbook)
├── /screenshots       (Tableau dashboard screenshots, 2–3)
├── /docs              (PROJECT_CONTEXT.md — full project spec)
├── CLAUDE.md          (agent instructions — takes precedence over this doc)
├── tableau_link.txt   (Tableau Public URL)
└── README.md          (business context, metric definitions, data sources, limitations, future enhancements)
```

---

## 10. EXCEL/SHEETS QA CHECKLIST

1. **Episode count pivot:** Rows = Show × Season → COUNT of episodes. Must match SQL `agg_season_kpis.episode_count` exactly.
2. **Weighted rating spot-check:** Manually compute `SUM(rating × votes) / SUM(votes)` for one season of one show in Excel. Must match SQL output within ±0.01.
3. **No duplicate episode keys:** COUNTIF on `episode_tconst` column — all values must = 1.
4. **Shark-jump sanity:** Shark-jump season should not be Season 1 or 2 unless the show genuinely tanked immediately. Flag and investigate if so.
5. **Vote count reasonableness:** Spot-check 2–3 episodes against the IMDb website to confirm `num_votes` is in the right ballpark.

---

## 11. RESUME BULLETS (Metric Placeholders)

1. "Built an episode-level SQL model (CTEs + window functions) on IMDb data to identify franchise 'shark-jump' inflection points across [N] long-running TV series, ranking [K] seasons by votes-weighted catalog value to inform content licensing strategy."

2. "Delivered a Tableau Public story dashboard + Excel QA workbook operationalizing [K] KPIs (durability index, rating consistency, break-point season), enabling data-driven decisions on season-level promotion and licensing."

---

## 12. INTERVIEW DEMO SCRIPT (60–90 Seconds)

> "This dashboard answers a question streaming content teams deal with every day: when does a franchise stop being high-value?
>
> I modeled IMDb episode data into a season-level star schema — episodes roll up to seasons, seasons roll up to shows. Then I used SQL window functions to compute a rolling 3-season weighted average and detect the first point where it permanently drops below the series baseline. That's my 'shark-jump' rule.
>
> [Tile 2] This line chart shows The Simpsons' season-by-season trajectory — you can see the break right here at Season [X], and it never recovers. Compare that to SpongeBob [click filter], where the decline is sharper and aligns with the creator's departure after Season 3.
>
> [Tile 7] This cross-show view ranks franchise durability — and notice The Walking Dead is here too, proving the model works for live-action, not just animation.
>
> [Tile 8] This table is the action item: if you can only license 5 seasons of The Simpsons, these are the 5 with the highest catalog value index.
>
> I validated all outputs with Excel pivot checks against the raw SQL, and the pipeline is parameterized so I can add more shows by just appending IDs to a config list."

---

## 13. SCOPE TRAPS TO AVOID

1. **Don't load full IMDb TSVs into Tableau or SQL without filtering first.** Raw files are 500MB+. Subset to your 4 shows in Python BEFORE anything else.
2. **Don't invent causal stories.** "The writers got worse" or "the network interfered" are not supported by ratings data. Stick to measurable trends. You CAN note correlations ("SpongeBob's decline coincides with the creator's departure") but frame them as observations, not conclusions.
3. **Don't over-engineer the shark-jump definition.** Keep it simple: rolling avg < series avg for 2+ consecutive seasons. A simple, defensible rule beats a complex one you can't explain in 60 seconds.
4. **Don't hardcode show names in dashboard titles/annotations.** Use a show selector filter and generic naming so v2 expansion is seamless.
5. **Don't try to add episode-level detail in Tableau beyond what the 4-show dataset supports.** Thousands of episode rows across 4 shows is fine. Don't accidentally load all of IMDb.

---

## 14. ITERABILITY & FUTURE ENHANCEMENTS (for README)

This project is fully iterable:
- **Tableau Public** allows unlimited republishing — same URL, updated content.
- **Python pipeline** is parameterized — adding shows = adding tconst IDs to a config list and rerunning.
- **SQL queries** partition by show_tconst — no query changes needed for new shows.
- **Dashboard** has show selector filter from day 1 — new shows appear automatically.

### Planned v2 Enhancements
- Expand to 8–10 shows (add South Park, Rick & Morty, Adventure Time, Grey's Anatomy, Supernatural)
- Cross-format lifecycle comparison (animation vs. live-action durability analysis)
- Add a "recovery detection" metric for shows like Family Guy that decline then rebound
- Genre-level aggregation layer (bridges to the alternate "Golden Age" project concept using richer episode-level data)

---

## 15. KEY DESIGN DECISIONS & DISCUSSION NOTES

### Why 4 Shows (Not 3 or 6)
- **3 feels thin** ("I picked a few examples"). **6 is overkill for MVP** — save for v2.
- **4 feels like a framework** ("I built a model that scales across formats").
- Time difference between 3 and 4 shows is ~15 minutes of Python subsetting.
- Interview impact: "I analyzed 4 franchises across 2 formats" > "I analyzed 3 cartoons."

### Why SpongeBob Over South Park
- SpongeBob has a **dramatic creator-departure inflection point** (Stephen Hillenburg left after Season 3 / first movie in 2004) — makes for a sharper "event-driven decline" story vs. South Park's more gradual/consistent pattern.
- Adds **audience diversity** (kids/family vs. adult animation vs. live-action).
- South Park is reserved for v2.

### Why The Walking Dead as the Live-Action Pick
- **Most famous quality decline in recent live-action TV** — Seasons 1-6 avg ~8.5, Seasons 7+ avg ~7.2.
- **You don't need to have watched it** — this is a data project, not a fan essay. The ratings data tells the story clearly.
- Proves the shark-jump model is **format-agnostic**, which is the single strongest recruiter signal upgrade from an all-animation lineup.

### On Not Having Watched All Shows
- Personal viewing history is irrelevant for a quantitative data project. You're analyzing rating trends, not writing episode critiques.
- For shows you haven't watched deeply (TWD, possibly SpongeBob later seasons): spend 10 minutes reading Wikipedia/IMDb summaries to understand key context events. Let the data tell the story.

### Tools Discussion
- **DuckDB** (confirmed choice) for local SQL work. DuckDB is faster for analytical queries, reads CSV/TSV natively, and is increasingly popular in the data community. Free, local, no server setup.
- **Python (pandas/numpy)** for data cleaning and subsetting. Standard data analyst stack.
- **Tableau Public** as primary dashboard deliverable. Free, publishable, shareable link.
- **Excel/Google Sheets** for QA reconciliation. Demonstrates you validate your own work — a strong recruiter signal.

### What NOT to Build
- No recommendation engine. No ML. No collaborative filtering.
- No sentiment analysis of reviews. That's NLP, not analytics.
- No causal inference about WHY shows declined. Stick to WHEN and HOW MUCH.
- No scraping. IMDb TSVs are the clean, legal path.

---

## 16. ADDITIONAL IMPROVEMENT NOTES

### Modern Tool Signals (Within Timeframe)
- **DuckDB (confirmed):** Mention DuckDB in README as a modern OLAP-oriented choice. DuckDB is increasingly valued in analytics roles — it signals you know current tooling trends.
- **Python config pattern:** Use a simple `SHOW_IDS` list at the top of your script. This is a basic but important software engineering practice that shows you think about maintainability:
  ```python
  SHOW_IDS = ['tt0096697', 'tt0206512', 'tt0182576', 'tt1520211']
  ```
- **Data dictionary:** Include a brief one in your README (table name, grain, key columns, metric formulas). Takes 15 minutes, signals data governance awareness.
- **Git commits:** Make meaningful commits at each phase ("Add Python subsetting script", "Add SQL season KPI queries", "Add Tableau dashboard v1"). Shows professional workflow even on a solo project.

### Stretch Goals (Only If Ahead of Schedule)
- Add a "recovery detection" flag for Family Guy (did it improve after being un-cancelled?).
- Compute a "best 5 seasons to license" recommendation per show.
- Add a brief Loom video walkthrough (90 seconds) linked in README — increasingly common in portfolio projects.

---

## 17. QUICK REFERENCE: WHAT TO TELL THE NEW CHAT

When starting a new chat, paste this entire document and then say something like:

> "I'm building this portfolio project. The context document above contains the full plan, data model, SQL queries, and Tableau dashboard plan. I'm ready to start Phase [X]. Help me with [specific task]."

Suggested first request:
> "Help me write the Python script to download and filter the IMDb TSV files for my 4 selected shows, exporting clean CSVs ready for DuckDB."
