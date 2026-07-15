# Phase 4 — Tableau Public Build Spec (v2, MERGED FINAL)
**Project:** TV Series Drop-off Analysis ("Jump the Shark")
**Precedence:** `CLAUDE.md` > `docs/PROJECT_CONTEXT.md`. Where older PROJECT_CONTEXT wording sounds interpretive, this spec uses the stricter current rules: neutral language, no causal claims, no metric recomputation.
**Status:** Pre-build gates passed (schema, reference values, S1/S2 rolling verified). Build one data source → 9 worksheets → 1 dashboard → publish.
**Repo home:** `docs/PHASE4_TILE_SPEC.md`

---

## Final locked choices

| Area | Decision |
|---|---|
| Data model | Existing 5 CSVs + Tableau relationships (no denormalized export, no physical joins) |
| Dashboard format | One dashboard, no Tableau Story |
| Default state | Show selector defaults to **All** |
| KPI behavior | Always-valid overview cards show real values; show-specific cards show "Select a show" under All |
| Tile 7 | Global show selector filters it; cross-show comparison lives in the All view |
| Tile 8 | Top N by `catalog_value_index` (via `Season Key` target), **not** `season_rank_best` |
| Rolling line | S3+ display calc only; pipeline data unchanged |
| Fan-out rule | Never mix episode-grain and season-grain fields in one worksheet (incl. tooltips) |
| Naming | Generic dashboard title + workbook filename; no hardcoded show names anywhere |
| Attribution | **On-dashboard IMDb footer** + README attribution in Phase 5 |

Dashboard title: **`Franchise Lifecycle Analysis`**

---

## 0. Build order (follow exactly)

1. Connect 5 CSVs as ONE data source.
2. Build relationships (§1).
3. Verify table row counts in preview (§1).
4. **Run the fan-out smoke test (§1) — hard gate. Do not build a tile until it passes.**
5. Create the parameter + calculated fields (§2).
6. **Build Tile 5 first** (locks episode-grain isolation before any season tile).
7. Build season-grain tiles: 2, 3, 4, 6, 8.
8. Build show-level Tile 7.
9. Build KPI Tile 1.
10. Assemble the dashboard (§4); apply global show selector to all worksheets; scope the Tile 5 season selector to Tile 5 only.
11. Test the All view, then each single-show view.
12. Run the segmented QA checklist (§5).
13. Publish to Tableau Public; save a local `.twb` backup (do NOT commit it).

---

## 1. Data model setup + fan-out smoke test

### Connect (separate logical tables — do NOT physically join)
```
docs/dim_show_category.csv
data/agg_season_kpis.csv
data/shark_jump_results.csv
data/durability_index.csv
data/episodes_filtered.csv
```

### Relationship canvas (hub-and-spoke)
Set `dim_show_category` as the hub; relate each spoke on **`show_tconst`**:
```
dim_show_category (hub)
├── agg_season_kpis      on show_tconst   (1 → many)
├── episodes_filtered    on show_tconst   (1 → many)
├── shark_jump_results   on show_tconst   (1 → 1)
└── durability_index     on show_tconst   (1 → 1)
```
**Hard rule:** do NOT relate `episodes_filtered` directly to `agg_season_kpis`.

Performance options: hub side = **One**; `agg_season_kpis` & `episodes_filtered` = **Many**; `shark_jump_results` & `durability_index` = **One**. Referential integrity: **"Some records match"** (safe default).

### Data types (verify in Data pane)
- String: `show_tconst`, `title`, `category`, `episode_tconst`
- Whole number: `season_num`, `episode_num`, `episode_count`, `num_votes`, `season_rank_best`, `shark_jump_season`, `durability_index`
- Decimal: `weighted_rating`, `mean_rating`, `rating_stddev`, `pct_high_rated`, `series_avg`, `rolling_3_season_avg`, `catalog_value_index`, `avg_rating`, `season_total_votes`

### Table row-count preview (import check)
| Table | Expected |
|---|---:|
| `dim_show_category` | 4 |
| `agg_season_kpis` | 87 |
| `episodes_filtered` | 1751 |
| `shark_jump_results` | 4 |
| `durability_index` | 4 |

### Fan-out smoke test (MANDATORY — active test, not just the previews)
Row-count previews only confirm the import; fan-out shows up when fields are combined. Before building tiles:
- **Test A:** Rows = `title`; Columns = `SUM([episode_count])` → must read **The Simpsons 802, SpongeBob 326, Family Guy 446, The Walking Dead 177**.
- **Test B:** Rows = `title`; Columns = `COUNTD([episode_tconst])` → same four totals.

If `SUM([episode_count])` is inflated, a relationship/grain is wrong — **stop and fix** before any tile. Delete the scratch sheets afterward.

---

## 2. Parameter + calculated fields (create first)

None recompute a metric — these are flags, labels, conditionals, and a filter/count target.

**Parameter `p_Top N Seasons`** — Integer · range 1–15 · default 5 · Tile 8 only.

**`Season Key`** (in `agg_season_kpis`) — used by Tile 1 (count) and Tile 8 (Top-N target)
```
[show_tconst] + "-" + STR([season_num])
```

**`Season Label`** (in `agg_season_kpis`)
```
"S" + STR(INT([season_num]))
```

**`Rolling 3S Avg Display`** (in `agg_season_kpis`) — Tile 3 gap for S1–S2
```
IF [season_num] >= 3 THEN [rolling_3_season_avg] END
```

**`Shark Jump Marker Rating`** (Tile 2 — aggregate; resolves at mark grain)
```
IF MIN([season_num]) = MIN([shark_jump_season]) THEN AVG([weighted_rating]) END
```

**`Shark Jump Label`** (Tile 2 label/tooltip)
```
IF ISNULL(MIN([shark_jump_season])) THEN "No shark-jump detected"
ELSE "Shark-jump: S" + STR(INT(MIN([shark_jump_season]))) END
```

**`KPI Series Avg Display`** (Tile 1, gated)
```
IF COUNTD([title]) = 1 THEN STR(ROUND(AVG([series_avg]), 2)) ELSE "Select a show" END
```

**`KPI Best Season Display`** (Tile 1, gated — used on a sheet filtered to `season_rank_best = 1`)
```
IF COUNTD([title]) = 1 THEN "S" + STR(INT(MIN([season_num]))) ELSE "Select a show" END
```

**`KPI Shark Jump Display`** (Tile 1, gated, null-safe)
```
IF COUNTD([title]) = 1 THEN
    IF ISNULL(MIN([shark_jump_season])) THEN "No shark-jump detected"
    ELSE "S" + STR(INT(MIN([shark_jump_season]))) END
ELSE "Select a show" END
```

**Used directly (no calc):** Total rated episodes → `SUM([episode_count])`; Franchises analyzed → `COUNTD([title])`; Seasons analyzed → `COUNTD([Season Key])`.

---

## 3. Global dashboard controls

**Show selector** — filter on `dim_show_category.title`; Single Value (dropdown) with an "All" option; default **All**; **Apply to Worksheets → All Using This Data Source**. (It's a filter, not a parameter. Show values may appear as data; never hardcode them into titles/labels/calcs.)

**Tile 5 season selector** — filter on `episodes_filtered.season_num`; Single Value (dropdown); applied to **Tile 5 worksheet only** (do NOT use `agg_season_kpis.season_num`, do NOT apply globally).

**Top-N control** — parameter `p_Top N Seasons`; applies to **Tile 8 only**; filters by `catalog_value_index`.

---

## 4. Dashboard layout

Fixed size **1366 × 768** (or 1280 × 800). One dashboard, neutral section labels.
```
Title (generic) + short neutral subtitle
Global filters: Show selector + Top N parameter

Row 1: Tile 1 KPI strip
Row 2: Tile 2 | Tile 3
Row 3: Tile 4 | Tile 5
Row 4: Tile 6 | Tile 7
Row 5: Tile 8 (full width)
Footer: IMDb attribution
```

**Neutral section labels** (no causal/interpretive phrasing, no show names):
```
Overview
Season-Level Rating Patterns
Episode Rating Distribution
Volatility and Durability
Catalog Value Ranking
```
Avoid: "Why did quality decline?", "What caused the drop-off?", "When did the show get bad?"

**Attribution footer (text object):**
> Source: IMDb Datasets, used under non-commercial terms — https://www.imdb.com/conditions. Not affiliated with or endorsed by IMDb.

---

# Tile 1 — KPI cards + show selector

**Purpose:** Clean dataset summary under All; show-specific KPIs after a show is selected.
**Required tables:** `dim_show_category`, `agg_season_kpis`, `shark_jump_results`.
**Do NOT use:** `episodes_filtered`, `durability_index` (durability is Tile 7).

**Build:** small text worksheets (one BAN each), assembled as a strip. Keep to the four spec cards plus a single combined overview line.

| Element | Field / calc | Under All | Under single show |
|---|---|---|---|
| Overview line | `COUNTD(title)` · `COUNTD(Season Key)` · `SUM(episode_count)` | "4 franchises · 87 seasons · 1,751 rated episodes" | selected show's counts |
| Series avg | `KPI Series Avg Display` | "Select a show" | `series_avg` (2 dp) |
| Best season | `KPI Best Season Display` *(sheet filter `season_rank_best = 1`)* | "Select a show" | rank-1 season |
| Shark-jump season | `KPI Shark Jump Display` | "Select a show" | shark-jump season / "No shark-jump detected" |

**Aggregation:** `episode_count` SUM; `series_avg` AVG/MIN; `shark_jump_season` MIN/AVG; best-season `season_num` MIN after the `season_rank_best = 1` filter.
**Labels:** plain ("Series Avg", "Best Season", "Shark-Jump Season"). No interpretive phrasing.
**Acceptance:** All → overview line populated, three gated cards say "Select a show". Simpsons → 802 episodes, 37 seasons, Shark-Jump = "S15", Series avg a single rating, Best season = the `season_rank_best = 1` season. No durability card. No hardcoded names.
**Common failure modes:** averaging shark-jump/best-season under All; `SUM(series_avg)`; adding durability; pulling `episodes_filtered` (grain risk).

---

# Tile 2 — Weighted rating by season + shark-jump marker

**Purpose:** Season-level quality trajectory with the detected shark-jump season marked.
**Required tables:** `dim_show_category`, `agg_season_kpis`, `shark_jump_results`.
**Do NOT use:** `episodes_filtered`, `durability_index`.

**Fields & shelves:**
- Columns: `season_num` → **Dimension, Continuous**
- Rows: `AVG(weighted_rating)` — and `Shark Jump Marker Rating` for a **dual axis**
- Color: `title`
- Marks: Layer 1 (`weighted_rating`) = **Line**; Layer 2 (`Shark Jump Marker Rating`) = **Circle** (larger, bordered); Label = `Shark Jump Label`. Right-click 2nd axis → **Dual Axis** → **Synchronize Axis**; hide secondary header.

**Aggregation:** `weighted_rating` AVG; `shark_jump_season` MIN/AVG (inside the marker calc).
**Filters:** global show selector.
**Tooltip:** Show `<title>` · Season S`<season_num>` · Weighted Rating `<AVG(weighted_rating)>` · `Shark Jump Label`. *Def:* "Weighted rating = vote-weighted average episode rating for the season." No "this caused…" phrasing. Do NOT use a manual annotation object.
**Format:** weighted_rating 1 dp.
**Acceptance:** marker on Simpsons **S15**, SpongeBob **S6**, Family Guy **S12**, Walking Dead **S8**; under All, four lines each with one marker; seasons don't collapse across shows.
**Common failure modes:** manual annotation at a fixed season; one merged line (missing `title`); using simple average instead of `weighted_rating`; episode fields on the sheet.

---

# Tile 3 — Rolling 3-season avg vs series avg

**Purpose:** Smoothed trend against the precomputed series baseline.
**Required tables:** `dim_show_category`, `agg_season_kpis`.
**Do NOT use:** `episodes_filtered`, `shark_jump_results`, `durability_index`.

> Tableau line *marks* have no dash style (only reference lines do), so do NOT specify "dashed data lines." Use one of the two builds below.

**Primary build — small multiples + dashed reference line (strongest, fully compliant):**
- Rows: `title` (trellis — one panel per show), then `AVG(Rolling 3S Avg Display)`
- Columns: `season_num` → **Dimension, Continuous**
- Marks: **Line** (the rolling line)
- Add `series_avg` as a **per-pane Reference Line**: Analytics → Reference Line → Per Cell → Value = `AVG(series_avg)` → Line = **dashed**. (A reference line displays the precomputed value; it does not recompute it.)
- Rolling null handling: **Format → Special Values → "Leave gap"** so the line starts at **Season 3** (no interpolation, no zero-fill).

**Simple fallback (if vertical space is tight):**
- Columns: `season_num` (Continuous); Rows: Measure Values = `AVG(Rolling 3S Avg Display)` + `AVG(series_avg)`; Color: **Measure Names**; Detail: `title`. Add a generic subtitle: "Clearest with a single show selected." (Under All, shows are distinguishable only on hover — acceptable.)

**Aggregation:** `Rolling 3S Avg Display` AVG; `series_avg` AVG/MIN.
**Filters:** global show selector.
**Tooltip (neutral):** "Rolling average shown from Season 3 onward; S1–S2 are expanding-window values in the data (documented in the README)."
**Format:** 1–2 dp.
**Acceptance:** rolling line starts at **S3** with an S1–S2 gap (not zero-filled, not non-null-filtered); pipeline data unchanged; `series_avg` not summed; the rolling line crosses below the baseline around each show's shark-jump season.
**Common failure modes:** "dashed data line" (unsupported); filtering non-null to fake the gap; recomputing/zero-filling rolling; summing `series_avg`; missing `title` (season collapse).

---

# Tile 4 — Season ranking (best → worst)

**Purpose:** Rank a show's seasons by precomputed quality rank.
**Required tables:** `dim_show_category`, `agg_season_kpis`.
**Do NOT use:** `episodes_filtered`, `shark_jump_results`, `durability_index`.

**Fields & shelves:**
- Rows: `title`, then `Season Label` (nested)
- Columns: `AVG(weighted_rating)`
- Sort: `Season Label` by **`season_rank_best` ascending** within each `title` (rank 1 on top) — **no Tableau `RANK()`**
- Marks: **Bar** (horizontal); Color optional (single color, or sequential by `weighted_rating`)

**Aggregation:** `weighted_rating` AVG; `season_rank_best` MIN; `episode_count` SUM if displayed.
**Filters:** global show selector.
**Tooltip:** Show · Season · Quality Rank (`MIN(season_rank_best)`) · Weighted Rating · Episodes. Use "Quality Rank" only with the note that it is rank by weighted rating.
**Format:** weighted_rating 1 dp.
**Acceptance:** the `season_rank_best = 1` season is at the top of each show's panel; order matches `season_rank_best` ascending; rank-1 bar = max `weighted_rating`; seasons don't merge across shows under All.
**Common failure modes:** Tableau `RANK()`; sorting by `catalog_value_index`; missing `title`; treating rank as cross-show.

---

# Tile 5 — Episode rating distribution (box plot) — `episodes_filtered` ONLY

**Purpose:** Episode-level rating spread by show and selected season.
**Required tables:** `episodes_filtered`, `dim_show_category`. **This is the only episode-grain tile.**
**Do NOT use:** `agg_season_kpis`, `shark_jump_results`, `durability_index`.

**Fields & shelves:**
- Columns: `title`
- Rows: `avg_rating` — set **Analysis → Aggregate Measures = OFF** (disaggregated)
- Detail: `episode_tconst` (one mark per episode)
- Marks: **Circle** (lower opacity) + drag **Box Plot** from the **Analytics** pane onto the view
- Filter: **`episodes_filtered.season_num`** (the Tile-5 season selector) → single value; applies to this sheet only

**Tooltip (episode fields ONLY):** Show · Season S`<season_num>` · Episode `<episode_num>` · Rating `<avg_rating>` · Votes `<num_votes>`.
**Format:** avg_rating 1 dp.
**Acceptance:** worksheet fields come only from `episodes_filtered` + `dim_show_category.title` — **no `agg_season_kpis` field in Rows / Columns / Marks / Filters / Tooltip / Detail**; under All, one box per show; season selector is from `episodes_filtered`; with no season filter, total marks = **1,751**. (1–2 episode seasons render a degenerate box — expected.)
**Common failure modes:** using `agg_season_kpis.season_num` as the filter; `weighted_rating` in the tooltip; pooling all shows into one Season-1 box (missing `title`); relating `episodes_filtered` to `agg_season_kpis`.

---

# Tile 6 — Rating stddev by season (volatility)

**Purpose:** Season-level rating consistency vs volatility.
**Required tables:** `dim_show_category`, `agg_season_kpis`.
**Do NOT use:** `episodes_filtered`, `shark_jump_results`, `durability_index`.

**Fields & shelves:**
- Columns: `season_num` → **Dimension, Continuous**
- Rows: `AVG(rating_stddev)`
- Color: `title`
- Marks: **Line** (one per show). *Cluttered-All fallback: `title` on Rows as small multiples, or Bar for single-show emphasis.*

**Aggregation:** `rating_stddev` AVG. **No Tableau `STDEV(avg_rating)`** — use the precomputed field.
**Filters:** global show selector.
**Tooltip:** Show · Season · Rating Stddev. *Def:* "How spread-out episode ratings were within the season (higher = less consistent)." Use "volatility" descriptively; imply no cause.
**Format:** stddev 2 dp.
**Acceptance:** one stddev per show-season; small values (~0.2–1.0); never negative; null/0 for 1-episode seasons (line gaps/dips — expected).
**Common failure modes:** `STDEV(avg_rating)`; missing `title`; episode fields on the sheet; interpreting volatility causally.

---

# Tile 7 — Durability index (cross-show bars)

**Purpose:** Which franchise sustains above-average smoothed quality longest.
**Required tables:** `dim_show_category`, `durability_index`.
**Do NOT use:** `agg_season_kpis`, `episodes_filtered`, `shark_jump_results`.

**Fields & shelves:**
- Rows: `title`
- Columns: `AVG(durability_index)` (or MIN) — **never SUM**
- Sort: by `durability_index` descending
- Marks: **Bar** (horizontal); Label = durability value
- Subtitle (generic): "Best viewed with the show selector set to All."

**Filters:** global show selector applies (collapses to one bar on single-show — acceptable per locked decision C).
**Tooltip:** Show · Durability Index. *Def:* "Count of seasons where the rolling 3-season average is at or above the show's overall average."
**Format:** integer.
**Acceptance:** under All, four bars sorted desc = The Simpsons **14**, Family Guy **12**, The Walking Dead **7**, SpongeBob **6**; selector filters Tile 7; single-show = one bar.
**Common failure modes:** exempting Tile 7 from the selector; summing durability; recomputing durability from rolling average; adding Tile 7 logic to Tile 1.

---

# Tile 8 — Top seasons by Catalog Value Index (Top-N action table)

**Purpose:** Licensing/promotion shortlist — highest catalog-value seasons.
**Required tables:** `dim_show_category`, `agg_season_kpis`.
**Do NOT use:** `episodes_filtered`, `shark_jump_results`, `durability_index`.

**Fields & shelves:**
- Rows: `title`, then `Season Label` (nested)
- Marks: **Text** (table) — `AVG(catalog_value_index)`, `SUM(episode_count)`. *(Optional context: `AVG(weighted_rating)`, `SUM(season_total_votes)`.)*
- Sort: by `AVG(catalog_value_index)` descending
- **Top-N filter:** apply to **`Season Key`** → Top → **By field `catalog_value_index`, Aggregation Average** → **Top N = `p_Top N Seasons`**. (A filter over a precomputed field — allowed. The `Season Key` target keeps show-seasons distinct so `season_num` does NOT collide across shows under All. **Never** `season_rank_best`; **never** Top-N on `season_num`/`Season Label` alone.)

**Aggregation:** `catalog_value_index` AVG; `episode_count` SUM; `weighted_rating` AVG; `season_total_votes` SUM (all single-valued at show-season grain). Keep `title` + `season_num` in the view.
**Filters:** global show selector; `p_Top N Seasons` (this tile only).
**Tooltip / columns:** Show · Season · Catalog Value Index · Weighted Rating · Season Total Votes · Episodes. *Def:* "Catalog Value Index = vote-weighted rating × log10(1 + season votes); higher = stronger licensing/promotion candidate." Title: "Top Seasons by Catalog Value Index." Avoid "top seasons to buy", "guaranteed value", "most profitable".
**Format:** catalog_value_index 1 dp; episode_count integer.
**Acceptance:** N=5 + single show → 5 rows = that show's five highest-CVI seasons with episode counts; changing N changes the count; Top-N is by `catalog_value_index` (not `season_rank_best`); under All, top-N show-seasons across the pool (high-vote shows dominate — by design).
**Common failure modes:** filtering by `season_rank_best`; Top-N on `season_num`/`Season Label` (collides under All); recomputing CVI; missing `title`; fabricated financial language.

---

## 5. Final pre-publish QA checklist

### Data model
- [ ] 5 CSVs connected as separate logical tables; no physical joins.
- [ ] `dim_show_category` is the hub; four spokes relate on `show_tconst`.
- [ ] No `episodes_filtered` ↔ `agg_season_kpis` relationship.
- [ ] **Fan-out smoke test passed** (802 / 326 / 446 / 177 both ways).

### Row counts (in Tableau vs CSVs)
- [ ] `dim_show_category` 4 · `agg_season_kpis` 87 · `episodes_filtered` 1751 · `shark_jump_results` 4 · `durability_index` 4.

### Global filter
- [ ] Show selector uses `dim_show_category.title`, applies to all worksheets, default All.
- [ ] Tile 7 is filtered by the selector.
- [ ] Tile 5 season selector uses `episodes_filtered.season_num`, scoped to Tile 5 only.

### Grain
- [ ] Every season-grain worksheet includes `title`.
- [ ] Tile 5 separates boxes by `title` and uses only episode fields + title.
- [ ] No worksheet mixes `episodes_filtered` with `agg_season_kpis` fields (incl. tooltips).

### Metric (no recompute)
- [ ] No Tableau calc recomputes `weighted_rating`, `rolling_3_season_avg`, `series_avg`, `rating_stddev`, `catalog_value_index`, `durability_index`, or `shark_jump_season`.
- [ ] Tile 8 Top-N uses `catalog_value_index` via the `Season Key` target.

### Aggregation
- [ ] `series_avg`, `shark_jump_season`, `durability_index` use AVG/MIN (never SUM).
- [ ] `episode_count` uses SUM where showing totals.

### Reference values
| Show | Seasons | Rated episodes | Shark-jump | Durability |
|---|---:|---:|---:|---:|
| Simpsons | 37 | 802 | S15 | 14 |
| SpongeBob | 16 | 326 | S6 | 6 |
| Family Guy | 23 | 446 | S12 | 12 |
| Walking Dead | 11 | 177 | S8 | 7 |

### Text / policy
- [ ] Generic dashboard title AND workbook filename.
- [ ] No hardcoded show names in titles/headers/annotations/calcs.
- [ ] No causal language, no fabricated financials, no ML/recommendation/scraping references.
- [ ] **IMDb attribution footer present on the dashboard** (README attribution handled in Phase 5).

---

## 6. Phase 4 vs Phase 5 deliverables

**End of Phase 4:** dashboard built, published to Tableau Public, local `.twb` backup saved.
**Do not commit:** `.twb`, full real IMDb-derived CSVs, raw IMDb TSVs.

**Phase 5:**
- Save the Tableau Public URL to `tableau_link.txt`.
- Add 2–3 dashboard screenshots to `screenshots/` (commit these).
- Update README: project summary, metric definitions, limitations, **IMDb attribution + conditions link (https://www.imdb.com/conditions)**, reproduction instructions (note the anaconda/env used), the **rolling-average S1/S2 expanding-window note**, and correlation-not-causation language.

---

## 7. v2 / polish backlog (NOT MVP unless approved)

- **Tile 7 highlight** — keep all durability bars visible and highlight the selected show (needs a parameter/highlight model instead of the global filter).
- **Tableau Story** — portfolio walkthrough (adds QA burden + causal/manual narrative drift risk).
- **Composite metrics** — Renewal Risk Index, Marginal Value Curve, revenue/dollar estimates (out of scope; data-policy risk).
- **Expanded show set** — current verified dataset is 4 franchises.

---

## Acceptance standard
Phase 4 is done only when: the dashboard is published to Tableau Public; all eight tiles are present; all controls behave correctly; row counts and reference values match; no fan-out/grain issue is visible; no metric is recomputed in Tableau; no hardcoded show names appear; the IMDb attribution footer is on the dashboard; and the local `.twb` backup is saved but not committed.
