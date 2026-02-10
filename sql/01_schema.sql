-- 01_schema.sql — Create dim_show and fact_episode tables from CSV inputs.
-- Placeholder tokens (__EPISODES_CSV__, __SHOWS_CSV__, __CATEGORY_CSV__)
-- are replaced at runtime by pipeline/02_run_sql.py.

DROP TABLE IF EXISTS fact_episode;
DROP TABLE IF EXISTS dim_show;

-- ── dim_show: 1 row per show ────────────────────────────────────────
CREATE TABLE dim_show AS
SELECT
    s.show_tconst,
    s.primary_title,
    CAST(s.start_year AS INTEGER) AS start_year,
    CAST(s.end_year AS INTEGER)   AS end_year,
    s.genres,
    c.category
FROM read_csv_auto('__SHOWS_CSV__', header = true, all_varchar = true) s
LEFT JOIN read_csv_auto('__CATEGORY_CSV__', header = true, all_varchar = true) c
    ON s.show_tconst = c.show_tconst;

-- ── fact_episode: 1 row per episode ─────────────────────────────────
CREATE TABLE fact_episode AS
SELECT
    episode_tconst,
    show_tconst,
    CAST(season_num  AS INTEGER) AS season_num,
    CAST(episode_num AS INTEGER) AS episode_num,
    CAST(avg_rating  AS DOUBLE)  AS avg_rating,
    CAST(num_votes   AS BIGINT)  AS num_votes
FROM read_csv_auto('__EPISODES_CSV__', header = true, all_varchar = true);
