-- 02_season_kpis.sql — Produces agg_season_kpis (1 row per show-season).
-- Reads from fact_episode (created by 01_schema.sql).

DROP VIEW IF EXISTS agg_season_kpis;

CREATE VIEW agg_season_kpis AS
WITH season_base AS (
    SELECT
        show_tconst,
        season_num,
        COUNT(*)                                                AS episode_count,
        SUM(avg_rating * num_votes) * 1.0
            / NULLIF(SUM(num_votes), 0)                         AS weighted_rating,
        AVG(avg_rating)                                         AS mean_rating,
        STDDEV_SAMP(avg_rating)                                 AS rating_stddev,
        SUM(num_votes)                                          AS season_total_votes
    FROM fact_episode
    GROUP BY show_tconst, season_num
),
with_series_avg AS (
    SELECT *,
        -- series_avg: unweighted average of season-level weighted ratings (approved override)
        AVG(weighted_rating) OVER (PARTITION BY show_tconst)    AS series_avg,
        AVG(weighted_rating) OVER (
            PARTITION BY show_tconst ORDER BY season_num
            ROWS BETWEEN 2 PRECEDING AND CURRENT ROW
        )                                                       AS rolling_3_season_avg
    FROM season_base
),
with_pct AS (
    SELECT s.*,
        (SELECT COUNT(*) FROM fact_episode e2
         WHERE e2.show_tconst = s.show_tconst
           AND e2.season_num  = s.season_num
           AND e2.avg_rating >= s.series_avg) * 100.0
        / NULLIF(s.episode_count, 0)                            AS pct_high_rated
    FROM with_series_avg s
),
ranked AS (
    SELECT *,
        RANK() OVER (PARTITION BY show_tconst
                     ORDER BY weighted_rating DESC)             AS season_rank_best,
        weighted_rating * LOG10(1 + season_total_votes)         AS catalog_value_index
    FROM with_pct
)
SELECT
    show_tconst,
    season_num,
    episode_count,
    season_total_votes,
    weighted_rating,
    mean_rating,
    rating_stddev,
    pct_high_rated,
    series_avg,
    rolling_3_season_avg,
    season_rank_best,
    catalog_value_index
FROM ranked
ORDER BY show_tconst, season_num;
