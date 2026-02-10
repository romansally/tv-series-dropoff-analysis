-- 04_durability.sql — Computes Durability Index per show.
-- Reads from fact_episode and dim_show (created by 01_schema.sql).
-- Durability Index = count of seasons where rolling_3_season_avg >= series_avg.

WITH season_kpis AS (
    SELECT
        show_tconst,
        season_num,
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
        -- series_avg: unweighted average of season-level weighted ratings (approved override)
        AVG(weighted_rating) OVER (PARTITION BY show_tconst) AS series_avg
    FROM season_kpis
),
durable_seasons AS (
    SELECT
        show_tconst,
        COUNT(*) AS durability_index
    FROM with_roll
    WHERE rolling_3_season_avg >= series_avg
    GROUP BY show_tconst
)
SELECT
    d.show_tconst,
    COALESCE(ds.durability_index, 0) AS durability_index
FROM dim_show d
LEFT JOIN durable_seasons ds ON d.show_tconst = ds.show_tconst
ORDER BY d.show_tconst;
