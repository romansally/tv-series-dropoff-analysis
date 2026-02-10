-- 03_shark_jump.sql — Detects shark-jump season per CLAUDE.md algorithm.
-- Reads from fact_episode and dim_show (created by 01_schema.sql).

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
flagged AS (
    SELECT *,
        CASE WHEN rolling_3_season_avg < series_avg THEN 1 ELSE 0 END AS below_avg_flag,
        LAG(CASE WHEN rolling_3_season_avg < series_avg THEN 1 ELSE 0 END)
            OVER (PARTITION BY show_tconst ORDER BY season_num) AS prev_below
    FROM with_roll
),
detected AS (
    SELECT
        show_tconst,
        MIN(season_num - 1) AS shark_jump_season
    FROM flagged
    WHERE below_avg_flag = 1 AND prev_below = 1
    GROUP BY show_tconst
)
SELECT
    d.show_tconst,
    det.shark_jump_season
FROM dim_show d
LEFT JOIN detected det ON d.show_tconst = det.show_tconst
ORDER BY d.show_tconst;
