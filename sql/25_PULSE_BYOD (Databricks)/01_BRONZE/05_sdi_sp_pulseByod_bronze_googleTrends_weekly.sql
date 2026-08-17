/* =================================================================================================
FILE:         05_sdi_sp_pulseByod_bronze_googleTrends_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_bronze_googleTrends_weekly

SOURCE:
  prd_dbi_analytics.improvado.sdi_seo_googletrends_byod_weekly_tmo

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_googleTrends_weekly

PURPOSE:
  Source-close Bronze view for Google Trends BYOD weekly search interest data.
  Google Trends provides a market-level relative interest index (0-100) for the
  BYOD topic overall, plus the top 5 keywords driving that interest each week.
  This is not brand or site specific — it reflects overall market search behavior.
  This table is BYOD-specific by pipeline design — no topic filtering needed.
  Deduplicates the weekly snapshot and preserves all raw fields as-is.

BUSINESS GRAIN:
  One row per:
    date_yyyymmdd
  (Google Trends is BYOD-specific with one row per week — no account or asset dimension)

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

KEY MODELING NOTES:
  - account_id in the source is the pipeline name string "sdi_seo_googletrends_byod_weekly"
    not a real account ID — preserved as-is for lineage but excluded from dedup grain
  - account_name is empty/null in the source — preserved as-is
  - byod_index: relative interest score 0-100 where 100 = peak interest in the date range
    Primary KPI for this source — reliable across full history (2025-05-18 onward)
  - top_kw_1 through top_kw_5: text of top 5 keywords driving BYOD interest that week
  - kw1_interest through kw5_interest: relative interest score 0-100 per keyword
  - kw1_change through kw5_change: week-over-week change in keyword interest
    (e.g. -0.03 = interest decreased 3% vs prior week)
  - IMPORTANT: keyword columns only populated from 2026-05-09 onward
    Earlier rows have empty strings and 0.0 values — known pipeline backfill issue
    This is not a Bronze concern — handled in Silver
  - date_yyyymmdd reflects the Sunday start of the ISO week as supplied by source
  - event_date_sun is the parsed DATE version of date_yyyymmdd for downstream arithmetic
  - Week-end Saturday conversion (week_sun_to_sat) is applied in Silver:
      DATE_ADD(event_date_sun, 6)
  - Unpivoting of keyword columns from wide (kw1-kw5) to long format applied in Silver
    to support the Top N Keywords visualization in the dashboard

DOWNSTREAM:
  Silver : vw_sdi_pulseByod_silver_googleTrends_weekly
================================================================================================= */

/* =================================================================================================
STATUS: COMMENTED OUT — pending confirmation of this source's physical table location in the
Databricks Improvado catalog (prd_dbi_analytics.improvado). The SQL below is fully translated and
ready to run once that location is confirmed — just remove the surrounding block-comment markers.
Not referenced by any Silver or Gold object in this pipeline while commented out.
================================================================================================= */
/*
CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_pulseByod_bronze_googleTrends_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_pulseByod_bronze_googleTrends_weekly
  USING DELTA
  AS

WITH ranked AS (
    SELECT
        -- Pipeline identifier
        -- NOTE: account_id is the pipeline name "sdi_seo_googletrends_byod_weekly"
        -- not a true account ID — preserved for lineage, excluded from dedup grain
        TRY_CAST(raw.account_id AS STRING)    AS account_id,
        TRY_CAST(raw.account_name AS STRING)    AS account_name,

        -- Date fields
        -- date_yyyymmdd : raw string date key in YYYYMMDD format (Sunday week start)
        -- event_date_sun: parsed DATE type for downstream arithmetic and Silver week-end conversion
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        TO_DATE(CAST(raw.date_yyyymmdd AS STRING), 'yyyyMMdd')         AS event_date_sun,

        -- Market-level BYOD interest index
        -- byod_index: relative search interest 0-100 (100 = peak in date range)
        -- Reliable across full history — primary KPI for this source
        TRY_CAST(raw.byod_index AS DOUBLE)                            AS byod_index,

        -- Top 5 trending keywords driving BYOD interest this week
        -- NOTE: only populated from 2026-05-09 onward (pipeline backfill issue)
        -- Earlier rows have empty strings and 0.0 for all keyword fields
        -- Unpivoting to long format applied in Silver for dashboard Top N Keywords

        -- Keyword rank 1 (highest interest this week)
        TRY_CAST(raw.top_kw_1 AS STRING)                           AS top_kw_1,
        TRY_CAST(raw.kw1_interest AS DOUBLE)                          AS kw1_interest,
        TRY_CAST(raw.kw1_change AS DOUBLE)                          AS kw1_change,

        -- Keyword rank 2
        TRY_CAST(raw.top_kw_2 AS STRING)                           AS top_kw_2,
        TRY_CAST(raw.kw2_interest AS DOUBLE)                          AS kw2_interest,
        TRY_CAST(raw.kw2_change AS DOUBLE)                          AS kw2_change,

        -- Keyword rank 3
        TRY_CAST(raw.top_kw_3 AS STRING)                           AS top_kw_3,
        TRY_CAST(raw.kw3_interest AS DOUBLE)                          AS kw3_interest,
        TRY_CAST(raw.kw3_change AS DOUBLE)                          AS kw3_change,

        -- Keyword rank 4
        TRY_CAST(raw.top_kw_4 AS STRING)                           AS top_kw_4,
        TRY_CAST(raw.kw4_interest AS DOUBLE)                          AS kw4_interest,
        TRY_CAST(raw.kw4_change AS DOUBLE)                          AS kw4_change,

        -- Keyword rank 5
        TRY_CAST(raw.top_kw_5 AS STRING)                           AS top_kw_5,
        TRY_CAST(raw.kw5_interest AS DOUBLE)                          AS kw5_interest,
        TRY_CAST(raw.kw5_change AS DOUBLE)                          AS kw5_change,

        -- Audit fields (preserved for data lineage and dedup ordering)
        TRY_CAST(raw.__insert_date AS BIGINT)                           AS insert_date,
        CAST(raw.File_Load_datetime AS TIMESTAMP)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,

        -- Dedup: latest row per date_yyyymmdd
        -- One row per week expected — dedup guards against pipeline reruns
        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                CAST(raw.File_Load_datetime AS TIMESTAMP)     DESC,
                raw.Filename                          DESC,
                TRY_CAST(raw.__insert_date AS BIGINT) DESC
        ) AS rn

    FROM prd_dbi_analytics.improvado.sdi_seo_googletrends_byod_weekly_tmo raw

    -- Exclude rows missing the date key to prevent dedup grain pollution
    WHERE raw.date_yyyymmdd IS NOT NULL
)

SELECT
    account_id,
    account_name,
    date_yyyymmdd,
    event_date_sun,
    byod_index,
    top_kw_1,
    kw1_interest,
    kw1_change,
    top_kw_2,
    kw2_interest,
    kw2_change,
    top_kw_3,
    kw3_interest,
    kw3_change,
    top_kw_4,
    kw4_interest,
    kw4_change,
    top_kw_5,
    kw5_interest,
    kw5_change,
    insert_date,
    file_load_datetime,
    filename
FROM ranked
WHERE rn = 1
;

END;
*/