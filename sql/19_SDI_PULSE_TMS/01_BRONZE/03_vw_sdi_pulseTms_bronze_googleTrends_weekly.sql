/* =================================================================================================
FILE:         03_vw_sdi_pulseTms_bronze_googleTrends_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_googleTrends_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_googletrends_byod_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_googleTrends_weekly

PURPOSE:
  Source-close Bronze view for Google Trends BYOD weekly search interest data.
  Identical logic to vw_sdi_pulseByod_bronze_googleTrends_weekly.
  Renamed pulseTms to support the full TMS pipeline.
  One row per week. Deduplicates the weekly snapshot.

BUSINESS GRAIN:
  One row per date_yyyymmdd

DOWNSTREAM:
  Silver : vw_sdi_pulseTms_silver_googleTrends_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_googleTrends_weekly`
AS

WITH ranked AS (
    SELECT
        SAFE_CAST(raw.account_id   AS STRING)    AS account_id,
        SAFE_CAST(raw.account_name AS STRING)    AS account_name,
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date_sun,
        SAFE_CAST(raw.byod_index AS FLOAT64)                            AS byod_index,
        SAFE_CAST(raw.top_kw_1     AS STRING)                           AS top_kw_1,
        SAFE_CAST(raw.kw1_interest AS FLOAT64)                          AS kw1_interest,
        SAFE_CAST(raw.kw1_change   AS FLOAT64)                          AS kw1_change,
        SAFE_CAST(raw.top_kw_2     AS STRING)                           AS top_kw_2,
        SAFE_CAST(raw.kw2_interest AS FLOAT64)                          AS kw2_interest,
        SAFE_CAST(raw.kw2_change   AS FLOAT64)                          AS kw2_change,
        SAFE_CAST(raw.top_kw_3     AS STRING)                           AS top_kw_3,
        SAFE_CAST(raw.kw3_interest AS FLOAT64)                          AS kw3_interest,
        SAFE_CAST(raw.kw3_change   AS FLOAT64)                          AS kw3_change,
        SAFE_CAST(raw.top_kw_4     AS STRING)                           AS top_kw_4,
        SAFE_CAST(raw.kw4_interest AS FLOAT64)                          AS kw4_interest,
        SAFE_CAST(raw.kw4_change   AS FLOAT64)                          AS kw4_change,
        SAFE_CAST(raw.top_kw_5     AS STRING)                           AS top_kw_5,
        SAFE_CAST(raw.kw5_interest AS FLOAT64)                          AS kw5_interest,
        SAFE_CAST(raw.kw5_change   AS FLOAT64)                          AS kw5_change,
        SAFE_CAST(raw.__insert_date AS INT64)                           AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,
        ROW_NUMBER() OVER (
            PARTITION BY CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime)     DESC,
                raw.Filename                          DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_googletrends_byod_weekly_tmo` raw
    WHERE raw.date_yyyymmdd IS NOT NULL
)

SELECT
    account_id, account_name, date_yyyymmdd, event_date_sun,
    byod_index,
    top_kw_1, kw1_interest, kw1_change,
    top_kw_2, kw2_interest, kw2_change,
    top_kw_3, kw3_interest, kw3_change,
    top_kw_4, kw4_interest, kw4_change,
    top_kw_5, kw5_interest, kw5_change,
    insert_date, file_load_datetime, filename
FROM ranked
WHERE rn = 1
;