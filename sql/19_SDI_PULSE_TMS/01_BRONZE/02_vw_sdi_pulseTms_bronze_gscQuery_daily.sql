/* =================================================================================================
FILE:         02_vw_sdi_pulseTms_bronze_gscQuery_daily.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_gscQuery_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_gscQuery_daily

PURPOSE:
  Source-close Bronze view for Google Search Console query-level organic search data.
  Identical logic to vw_sdi_pulseByod_bronze_gscQuery_daily.
  Renamed pulseTms to support the full TMS pipeline (BYOD + Postpaid + HSI).
  BYOD query filtering is applied in Silver.
  Site URL filtering to sc-domain:t-mobile.com is applied in Silver.
  Brand vs nonbrand classification via query regex is applied in Silver.

BUSINESS GRAIN:
  One row per:
    account_id + site_url + page + query + search_type + date_yyyymmdd

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC, Filename DESC, __insert_date DESC,
    impressions DESC, clicks DESC

DOWNSTREAM:
  Silver : vw_sdi_pulseTms_silver_gsc_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_gscQuery_daily`
AS

WITH ranked AS (
    SELECT
        SAFE_CAST(raw.account_id   AS STRING)    AS account_id,
        SAFE_CAST(raw.account_name AS STRING)    AS account_name,
        SAFE_CAST(raw.site_url     AS STRING)    AS site_url,
        SAFE_CAST(raw.page         AS STRING)    AS page,
        SAFE_CAST(raw.query        AS STRING)    AS query,
        SAFE_CAST(raw.search_type  AS STRING)    AS search_type,
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date,
        SAFE_CAST(raw.impressions   AS FLOAT64)                         AS impressions,
        SAFE_CAST(raw.clicks        AS FLOAT64)                         AS clicks,
        SAFE_CAST(raw.position      AS FLOAT64)                         AS position,
        SAFE_CAST(raw.sum_position  AS FLOAT64)                         AS sum_position,
        SAFE_CAST(raw.__insert_date    AS INT64)                        AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,
        ROW_NUMBER() OVER (
            PARTITION BY
                SAFE_CAST(raw.account_id  AS STRING),
                SAFE_CAST(raw.site_url    AS STRING),
                SAFE_CAST(raw.page        AS STRING),
                SAFE_CAST(raw.query       AS STRING),
                SAFE_CAST(raw.search_type AS STRING),
                CAST(raw.date_yyyymmdd    AS STRING)
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime)       DESC,
                raw.Filename                            DESC,
                SAFE_CAST(raw.__insert_date  AS INT64)  DESC,
                SAFE_CAST(raw.impressions    AS FLOAT64) DESC,
                SAFE_CAST(raw.clicks         AS FLOAT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo` raw
    WHERE raw.account_id    IS NOT NULL
      AND raw.site_url      IS NOT NULL
      AND raw.query         IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
)

SELECT
    account_id, account_name, site_url, page, query, search_type,
    date_yyyymmdd, event_date,
    impressions, clicks, position, sum_position,
    insert_date, file_load_datetime, filename
FROM ranked
WHERE rn = 1
;