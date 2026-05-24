/* =================================================================================================
FILE:         04_vw_sdi_pulseByod_bronze_gscQuery_daily.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_bronze_gscQuery_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_gscQuery_daily

PURPOSE:
  Source-close Bronze view for Google Search Console query-level organic search data.
  GSC captures daily organic search impressions, clicks, and average position
  by query, page, and search type for T-Mobile's organic search performance.
  This source covers all queries across all T-Mobile domains and topics.
  BYOD query filtering is applied in Silver.
  Site URL filtering to sc-domain:t-mobile.com is applied in Silver.
  Brand vs nonbrand classification via query regex is applied in Silver.
  Deduplicates the daily snapshot and preserves all raw fields as-is.

BUSINESS GRAIN:
  One row per:
    account_id + site_url + page + query + search_type + date_yyyymmdd

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC
    impressions DESC  <- metric tiebreaker for rows with identical audit metadata
    clicks DESC       <- metric tiebreaker for rows with identical audit metadata

KEY MODELING NOTES:
  - All queries preserved as raw source text — no BYOD filtering applied here (pushed to Silver)
  - All site_url values preserved — no site filtering applied here (pushed to Silver)
    Silver filters to: UPPER(TRIM(site_url)) = 'SC-DOMAIN:T-MOBILE.COM'
  - No brand/nonbrand classification applied here (pushed to Silver)
    Silver applies the full T-Mobile brand regex consistent with vw_sdi_tsd_silver_gsc_daily:
    REGEXP_CONTAINS(LOWER(query), r'(^t$|tmobile|t-mobile|t mobile|tmo\b|magenta|metro|sprint|...)')
  - Raw query text preserved exactly as received — no normalization in Bronze
    Normalization (LOWER, TRIM) applied in Silver for classification only
  - Metric-based tiebreakers (impressions DESC, clicks DESC) included in dedup ORDER BY
    because source can contain conflicting duplicate rows with identical audit metadata
  - position   : average search result position for this query on this date
  - sum_position: raw sum of positions before averaging — preserved for reconciliation
  - event_date is the parsed DATE version of date_yyyymmdd for week rollup in Silver
  - Week rollup to Saturday (week_sun_to_sat) is applied in Silver:
      DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)

DOWNSTREAM:
  Silver : vw_sdi_pulseByod_silver_gsc_weekly
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseByod_bronze_gscQuery_daily`
AS

WITH ranked AS (
    SELECT
        -- Primary keys
        SAFE_CAST(raw.account_id   AS STRING)    AS account_id,
        SAFE_CAST(raw.account_name AS STRING)    AS account_name,
        SAFE_CAST(raw.site_url     AS STRING)    AS site_url,
        SAFE_CAST(raw.page         AS STRING)    AS page,

        -- Raw query text preserved exactly as received from source
        -- Normalization (LOWER, TRIM) and brand classification applied in Silver
        SAFE_CAST(raw.query        AS STRING)    AS query,
        SAFE_CAST(raw.search_type  AS STRING)    AS search_type,

        -- Date fields
        -- date_yyyymmdd: raw string date key in YYYYMMDD format (daily grain)
        -- event_date   : parsed DATE type for downstream week rollup in Silver
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING))         AS event_date,

        -- Core performance metrics
        -- impressions : times T-Mobile appeared in Google search results for this query
        -- clicks      : times users clicked through to T-Mobile from this query
        -- position    : average ranking position in search results for this query
        -- sum_position: raw sum of positions (preserved for reconciliation)
        SAFE_CAST(raw.impressions   AS FLOAT64)                         AS impressions,
        SAFE_CAST(raw.clicks        AS FLOAT64)                         AS clicks,
        SAFE_CAST(raw.position      AS FLOAT64)                         AS position,
        SAFE_CAST(raw.sum_position  AS FLOAT64)                         AS sum_position,

        -- Audit fields (preserved for data lineage and dedup ordering)
        SAFE_CAST(raw.__insert_date    AS INT64)                        AS insert_date,
        TIMESTAMP(raw.File_Load_datetime)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,

        -- Dedup: latest row per account_id + site_url + page + query + search_type + date_yyyymmdd
        -- Metric tiebreakers included because source can contain conflicting
        -- duplicate rows with identical audit metadata
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

    -- Exclude rows missing primary key fields to prevent dedup grain pollution
    WHERE raw.account_id    IS NOT NULL
      AND raw.site_url      IS NOT NULL
      AND raw.query         IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
)

SELECT
    account_id,
    account_name,
    site_url,
    page,
    query,
    search_type,
    date_yyyymmdd,
    event_date,
    impressions,
    clicks,
    position,
    sum_position,
    insert_date,
    file_load_datetime,
    filename
FROM ranked
WHERE rn = 1
;