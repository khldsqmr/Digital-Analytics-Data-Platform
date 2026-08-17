/* =================================================================================================
FILE:         sdi_sp_dashboardPulseByod_bronze_sa360Adgroup_daily.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseByod_bronze_sa360Adgroup_daily

SOURCE:
  prd_dbi_analytics.improvado.sdi_ps_sa360_adgroup_daily_tmo

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_sa360Adgroup_daily

PURPOSE:
  Source-close Bronze view for SA360 paid search performance at ad group level.
  SA360 captures daily paid search metrics (impressions, clicks, cost, conversions)
  for T-Mobile paid search campaigns across Google and Bing at the ad group grain.
  This source covers all T-Mobile paid search ad groups across all campaigns.
  BYOD/BYOP ad group filtering is applied in Silver.
  Brand vs nonbrand classification via ad_group_name regex is applied in Silver.
  Deduplicates the daily snapshot and preserves relevant raw fields as-is.

BUSINESS GRAIN:
  One row per:
    account_id + ad_group_id + date_yyyymmdd

DEDUPE LOGIC:
  Latest row per grain ordered by:
    File_Load_datetime DESC
    Filename DESC
    __insert_date DESC

KEY MODELING NOTES:
  - All ad groups preserved — no BYOD/BYOP filtering applied here (pushed to Silver)
  - No brand/nonbrand classification applied here (pushed to Silver)
    Silver uses: REGEXP_CONTAINS(LOWER(ad_group_name), r'(^|[^a-z])brand([^a-z]|$)')
    consistent with existing Total Search Dashboard classification logic
  - cost_micros: raw native SA360 unit preserved for precision
  - cost: derived convenience field in USD (cost_micros / 1000000)
  - Only BYOD-relevant conversion columns retained:
      postpaid_prospect_web_order : primary KPI — web orders from postpaid prospects
      postpaid_cart_start         : mid-funnel signal scoped to postpaid prospects
      postpaid_pspv               : page-specific visit signal for postpaid
      cart_start                  : all cart starts regardless of prospect scope
  - campaign_id and campaign_name retained for context and potential future use
  - event_date is the parsed DATE version of date_yyyymmdd for week rollup in Silver
  - Week rollup to Saturday (week_sun_to_sat) is applied in Silver:
      DATE_ADD(DATE_TRUNC(event_date, WEEK(SUNDAY)), INTERVAL 6 DAY)

DOWNSTREAM:
  Silver : vw_sdi_pulseByod_silver_sa360_weekly
================================================================================================= */

/* =================================================================================================
STATUS: COMMENTED OUT — pending confirmation of this source's physical table location in the
Databricks Improvado catalog (prd_dbi_analytics.improvado). The SQL below is fully translated and
ready to run once that location is confirmed — just remove the surrounding block-comment markers.
Not referenced by any Silver or Gold object in this pipeline while commented out.
================================================================================================= */
/*
CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_dashboardPulseByod_bronze_sa360Adgroup_daily()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseByod_bronze_sa360Adgroup_daily
  USING DELTA
  AS

WITH ranked AS (
    SELECT
        -- Primary keys
        TRY_CAST(raw.account_id AS STRING)   AS account_id,
        TRY_CAST(raw.account_name AS STRING)   AS account_name,
        TRY_CAST(raw.ad_group_id AS STRING)   AS ad_group_id,
        TRY_CAST(raw.ad_group_name AS STRING)   AS ad_group_name,

        -- Campaign context (retained for reference and potential future use)
        TRY_CAST(raw.campaign_id AS STRING)   AS campaign_id,
        TRY_CAST(raw.campaign_name AS STRING)   AS campaign_name,

        -- Date fields
        -- date_yyyymmdd: raw string date key in YYYYMMDD format (daily grain)
        -- event_date   : parsed DATE type for downstream week rollup in Silver
        CAST(raw.date_yyyymmdd AS STRING)                               AS date_yyyymmdd,
        TO_DATE(CAST(raw.date_yyyymmdd AS STRING), 'yyyyMMdd')         AS event_date,

        -- Core performance metrics
        TRY_CAST(raw.impressions AS DOUBLE)                           AS impressions,
        TRY_CAST(raw.clicks AS DOUBLE)                           AS clicks,

        -- Cost
        -- cost_micros: raw native SA360 unit (preserved for precision)
        -- cost       : derived convenience field in USD (cost_micros / 1000000)
        TRY_CAST(raw.cost_micros AS DOUBLE)                           AS cost_micros,
        TRY_CAST(raw.cost_micros AS DOUBLE) / 1000000                 AS cost,

        -- Conversion metrics relevant to BYOD postpaid prospect funnel
        -- postpaid_prospect_web_order: primary conversion KPI
        -- postpaid_cart_start        : mid-funnel signal scoped to postpaid prospects
        -- postpaid_pspv              : page-specific visit signal for postpaid
        -- cart_start                 : all cart starts regardless of prospect scope
        TRY_CAST(raw.postpaid__prospect__web__order AS DOUBLE)        AS postpaid_prospect_web_order,
        TRY_CAST(raw.postpaid__cart__start_ AS DOUBLE)        AS postpaid_cart_start,
        TRY_CAST(raw.postpaid_pspv_ AS DOUBLE)        AS postpaid_pspv,
        TRY_CAST(raw.cart__start_ AS DOUBLE)        AS cart_start,

        -- Audit fields (preserved for data lineage and dedup ordering)
        TRY_CAST(raw.__insert_date AS BIGINT)                           AS insert_date,
        CAST(raw.File_Load_datetime AS TIMESTAMP)                               AS file_load_datetime,
        raw.Filename                                                    AS filename,

        -- Dedup: latest row per account_id + ad_group_id + date_yyyymmdd
        ROW_NUMBER() OVER (
            PARTITION BY
                TRY_CAST(raw.account_id AS STRING),
                TRY_CAST(raw.ad_group_id AS STRING),
                CAST(raw.date_yyyymmdd    AS STRING)
            ORDER BY
                CAST(raw.File_Load_datetime AS TIMESTAMP)     DESC,
                raw.Filename                          DESC,
                TRY_CAST(raw.__insert_date AS BIGINT) DESC
        ) AS rn

    FROM prd_dbi_analytics.improvado.sdi_ps_sa360_adgroup_daily_tmo raw

    -- Exclude rows missing primary key fields to prevent dedup grain pollution
    WHERE raw.account_id    IS NOT NULL
      AND raw.ad_group_id   IS NOT NULL
      AND raw.date_yyyymmdd IS NOT NULL
)

SELECT
    account_id,
    account_name,
    ad_group_id,
    ad_group_name,
    campaign_id,
    campaign_name,
    date_yyyymmdd,
    event_date,
    impressions,
    clicks,
    cost_micros,
    cost,
    postpaid_prospect_web_order,
    postpaid_cart_start,
    postpaid_pspv,
    cart_start,
    insert_date,
    file_load_datetime,
    filename
FROM ranked
WHERE rn = 1
;

END;
*/