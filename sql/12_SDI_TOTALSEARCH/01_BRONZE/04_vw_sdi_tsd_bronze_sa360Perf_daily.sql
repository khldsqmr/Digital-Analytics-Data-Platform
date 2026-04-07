/* =================================================================================================
FILE: 04_vw_sdi_tsd_bronze_sa360Perf_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_sa360Perf_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily

PURPOSE:
  Canonical Bronze SA360 performance daily view for the Total Search Dashboard.
  This view deduplicates the SA360 campaign daily performance snapshot and preserves
  the campaign-level daily metrics needed downstream.

BUSINESS GRAIN:
  One row per:
      account_id
      campaign_id
      event_date

DEDUPE LOGIC:
  Latest row per:
      account_id + campaign_id + date_yyyymmdd
  ordered by:
      File_Load_datetime DESC,
      Filename DESC,
      __insert_date DESC

KEY MODELING NOTES:
  - This is a source-close Bronze layer object
  - No brand / nonbrand logic is applied here
  - No joins to entity/settings metadata are applied here

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_sa360Perf_daily`
AS

WITH ranked AS (
    SELECT
        raw.account_id,
        raw.campaign_id,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,

        raw.account_name,
        raw.customer_id,
        raw.customer_name,
        raw.resource_name,
        raw.segments_date,
        raw.client_manager_id,
        raw.client_manager_name,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        DATETIME(raw.File_Load_datetime) AS file_load_datetime,
        raw.Filename AS filename,

        SAFE_CAST(raw.clicks AS FLOAT64) AS clicks,
        SAFE_CAST(raw.impressions AS FLOAT64) AS impressions,
        SAFE_CAST(raw.cost_micros AS FLOAT64) AS cost_micros,
        SAFE_CAST(raw.cost_micros AS FLOAT64) / 1000000 AS cost,

        SAFE_CAST(raw.cart__start_ AS FLOAT64) AS cart_start,
        SAFE_CAST(raw.postpaid__cart__start_ AS FLOAT64) AS postpaid_cart_start,
        SAFE_CAST(raw.postpaid_pspv_ AS FLOAT64) AS postpaid_pspv,

        ROW_NUMBER() OVER (
            PARTITION BY raw.account_id, raw.campaign_id, CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                DATETIME(raw.File_Load_datetime) DESC,
                raw.Filename DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo` raw
)

SELECT
    account_id,
    campaign_id,
    date_yyyymmdd,
    event_date,
    account_name,
    customer_id,
    customer_name,
    resource_name,
    segments_date,
    client_manager_id,
    client_manager_name,
    insert_date,
    file_load_datetime,
    filename,
    clicks,
    impressions,
    cost_micros,
    cost,
    cart_start,
    postpaid_cart_start,
    postpaid_pspv
FROM ranked
WHERE rn = 1;