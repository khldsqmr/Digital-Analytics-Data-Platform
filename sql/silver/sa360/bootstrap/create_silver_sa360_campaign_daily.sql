/*
===============================================================================
SILVER | PAID SEARCH | CAMPAIGN DAILY
===============================================================================

PURPOSE:
  Clean, enriched, business-ready daily campaign fact table.

GRAIN:
  account_id + campaign_id + date

SOURCE:
  sdi_bronze_sa360_campaign_daily
  sdi_bronze_sa360_campaign_entity

NOTES:
  - Uses latest entity snapshot per campaign
  - Uses cleaned Bronze metric names
  - Excludes cost_micros
===============================================================================
*/

CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_paid_search_campaign_daily`
PARTITION BY date
CLUSTER BY account_id, campaign_id, campaign_type
AS

-- ============================================================
-- STEP 1: Get Latest Campaign Metadata
-- ============================================================

WITH latest_entity AS (
  SELECT *
  FROM (
    SELECT
      *,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, campaign_id
        ORDER BY file_load_datetime DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  )
  WHERE rn = 1
)

-- ============================================================
-- STEP 2: Join Daily Metrics with Latest Entity
-- ============================================================

SELECT

  -- ============================================================
  -- GRAIN
  -- ============================================================

  d.account_id,
  d.account_name,
  d.campaign_id,
  e.name AS campaign_name,
  d.date,

  -- ============================================================
  -- CALENDAR DIMENSIONS
  -- ============================================================

  EXTRACT(YEAR FROM d.date) AS year,
  EXTRACT(MONTH FROM d.date) AS month,
  FORMAT_DATE('%Y-Q%Q', d.date) AS quarter,
  EXTRACT(WEEK FROM d.date) AS week,
  FORMAT_DATE('%A', d.date) AS day_of_week,

  -- ============================================================
  -- CAMPAIGN CLASSIFICATION
  -- ============================================================

  CASE
    WHEN LOWER(e.name) LIKE '%brand%' THEN 'Brand'
    WHEN LOWER(e.name) LIKE '%generic%' THEN 'Generic'
    WHEN LOWER(e.name) LIKE '%shopping%' THEN 'Shopping'
    WHEN LOWER(e.name) LIKE '%shop%' THEN 'Shopping'
    WHEN LOWER(e.name) LIKE '%pmax%' THEN 'PMax'
    WHEN LOWER(e.name) LIKE '%demandgen%' THEN 'DemandGen'
    ELSE 'Unclassified'
  END AS campaign_type,

  e.advertising_channel_type,
  e.advertising_channel_sub_type,
  e.bidding_strategy_type,
  e.status AS campaign_status,
  e.serving_status,

  -- ============================================================
  -- CORE PERFORMANCE
  -- ============================================================

  d.impressions,
  d.clicks,
  d.cost,
  d.all_conversions,

  -- ============================================================
  -- POSTPAID
  -- ============================================================

  d.postpaid_cart_start,
  d.postpaid_pspv,
  d.aal,
  d.add_a_line,

  -- ============================================================
  -- HINT
  -- ============================================================

  d.hint_ec,
  d.hint_sec,
  d.hint_web_orders,
  d.hint_invoca_calls,
  d.hint_offline_invoca_calls,
  d.hint_offline_invoca_eligibility,
  d.hint_offline_invoca_order,
  d.hint_offline_invoca_order_rt,
  d.hint_offline_invoca_sales_opp,
  d.ma_hint_ec_eligibility_check,

  -- ============================================================
  -- FIBER
  -- ============================================================

  d.fiber_activations,
  d.fiber_pre_order,
  d.fiber_waitlist_sign_up,
  d.fiber_web_orders,
  d.fiber_ec,
  d.fiber_ec_dda,
  d.fiber_sec,
  d.fiber_sec_dda,

  -- ============================================================
  -- METRO
  -- ============================================================

  d.metro_top_funnel_prospect,
  d.metro_upper_funnel_prospect,
  d.metro_mid_funnel_prospect,
  d.metro_low_funnel_cs,
  d.metro_qt,
  d.metro_hint_qt,

  -- ============================================================
  -- TMO
  -- ============================================================

  d.tmo_top_funnel_prospect,
  d.tmo_upper_funnel_prospect,
  d.tmo_prepaid_low_funnel_prospect,

  -- ============================================================
  -- TFB
  -- ============================================================

  d.tfb_credit_check,
  d.tfb_invoca_sales_calls,
  d.tfb_leads,
  d.tfb_quality_traffic,
  d.tfb_hint_ec,
  d.total_tfb_conversions,

  -- ============================================================
  -- OTHER
  -- ============================================================

  d.magenta_pqt,

  -- ============================================================
  -- METADATA
  -- ============================================================

  d.file_load_datetime,
  CURRENT_TIMESTAMP() AS silver_inserted_at

FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` d

LEFT JOIN latest_entity e
  ON d.account_id = e.account_id
 AND d.campaign_id = e.campaign_id;
