CREATE OR REPLACE TABLE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_paid_search_campaign_daily`
PARTITION BY date
CLUSTER BY account_id, campaign_id, campaign_type
AS

/* ===============================================================
   STEP 1: Deduplicate Campaign Entity (Latest Snapshot Per Campaign)
   =============================================================== */

WITH latest_campaign_entity AS (

  SELECT
      account_id,
      campaign_id,
      name AS campaign_name,
      advertising_channel_type,
      advertising_channel_sub_type,
      bidding_strategy_type,
      status,
      serving_status
  FROM (
      SELECT
          account_id,
          campaign_id,
          name,
          advertising_channel_type,
          advertising_channel_sub_type,
          bidding_strategy_type,
          status,
          serving_status,
          file_load_datetime,
          bronze_inserted_at,
          ROW_NUMBER() OVER (
              PARTITION BY account_id, campaign_id
              ORDER BY file_load_datetime DESC,
                       bronze_inserted_at DESC
          ) AS rn
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
  )
  WHERE rn = 1
)

/* ===============================================================
   STEP 2: Build Clean Enriched Daily Fact
   =============================================================== */

SELECT

  /* =========================
     GRAIN
     ========================= */

  d.account_id,
  d.account_name,
  d.campaign_id,
  e.campaign_name,
  d.date,

  /* =========================
     CALENDAR
     ========================= */

  EXTRACT(YEAR FROM d.date) AS year,
  EXTRACT(MONTH FROM d.date) AS month,
  FORMAT_DATE('%Y-Q%Q', d.date) AS quarter,
  EXTRACT(ISOWEEK FROM d.date) AS iso_week,

  /* =========================
     CAMPAIGN CLASSIFICATION
     ========================= */

  CASE
      WHEN LOWER(COALESCE(e.campaign_name, '')) LIKE '%brand%' THEN 'Brand'
      WHEN LOWER(COALESCE(e.campaign_name, '')) LIKE '%generic%' THEN 'Generic'
      WHEN LOWER(COALESCE(e.campaign_name, '')) LIKE '%shopping%' THEN 'Shopping'
      WHEN LOWER(COALESCE(e.campaign_name, '')) LIKE '%shop%' THEN 'Shopping'
      WHEN LOWER(COALESCE(e.campaign_name, '')) LIKE '%pmax%' THEN 'PMax'
      WHEN LOWER(COALESCE(e.campaign_name, '')) LIKE '%demandgen%' THEN 'DemandGen'
      ELSE 'Unclassified'
  END AS campaign_type,

  e.advertising_channel_type,
  e.advertising_channel_sub_type,
  e.bidding_strategy_type,
  e.status,
  e.serving_status,

  /* =========================
     CORE PERFORMANCE
     ========================= */

  d.impressions,
  d.clicks,
  d.cost,
  d.all_conversions,

  /* =========================
     POSTPAID
     ========================= */

  d.postpaid_cart_start,
  d.postpaid_pspv,
  d.digital_gross_add,
  d.buying_intent,
  d.aal,
  d.add_a_line,
  d.ma_hint_ec_eligibility_check,

  /* =========================
     HINT
     ========================= */

  d.hint_ec,
  d.hint_sec,
  d.hint_web_orders,
  d.hint_invoca_calls,
  d.hint_offline_invoca_calls,

  /* =========================
     FIBER
     ========================= */

  d.fiber_activations,
  d.fiber_pre_order,
  d.fiber_waitlist_sign_up,
  d.fiber_web_orders,
  d.fiber_ec,
  d.fiber_ec_dda,
  d.fiber_sec_dda,

  /* =========================
     TMO
     ========================= */

  d.tmo_top_funnel_prospect,
  d.tmo_upper_funnel_prospect,

  /* =========================
     TBG
     ========================= */

  d.tbg_low_funnel,
  d.tbg_lead_form_submit,
  d.tbg_invoca_sales_intent_dda,
  d.tbg_invoca_order_dda,

  /* =========================
     METRO
     ========================= */

  d.metro_top_funnel_prospect,
  d.metro_upper_funnel_prospect,
  d.metro_mid_funnel_prospect,
  d.metro_low_funnel_cs,

  /* =========================
     LOAD METADATA
     ========================= */

  d.file_load_datetime,
  CURRENT_TIMESTAMP() AS silver_inserted_at

FROM
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` d

LEFT JOIN latest_campaign_entity e
  ON d.account_id = e.account_id
 AND d.campaign_id = e.campaign_id;
