/*
===============================================================================
FILE: 01_merge_sdi_silver_sa360_campaign_daily.sql
LAYER: Silver
PROC:  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_silver_sa360_campaign_daily

CORRECTNESS GUARANTEES:
  1) Row coverage: every Bronze Daily row in window produces exactly 1 Silver row
     (assuming Bronze Daily is unique on account_id+campaign_id+date_yyyymmdd).
  2) Entity enrichment:
     - AS-OF entity row used for settings fields (e.date <= d.date).
     - campaign_name fallback to latest non-null name for that campaign_id.
  3) Deterministic tie-breakers.
===============================================================================
*/

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_merge_silver_sa360_campaign_daily`()
OPTIONS(strict_mode=false)
BEGIN
  DECLARE lookback_days INT64 DEFAULT 7;

  MERGE `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_silver_sa360_campaign_daily` T
  USING (
    WITH filtered_daily AS (
      SELECT *
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily`
      WHERE date >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)
    ),

    latest_name AS (
      SELECT
        account_id,
        campaign_id,
        ARRAY_AGG(NULLIF(campaign_name,'') IGNORE NULLS
                  ORDER BY date DESC, file_load_datetime DESC, filename DESC
                  LIMIT 1)[OFFSET(0)] AS latest_nonnull_campaign_name
      FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity`
      GROUP BY 1,2
    ),

    asof_entity AS (
      SELECT
        d.account_id,
        d.campaign_id,
        d.date,
        e.campaign_name,
        e.advertising_channel_type,
        e.advertising_channel_sub_type,
        e.bidding_strategy_type,
        e.serving_status
      FROM filtered_daily d
      LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_entity` e
        ON  d.account_id = e.account_id
        AND d.campaign_id = e.campaign_id
        AND e.date <= d.date
      QUALIFY
        ROW_NUMBER() OVER (
          PARTITION BY d.account_id, d.campaign_id, d.date
          ORDER BY e.date DESC, e.file_load_datetime DESC, e.filename DESC
        ) = 1
    ),

    final AS (
      SELECT
        d.account_id,
        d.campaign_id,
        d.date,
        d.date_yyyymmdd,

        d.account_name,

        CASE
          WHEN d.account_name IN ('Postpaid Google','Postpaid Bing','BTS Google','BTS Bing') THEN 'Postpaid'
          WHEN d.account_name IN ('Broadband Google','Broadband Bing') THEN 'HSI'
          WHEN d.account_name IN ('Fiber Google','Fiber Bing') THEN 'Fiber'
          WHEN d.account_name IN ('Metro Google','Metro Bing') THEN 'Metro'
          WHEN d.account_name IN ('TFB Google','TFB Bing') THEN 'TFB'
          ELSE 'Unclassified'
        END AS lob,

        CASE
          WHEN LOWER(d.account_name) LIKE '%google%' THEN 'Google'
          WHEN LOWER(d.account_name) LIKE '%bing%' THEN 'Bing'
          ELSE 'Unknown'
        END AS ad_platform,

        COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name) AS campaign_name,

        CASE
          WHEN COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name) IS NULL THEN 'Unclassified'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'(^|[^a-z])brand([^a-z]|$)') THEN 'Brand'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'(^|[^a-z])generic([^a-z]|$)') THEN 'Generic'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'shopping|shop') THEN 'Shopping'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'pmax|performance\s*max') THEN 'PMax'
          WHEN REGEXP_CONTAINS(LOWER(COALESCE(NULLIF(a.campaign_name,''), n.latest_nonnull_campaign_name)), r'demand\s*gen|demandgen') THEN 'DemandGen'
          ELSE 'Unclassified'
        END AS campaign_type,

        a.advertising_channel_type,
        a.advertising_channel_sub_type,
        a.bidding_strategy_type,
        a.serving_status,

        d.customer_id,
        d.customer_name,
        d.resource_name,
        d.segments_date,
        d.client_manager_id,
        d.client_manager_name,

        d.impressions,
        d.clicks,
        d.cost,
        d.all_conversions,

        d.bi,
        d.buying_intent,
        d.bts_quality_traffic,
        d.digital_gross_add,
        d.magenta_pqt,

        d.cart_start,
        d.postpaid_cart_start,
        d.postpaid_pspv,
        d.aal,
        d.add_a_line,

        d.connect_low_funnel_prospect,
        d.connect_low_funnel_visit,
        d.connect_qt,

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

        d.fiber_activations,
        d.fiber_pre_order,
        d.fiber_waitlist_sign_up,
        d.fiber_web_orders,
        d.fiber_ec,
        d.fiber_ec_dda,
        d.fiber_sec,
        d.fiber_sec_dda,

        d.metro_low_funnel_cs,
        d.metro_mid_funnel_prospect,
        d.metro_top_funnel_prospect,
        d.metro_upper_funnel_prospect,
        d.metro_hint_qt,
        d.metro_qt,

        d.tmo_prepaid_low_funnel_prospect,
        d.tmo_top_funnel_prospect,
        d.tmo_upper_funnel_prospect,

        d.tfb_low_funnel,
        d.tfb_lead_form_submit,
        d.tfb_invoca_sales_intent_dda,
        d.tfb_invoca_order_dda,
        d.tfb_credit_check,
        d.tfb_hint_ec,
        d.tfb_invoca_sales_calls,
        d.tfb_leads,
        d.tfb_quality_traffic,
        d.total_tfb_conversions,

        d.file_load_datetime,
        CURRENT_TIMESTAMP() AS silver_inserted_at

      FROM filtered_daily d
      LEFT JOIN asof_entity a
        ON d.account_id = a.account_id
       AND d.campaign_id = a.campaign_id
       AND d.date = a.date
      LEFT JOIN latest_name n
        ON d.account_id = n.account_id
       AND d.campaign_id = n.campaign_id
    )

    SELECT * FROM final
  ) S
  ON  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date = S.date

  WHEN MATCHED THEN UPDATE SET
    date_yyyymmdd = S.date_yyyymmdd,
    account_name = S.account_name,
    lob = S.lob,
    ad_platform = S.ad_platform,
    campaign_name = S.campaign_name,
    campaign_type = S.campaign_type,
    advertising_channel_type = S.advertising_channel_type,
    advertising_channel_sub_type = S.advertising_channel_sub_type,
    bidding_strategy_type = S.bidding_strategy_type,
    serving_status = S.serving_status,
    customer_id = S.customer_id,
    customer_name = S.customer_name,
    resource_name = S.resource_name,
    segments_date = S.segments_date,
    client_manager_id = S.client_manager_id,
    client_manager_name = S.client_manager_name,
    impressions = S.impressions,
    clicks = S.clicks,
    cost = S.cost,
    all_conversions = S.all_conversions,
    bi = S.bi,
    buying_intent = S.buying_intent,
    bts_quality_traffic = S.bts_quality_traffic,
    digital_gross_add = S.digital_gross_add,
    magenta_pqt = S.magenta_pqt,
    cart_start = S.cart_start,
    postpaid_cart_start = S.postpaid_cart_start,
    postpaid_pspv = S.postpaid_pspv,
    aal = S.aal,
    add_a_line = S.add_a_line,
    connect_low_funnel_prospect = S.connect_low_funnel_prospect,
    connect_low_funnel_visit = S.connect_low_funnel_visit,
    connect_qt = S.connect_qt,
    hint_ec = S.hint_ec,
    hint_sec = S.hint_sec,
    hint_web_orders = S.hint_web_orders,
    hint_invoca_calls = S.hint_invoca_calls,
    hint_offline_invoca_calls = S.hint_offline_invoca_calls,
    hint_offline_invoca_eligibility = S.hint_offline_invoca_eligibility,
    hint_offline_invoca_order = S.hint_offline_invoca_order,
    hint_offline_invoca_order_rt = S.hint_offline_invoca_order_rt,
    hint_offline_invoca_sales_opp = S.hint_offline_invoca_sales_opp,
    ma_hint_ec_eligibility_check = S.ma_hint_ec_eligibility_check,
    fiber_activations = S.fiber_activations,
    fiber_pre_order = S.fiber_pre_order,
    fiber_waitlist_sign_up = S.fiber_waitlist_sign_up,
    fiber_web_orders = S.fiber_web_orders,
    fiber_ec = S.fiber_ec,
    fiber_ec_dda = S.fiber_ec_dda,
    fiber_sec = S.fiber_sec,
    fiber_sec_dda = S.fiber_sec_dda,
    metro_low_funnel_cs = S.metro_low_funnel_cs,
    metro_mid_funnel_prospect = S.metro_mid_funnel_prospect,
    metro_top_funnel_prospect = S.metro_top_funnel_prospect,
    metro_upper_funnel_prospect = S.metro_upper_funnel_prospect,
    metro_hint_qt = S.metro_hint_qt,
    metro_qt = S.metro_qt,
    tmo_prepaid_low_funnel_prospect = S.tmo_prepaid_low_funnel_prospect,
    tmo_top_funnel_prospect = S.tmo_top_funnel_prospect,
    tmo_upper_funnel_prospect = S.tmo_upper_funnel_prospect,
    tfb_low_funnel = S.tfb_low_funnel,
    tfb_lead_form_submit = S.tfb_lead_form_submit,
    tfb_invoca_sales_intent_dda = S.tfb_invoca_sales_intent_dda,
    tfb_invoca_order_dda = S.tfb_invoca_order_dda,
    tfb_credit_check = S.tfb_credit_check,
    tfb_hint_ec = S.tfb_hint_ec,
    tfb_invoca_sales_calls = S.tfb_invoca_sales_calls,
    tfb_leads = S.tfb_leads,
    tfb_quality_traffic = S.tfb_quality_traffic,
    total_tfb_conversions = S.total_tfb_conversions,
    file_load_datetime = S.file_load_datetime,
    silver_inserted_at = CURRENT_TIMESTAMP()

  WHEN NOT MATCHED THEN INSERT (
  account_id,
  campaign_id,
  date,
  date_yyyymmdd,
  account_name,
  lob,
  ad_platform,
  campaign_name,
  campaign_type,
  advertising_channel_type,
  advertising_channel_sub_type,
  bidding_strategy_type,
  serving_status,
  customer_id,
  customer_name,
  resource_name,
  segments_date,
  client_manager_id,
  client_manager_name,
  impressions,
  clicks,
  cost,
  all_conversions,
  bi,
  buying_intent,
  bts_quality_traffic,
  digital_gross_add,
  magenta_pqt,
  cart_start,
  postpaid_cart_start,
  postpaid_pspv,
  aal,
  add_a_line,
  connect_low_funnel_prospect,
  connect_low_funnel_visit,
  connect_qt,
  hint_ec,
  hint_sec,
  hint_web_orders,
  hint_invoca_calls,
  hint_offline_invoca_calls,
  hint_offline_invoca_eligibility,
  hint_offline_invoca_order,
  hint_offline_invoca_order_rt,
  hint_offline_invoca_sales_opp,
  ma_hint_ec_eligibility_check,
  fiber_activations,
  fiber_pre_order,
  fiber_waitlist_sign_up,
  fiber_web_orders,
  fiber_ec,
  fiber_ec_dda,
  fiber_sec,
  fiber_sec_dda,
  metro_low_funnel_cs,
  metro_mid_funnel_prospect,
  metro_top_funnel_prospect,
  metro_upper_funnel_prospect,
  metro_hint_qt,
  metro_qt,
  tmo_prepaid_low_funnel_prospect,
  tmo_top_funnel_prospect,
  tmo_upper_funnel_prospect,
  tfb_low_funnel,
  tfb_lead_form_submit,
  tfb_invoca_sales_intent_dda,
  tfb_invoca_order_dda,
  tfb_credit_check,
  tfb_hint_ec,
  tfb_invoca_sales_calls,
  tfb_leads,
  tfb_quality_traffic,
  total_tfb_conversions,
  file_load_datetime,
  silver_inserted_at
)
VALUES (
  S.account_id,
  S.campaign_id,
  S.date,
  S.date_yyyymmdd,
  S.account_name,
  S.lob,
  S.ad_platform,
  S.campaign_name,
  S.campaign_type,
  S.advertising_channel_type,
  S.advertising_channel_sub_type,
  S.bidding_strategy_type,
  S.serving_status,
  S.customer_id,
  S.customer_name,
  S.resource_name,
  S.segments_date,
  S.client_manager_id,
  S.client_manager_name,
  S.impressions,
  S.clicks,
  S.cost,
  S.all_conversions,
  S.bi,
  S.buying_intent,
  S.bts_quality_traffic,
  S.digital_gross_add,
  S.magenta_pqt,
  S.cart_start,
  S.postpaid_cart_start,
  S.postpaid_pspv,
  S.aal,
  S.add_a_line,
  S.connect_low_funnel_prospect,
  S.connect_low_funnel_visit,
  S.connect_qt,
  S.hint_ec,
  S.hint_sec,
  S.hint_web_orders,
  S.hint_invoca_calls,
  S.hint_offline_invoca_calls,
  S.hint_offline_invoca_eligibility,
  S.hint_offline_invoca_order,
  S.hint_offline_invoca_order_rt,
  S.hint_offline_invoca_sales_opp,
  S.ma_hint_ec_eligibility_check,
  S.fiber_activations,
  S.fiber_pre_order,
  S.fiber_waitlist_sign_up,
  S.fiber_web_orders,
  S.fiber_ec,
  S.fiber_ec_dda,
  S.fiber_sec,
  S.fiber_sec_dda,
  S.metro_low_funnel_cs,
  S.metro_mid_funnel_prospect,
  S.metro_top_funnel_prospect,
  S.metro_upper_funnel_prospect,
  S.metro_hint_qt,
  S.metro_qt,
  S.tmo_prepaid_low_funnel_prospect,
  S.tmo_top_funnel_prospect,
  S.tmo_upper_funnel_prospect,
  S.tfb_low_funnel,
  S.tfb_lead_form_submit,
  S.tfb_invoca_sales_intent_dda,
  S.tfb_invoca_order_dda,
  S.tfb_credit_check,
  S.tfb_hint_ec,
  S.tfb_invoca_sales_calls,
  S.tfb_leads,
  S.tfb_quality_traffic,
  S.total_tfb_conversions,
  S.file_load_datetime,
  CURRENT_TIMESTAMP()
);


END;
