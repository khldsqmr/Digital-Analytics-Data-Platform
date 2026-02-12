/*
===============================================================================
BRONZE | SA 360 | CAMPAIGN DAILY | INCREMENTAL MERGE
===============================================================================

GRAIN
account_id + campaign_id + date

--
-- FILE: campaign_daily_merge.sql
--

NOTES
- 7-day lookback window for late-arriving data
- Safe for daily scheduling
- Idempotent MERGE logic

===============================================================================
*/

DECLARE lookback_days INT64 DEFAULT 7;

MERGE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_sa360_campaign_daily` T

USING (

  SELECT
    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,
    customer_name,
    client_manager_id,
    client_manager_name,

    date_yyyymmdd,
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS date,
    SAFE.PARSE_DATE('%Y-%m-%d', segments_date) AS segments_date,

    date AS raw_numeric_date,
    __insert_date,
    File_Load_datetime AS file_load_datetime,
    Filename AS filename,

    clicks,
    impressions,
    cost_micros,
    SAFE_DIVIDE(cost_micros, 1000000) AS cost,
    all_conversions,

    postpaid__cart__start_ AS postpaid_cart_start,
    postpaid_pspv_ AS postpaid_pspv,
    aal,
    add_a__line AS add_a_line,

    hint_ec,
    hint_sec,
    hint__web__orders AS hint_web_orders,
    hint__invoca__calls AS hint_invoca_calls,
    hint__offline__invoca__calls AS hint_offline_invoca_calls,
    hint__offline__invoca__eligibility AS hint_offline_invoca_eligibility,
    hint__offline__invoca__order AS hint_offline_invoca_order,
    hint__offline__invoca__order_rt_ AS hint_offline_invoca_order_rt,
    hint__offline__invoca__sales__opp AS hint_offline_invoca_sales_opp,
    _ma_hint_ec__eligibility__check_ AS ma_hint_ec_eligibility_check,

    fiber__activations AS fiber_activations,
    fiber__pre__order AS fiber_pre_order,
    fiber__waitlist__sign__up AS fiber_waitlist_sign_up,
    fiber__web__orders AS fiber_web_orders,
    fiber_ec,
    fiber_ec_dda,
    fiber_sec,
    fiber_sec_dda,

    metro__top__funnel__prospect AS metro_top_funnel_prospect,
    metro__upper__funnel__prospect AS metro_upper_funnel_prospect,
    metro__mid__funnel__prospect AS metro_mid_funnel_prospect,
    metro__low__funnel_cs_ AS metro_low_funnel_cs,
    metro_qt,
    metro_hint_qt,

    tmo__top__funnel__prospect AS tmo_top_funnel_prospect,
    tmo__upper__funnel__prospect AS tmo_upper_funnel_prospect,
    t__mobile__prepaid__low__funnel__prospect AS tmo_prepaid_low_funnel_prospect,

    tfb__credit__check AS tfb_credit_check,
    tfb__invoca__sales__calls AS tfb_invoca_sales_calls,
    tfb__leads AS tfb_leads,
    tfb__quality__traffic AS tfb_quality_traffic,
    tfb_hint_ec,
    total_tfb__conversions AS total_tfb_conversions,

    magenta_pqt,

    CURRENT_TIMESTAMP() AS bronze_inserted_at

  FROM
  `prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo`

  WHERE
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING))
    >= DATE_SUB(CURRENT_DATE(), INTERVAL lookback_days DAY)

) S

ON
  T.account_id = S.account_id
  AND T.campaign_id = S.campaign_id
  AND T.date_yyyymmdd = S.date_yyyymmdd

WHEN MATCHED THEN
  UPDATE SET
    clicks = S.clicks,
    impressions = S.impressions,
    cost_micros = S.cost_micros,
    cost = S.cost,
    all_conversions = S.all_conversions,
    bronze_inserted_at = CURRENT_TIMESTAMP()

WHEN NOT MATCHED THEN
  INSERT (
    account_id,
    account_name,
    campaign_id,
    resource_name,
    customer_id,
    customer_name,
    client_manager_id,
    client_manager_name,
    date_yyyymmdd,
    date,
    segments_date,
    raw_numeric_date,
    __insert_date,
    file_load_datetime,
    filename,
    clicks,
    impressions,
    cost_micros,
    cost,
    all_conversions,
    postpaid_cart_start,
    postpaid_pspv,
    aal,
    add_a_line,
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
    metro_top_funnel_prospect,
    metro_upper_funnel_prospect,
    metro_mid_funnel_prospect,
    metro_low_funnel_cs,
    metro_qt,
    metro_hint_qt,
    tmo_top_funnel_prospect,
    tmo_upper_funnel_prospect,
    tmo_prepaid_low_funnel_prospect,
    tfb_credit_check,
    tfb_invoca_sales_calls,
    tfb_leads,
    tfb_quality_traffic,
    tfb_hint_ec,
    total_tfb_conversions,
    magenta_pqt,
    bronze_inserted_at
  )
  VALUES (
    S.account_id,
    S.account_name,
    S.campaign_id,
    S.resource_name,
    S.customer_id,
    S.customer_name,
    S.client_manager_id,
    S.client_manager_name,
    S.date_yyyymmdd,
    S.date,
    S.segments_date,
    S.raw_numeric_date,
    S.__insert_date,
    S.file_load_datetime,
    S.filename,
    S.clicks,
    S.impressions,
    S.cost_micros,
    S.cost,
    S.all_conversions,
    S.postpaid_cart_start,
    S.postpaid_pspv,
    S.aal,
    S.add_a_line,
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
    S.metro_top_funnel_prospect,
    S.metro_upper_funnel_prospect,
    S.metro_mid_funnel_prospect,
    S.metro_low_funnel_cs,
    S.metro_qt,
    S.metro_hint_qt,
    S.tmo_top_funnel_prospect,
    S.tmo_upper_funnel_prospect,
    S.tmo_prepaid_low_funnel_prospect,
    S.tfb_credit_check,
    S.tfb_invoca_sales_calls,
    S.tfb_leads,
    S.tfb_quality_traffic,
    S.tfb_hint_ec,
    S.total_tfb_conversions,
    S.magenta_pqt,
    S.bronze_inserted_at
  );
