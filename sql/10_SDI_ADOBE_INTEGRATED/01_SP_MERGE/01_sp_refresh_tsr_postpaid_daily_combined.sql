/* =================================================================================================
FILE: 01_sp_refresh_tsr_postpaid_daily_combined.sql
OBJECT TYPE: Stored Procedure
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE: sp_refresh_tsr_postpaid_daily_combined

TARGET TABLE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.tsr_postpaid_daily_combined

SOURCES:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_gold_entryPages_daily

PURPOSE:
  Refreshes a physical POSTPAID daily combined table using TSR as the reporting spine.
  The output includes:
    - TSR daily funnel metrics
    - Orders daily metrics
    - Cart Start Plus daily metrics
    - Clickstream Entry Page metrics pivoted into separate page-group columns
    - LY clickstream metrics based on same calendar date last year from the clickstream daily Gold view

BUSINESS GRAIN:
  One row per:
    event_date
    + lob
    + last_touch_channel

KEY MODELING NOTES:
  - TSR remains the spine, so all TSR rows are preserved.
  - Orders, Cart Start Plus, and Clickstream are aggregated to the TSR grain before joining.
  - Clickstream is first aggregated by daily channel + entry_page_group, then pivoted to one row per daily channel.
  - This prevents join-driven row duplication.
  - Clickstream LY values come from vw_sdi_clickstream_gold_entryPages_daily and therefore represent same-calendar-date last year.

REFRESH PATTERN:
  CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_refresh_tsr_postpaid_daily_combined`();
================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_refresh_tsr_postpaid_daily_combined`()
BEGIN

  CREATE OR REPLACE TABLE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.tsr_postpaid_daily_combined`
  PARTITION BY event_date
  CLUSTER BY lob, last_touch_channel
  AS

  WITH tsr_daily AS (
    SELECT
      SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) AS event_date,
      date_yyyymmdd,
      'POSTPAID' AS lob,
      last_touch_channel,
      COALESCE(NULLIF(UPPER(TRIM(last_touch_channel)), ''), 'NONE') AS channel_raw_upper,

      SUM(COALESCE(visits_enterprise_prospect_visits, 0)) AS entries,
      SUM(COALESCE(postpaid_voice_prospect_shop_page_visits, 0)) AS pspv_actuals,
      SUM(COALESCE(postpaid_voice_prospect_cart_start_visits, 0)) AS cart_starts,
      SUM(COALESCE(postpaid_voice_prospect_cart_checkout_visits, 0)) AS cart_checkout_visits,
      SUM(COALESCE(postpaid_voice_prospect_checkout_4_0_review, 0)) AS checkout_review_visits,
      SUM(COALESCE(postpaid_voice_prospect_orders, 0)) AS postpaid_orders_tsr
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo`
    WHERE (
        visits_enterprise_prospect_visits IS NOT NULL
        OR postpaid_voice_prospect_shop_page_visits IS NOT NULL
        OR postpaid_voice_prospect_cart_start_visits IS NOT NULL
        OR postpaid_voice_prospect_cart_checkout_visits IS NOT NULL
        OR postpaid_voice_prospect_checkout_4_0_review IS NOT NULL
        OR postpaid_voice_prospect_orders IS NOT NULL
    )
      AND last_touch_channel IS NOT NULL
      AND SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
    GROUP BY
      1, 2, 3, 4, 5
  ),

  orders_daily AS (
    SELECT
      date,
      'POSTPAID' AS lob,
      last_touch_channel,
      COALESCE(NULLIF(UPPER(TRIM(last_touch_channel)), ''), 'NONE') AS channel_raw_upper,

      SUM(COALESCE(orders_web_unassisted, 0)) AS orders_web_unassisted,
      SUM(COALESCE(orders_web_assisted, 0)) AS orders_web_assisted,
      SUM(COALESCE(orders_app_unassisted, 0)) AS orders_app_unassisted,
      SUM(COALESCE(orders_app_assisted, 0)) AS orders_app_assisted,
      SUM(COALESCE(orders_web_all, 0)) AS orders_web_all,
      SUM(COALESCE(orders_app_all, 0)) AS orders_app_all,
      SUM(COALESCE(orders_fully_unassisted, 0)) AS orders_fully_unassisted,
      SUM(COALESCE(orders_fully_assisted, 0)) AS orders_fully_assisted,
      SUM(COALESCE(orders_all, 0)) AS orders_all
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily`
    WHERE UPPER(TRIM(lob)) = 'POSTPAID'
    GROUP BY
      1, 2, 3, 4
  ),

  cart_start_plus_daily AS (
    SELECT
      date,
      'POSTPAID' AS lob,
      last_touch_channel,
      COALESCE(NULLIF(UPPER(TRIM(last_touch_channel)), ''), 'NONE') AS channel_raw_upper,
      SUM(COALESCE(cart_start_plus, 0)) AS cart_start_plus
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily`
    WHERE UPPER(TRIM(lob)) = 'POSTPAID'
    GROUP BY
      1, 2, 3, 4
  ),

  clickstream_daily_pagegroup AS (
    SELECT
      session_day AS event_date,
      'POSTPAID' AS lob,
      COALESCE(NULLIF(UPPER(TRIM(session_channel_name)), ''), 'NONE') AS channel_raw_upper,
      entry_page_group,

      SUM(COALESCE(entry_sessions, 0)) AS entry_sessions,
      SUM(COALESCE(entry_sessions_LY, 0)) AS entry_sessions_ly,
      SUM(COALESCE(pspv_sessions, 0)) AS pspv_sessions,
      SUM(COALESCE(pspv_sessions_LY, 0)) AS pspv_sessions_ly,
      SUM(COALESCE(cart_start_sessions, 0)) AS cart_start_sessions,
      SUM(COALESCE(cart_start_sessions_LY, 0)) AS cart_start_sessions_ly,
      SUM(COALESCE(checkout_sessions, 0)) AS checkout_sessions,
      SUM(COALESCE(checkout_sessions_LY, 0)) AS checkout_sessions_ly,
      SUM(COALESCE(order_sessions, 0)) AS order_sessions,
      SUM(COALESCE(order_sessions_LY, 0)) AS order_sessions_ly
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_gold_entryPages_daily`
    WHERE UPPER(TRIM(lob)) = 'POSTPAID'
    GROUP BY
      1, 2, 3, 4
  ),

  clickstream_daily_pivot AS (
    SELECT
      event_date,
      lob,
      channel_raw_upper,

      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN entry_sessions ELSE 0 END) AS cs_pg01AccountBillingLogin_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN entry_sessions_ly ELSE 0 END) AS cs_pg01AccountBillingLogin_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN pspv_sessions ELSE 0 END) AS cs_pg01AccountBillingLogin_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg01AccountBillingLogin_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN cart_start_sessions ELSE 0 END) AS cs_pg01AccountBillingLogin_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg01AccountBillingLogin_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN checkout_sessions ELSE 0 END) AS cs_pg01AccountBillingLogin_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg01AccountBillingLogin_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN order_sessions ELSE 0 END) AS cs_pg01AccountBillingLogin_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Account / Billing / Login' THEN order_sessions_ly ELSE 0 END) AS cs_pg01AccountBillingLogin_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN entry_sessions ELSE 0 END) AS cs_pg02AppTLife_entrySessions,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN entry_sessions_ly ELSE 0 END) AS cs_pg02AppTLife_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN pspv_sessions ELSE 0 END) AS cs_pg02AppTLife_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg02AppTLife_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN cart_start_sessions ELSE 0 END) AS cs_pg02AppTLife_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg02AppTLife_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN checkout_sessions ELSE 0 END) AS cs_pg02AppTLife_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg02AppTLife_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN order_sessions ELSE 0 END) AS cs_pg02AppTLife_orderSessions,
      SUM(CASE WHEN entry_page_group = 'App / T-Life' THEN order_sessions_ly ELSE 0 END) AS cs_pg02AppTLife_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN entry_sessions ELSE 0 END) AS cs_pg03BrandMarketing_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN entry_sessions_ly ELSE 0 END) AS cs_pg03BrandMarketing_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN pspv_sessions ELSE 0 END) AS cs_pg03BrandMarketing_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg03BrandMarketing_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN cart_start_sessions ELSE 0 END) AS cs_pg03BrandMarketing_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg03BrandMarketing_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN checkout_sessions ELSE 0 END) AS cs_pg03BrandMarketing_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg03BrandMarketing_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN order_sessions ELSE 0 END) AS cs_pg03BrandMarketing_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Brand / Marketing' THEN order_sessions_ly ELSE 0 END) AS cs_pg03BrandMarketing_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN entry_sessions ELSE 0 END) AS cs_pg04CartCheckout_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN entry_sessions_ly ELSE 0 END) AS cs_pg04CartCheckout_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN pspv_sessions ELSE 0 END) AS cs_pg04CartCheckout_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg04CartCheckout_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN cart_start_sessions ELSE 0 END) AS cs_pg04CartCheckout_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg04CartCheckout_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN checkout_sessions ELSE 0 END) AS cs_pg04CartCheckout_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg04CartCheckout_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN order_sessions ELSE 0 END) AS cs_pg04CartCheckout_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Cart / Checkout' THEN order_sessions_ly ELSE 0 END) AS cs_pg04CartCheckout_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN entry_sessions ELSE 0 END) AS cs_pg05CoverageNetwork_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN entry_sessions_ly ELSE 0 END) AS cs_pg05CoverageNetwork_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN pspv_sessions ELSE 0 END) AS cs_pg05CoverageNetwork_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg05CoverageNetwork_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN cart_start_sessions ELSE 0 END) AS cs_pg05CoverageNetwork_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg05CoverageNetwork_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN checkout_sessions ELSE 0 END) AS cs_pg05CoverageNetwork_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg05CoverageNetwork_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN order_sessions ELSE 0 END) AS cs_pg05CoverageNetwork_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Coverage / Network' THEN order_sessions_ly ELSE 0 END) AS cs_pg05CoverageNetwork_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN entry_sessions ELSE 0 END) AS cs_pg06DealsOffers_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN entry_sessions_ly ELSE 0 END) AS cs_pg06DealsOffers_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN pspv_sessions ELSE 0 END) AS cs_pg06DealsOffers_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg06DealsOffers_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN cart_start_sessions ELSE 0 END) AS cs_pg06DealsOffers_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg06DealsOffers_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN checkout_sessions ELSE 0 END) AS cs_pg06DealsOffers_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg06DealsOffers_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN order_sessions ELSE 0 END) AS cs_pg06DealsOffers_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Deals / Offers' THEN order_sessions_ly ELSE 0 END) AS cs_pg06DealsOffers_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Homepage' THEN entry_sessions ELSE 0 END) AS cs_pg07Homepage_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN entry_sessions_ly ELSE 0 END) AS cs_pg07Homepage_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN pspv_sessions ELSE 0 END) AS cs_pg07Homepage_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg07Homepage_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN cart_start_sessions ELSE 0 END) AS cs_pg07Homepage_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg07Homepage_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN checkout_sessions ELSE 0 END) AS cs_pg07Homepage_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg07Homepage_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN order_sessions ELSE 0 END) AS cs_pg07Homepage_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Homepage' THEN order_sessions_ly ELSE 0 END) AS cs_pg07Homepage_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Order Status' THEN entry_sessions ELSE 0 END) AS cs_pg08OrderStatus_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN entry_sessions_ly ELSE 0 END) AS cs_pg08OrderStatus_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN pspv_sessions ELSE 0 END) AS cs_pg08OrderStatus_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg08OrderStatus_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN cart_start_sessions ELSE 0 END) AS cs_pg08OrderStatus_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg08OrderStatus_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN checkout_sessions ELSE 0 END) AS cs_pg08OrderStatus_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg08OrderStatus_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN order_sessions ELSE 0 END) AS cs_pg08OrderStatus_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Order Status' THEN order_sessions_ly ELSE 0 END) AS cs_pg08OrderStatus_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Other' THEN entry_sessions ELSE 0 END) AS cs_pg09Other_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Other' THEN entry_sessions_ly ELSE 0 END) AS cs_pg09Other_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Other' THEN pspv_sessions ELSE 0 END) AS cs_pg09Other_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Other' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg09Other_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Other' THEN cart_start_sessions ELSE 0 END) AS cs_pg09Other_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Other' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg09Other_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Other' THEN checkout_sessions ELSE 0 END) AS cs_pg09Other_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Other' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg09Other_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Other' THEN order_sessions ELSE 0 END) AS cs_pg09Other_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Other' THEN order_sessions_ly ELSE 0 END) AS cs_pg09Other_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN entry_sessions ELSE 0 END) AS cs_pg10PDPDetail_entrySessions,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN entry_sessions_ly ELSE 0 END) AS cs_pg10PDPDetail_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN pspv_sessions ELSE 0 END) AS cs_pg10PDPDetail_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg10PDPDetail_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN cart_start_sessions ELSE 0 END) AS cs_pg10PDPDetail_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg10PDPDetail_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN checkout_sessions ELSE 0 END) AS cs_pg10PDPDetail_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg10PDPDetail_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN order_sessions ELSE 0 END) AS cs_pg10PDPDetail_orderSessions,
      SUM(CASE WHEN entry_page_group = 'PDP / Detail' THEN order_sessions_ly ELSE 0 END) AS cs_pg10PDPDetail_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN entry_sessions ELSE 0 END) AS cs_pg11PLPBrowse_entrySessions,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN entry_sessions_ly ELSE 0 END) AS cs_pg11PLPBrowse_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN pspv_sessions ELSE 0 END) AS cs_pg11PLPBrowse_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg11PLPBrowse_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN cart_start_sessions ELSE 0 END) AS cs_pg11PLPBrowse_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg11PLPBrowse_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN checkout_sessions ELSE 0 END) AS cs_pg11PLPBrowse_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg11PLPBrowse_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN order_sessions ELSE 0 END) AS cs_pg11PLPBrowse_orderSessions,
      SUM(CASE WHEN entry_page_group = 'PLP / Browse' THEN order_sessions_ly ELSE 0 END) AS cs_pg11PLPBrowse_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN entry_sessions ELSE 0 END) AS cs_pg12PrivacyLegal_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN entry_sessions_ly ELSE 0 END) AS cs_pg12PrivacyLegal_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN pspv_sessions ELSE 0 END) AS cs_pg12PrivacyLegal_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg12PrivacyLegal_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN cart_start_sessions ELSE 0 END) AS cs_pg12PrivacyLegal_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg12PrivacyLegal_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN checkout_sessions ELSE 0 END) AS cs_pg12PrivacyLegal_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg12PrivacyLegal_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN order_sessions ELSE 0 END) AS cs_pg12PrivacyLegal_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Privacy / Legal' THEN order_sessions_ly ELSE 0 END) AS cs_pg12PrivacyLegal_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN entry_sessions ELSE 0 END) AS cs_pg13SearchTools_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN entry_sessions_ly ELSE 0 END) AS cs_pg13SearchTools_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN pspv_sessions ELSE 0 END) AS cs_pg13SearchTools_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg13SearchTools_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN cart_start_sessions ELSE 0 END) AS cs_pg13SearchTools_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg13SearchTools_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN checkout_sessions ELSE 0 END) AS cs_pg13SearchTools_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg13SearchTools_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN order_sessions ELSE 0 END) AS cs_pg13SearchTools_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Search / Tools' THEN order_sessions_ly ELSE 0 END) AS cs_pg13SearchTools_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN entry_sessions ELSE 0 END) AS cs_pg14StoreLocator_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN entry_sessions_ly ELSE 0 END) AS cs_pg14StoreLocator_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN pspv_sessions ELSE 0 END) AS cs_pg14StoreLocator_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg14StoreLocator_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN cart_start_sessions ELSE 0 END) AS cs_pg14StoreLocator_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg14StoreLocator_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN checkout_sessions ELSE 0 END) AS cs_pg14StoreLocator_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg14StoreLocator_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN order_sessions ELSE 0 END) AS cs_pg14StoreLocator_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Store / Locator' THEN order_sessions_ly ELSE 0 END) AS cs_pg14StoreLocator_orderSessions_ly,

      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN entry_sessions ELSE 0 END) AS cs_pg15SupportHelp_entrySessions,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN entry_sessions_ly ELSE 0 END) AS cs_pg15SupportHelp_entrySessions_ly,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN pspv_sessions ELSE 0 END) AS cs_pg15SupportHelp_pspvSessions,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN pspv_sessions_ly ELSE 0 END) AS cs_pg15SupportHelp_pspvSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN cart_start_sessions ELSE 0 END) AS cs_pg15SupportHelp_cartStartSessions,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN cart_start_sessions_ly ELSE 0 END) AS cs_pg15SupportHelp_cartStartSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN checkout_sessions ELSE 0 END) AS cs_pg15SupportHelp_checkoutSessions,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN checkout_sessions_ly ELSE 0 END) AS cs_pg15SupportHelp_checkoutSessions_ly,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN order_sessions ELSE 0 END) AS cs_pg15SupportHelp_orderSessions,
      SUM(CASE WHEN entry_page_group = 'Support / Help' THEN order_sessions_ly ELSE 0 END) AS cs_pg15SupportHelp_orderSessions_ly

    FROM clickstream_daily_pagegroup
    GROUP BY
      1, 2, 3
  )

  SELECT
    t.event_date,
    t.date_yyyymmdd,
    t.lob,
    t.last_touch_channel,

    t.entries,
    t.pspv_actuals,
    t.cart_starts,
    COALESCE(c.cart_start_plus, 0) AS cart_start_plus,
    t.cart_checkout_visits,
    t.checkout_review_visits,
    t.postpaid_orders_tsr,

    COALESCE(o.orders_web_unassisted, 0) AS orders_web_unassisted,
    COALESCE(o.orders_web_assisted, 0) AS orders_web_assisted,
    COALESCE(o.orders_app_unassisted, 0) AS orders_app_unassisted,
    COALESCE(o.orders_app_assisted, 0) AS orders_app_assisted,
    COALESCE(o.orders_web_all, 0) AS orders_web_all,
    COALESCE(o.orders_app_all, 0) AS orders_app_all,
    COALESCE(o.orders_fully_unassisted, 0) AS orders_fully_unassisted,
    COALESCE(o.orders_fully_assisted, 0) AS orders_fully_assisted,
    COALESCE(o.orders_all, 0) AS orders_all,

    COALESCE(cs.cs_pg01AccountBillingLogin_entrySessions, 0) AS cs_pg01AccountBillingLogin_entrySessions,
    COALESCE(cs.cs_pg01AccountBillingLogin_entrySessions_ly, 0) AS cs_pg01AccountBillingLogin_entrySessions_ly,
    COALESCE(cs.cs_pg01AccountBillingLogin_pspvSessions, 0) AS cs_pg01AccountBillingLogin_pspvSessions,
    COALESCE(cs.cs_pg01AccountBillingLogin_pspvSessions_ly, 0) AS cs_pg01AccountBillingLogin_pspvSessions_ly,
    COALESCE(cs.cs_pg01AccountBillingLogin_cartStartSessions, 0) AS cs_pg01AccountBillingLogin_cartStartSessions,
    COALESCE(cs.cs_pg01AccountBillingLogin_cartStartSessions_ly, 0) AS cs_pg01AccountBillingLogin_cartStartSessions_ly,
    COALESCE(cs.cs_pg01AccountBillingLogin_checkoutSessions, 0) AS cs_pg01AccountBillingLogin_checkoutSessions,
    COALESCE(cs.cs_pg01AccountBillingLogin_checkoutSessions_ly, 0) AS cs_pg01AccountBillingLogin_checkoutSessions_ly,
    COALESCE(cs.cs_pg01AccountBillingLogin_orderSessions, 0) AS cs_pg01AccountBillingLogin_orderSessions,
    COALESCE(cs.cs_pg01AccountBillingLogin_orderSessions_ly, 0) AS cs_pg01AccountBillingLogin_orderSessions_ly,

    COALESCE(cs.cs_pg02AppTLife_entrySessions, 0) AS cs_pg02AppTLife_entrySessions,
    COALESCE(cs.cs_pg02AppTLife_entrySessions_ly, 0) AS cs_pg02AppTLife_entrySessions_ly,
    COALESCE(cs.cs_pg02AppTLife_pspvSessions, 0) AS cs_pg02AppTLife_pspvSessions,
    COALESCE(cs.cs_pg02AppTLife_pspvSessions_ly, 0) AS cs_pg02AppTLife_pspvSessions_ly,
    COALESCE(cs.cs_pg02AppTLife_cartStartSessions, 0) AS cs_pg02AppTLife_cartStartSessions,
    COALESCE(cs.cs_pg02AppTLife_cartStartSessions_ly, 0) AS cs_pg02AppTLife_cartStartSessions_ly,
    COALESCE(cs.cs_pg02AppTLife_checkoutSessions, 0) AS cs_pg02AppTLife_checkoutSessions,
    COALESCE(cs.cs_pg02AppTLife_checkoutSessions_ly, 0) AS cs_pg02AppTLife_checkoutSessions_ly,
    COALESCE(cs.cs_pg02AppTLife_orderSessions, 0) AS cs_pg02AppTLife_orderSessions,
    COALESCE(cs.cs_pg02AppTLife_orderSessions_ly, 0) AS cs_pg02AppTLife_orderSessions_ly,

    COALESCE(cs.cs_pg03BrandMarketing_entrySessions, 0) AS cs_pg03BrandMarketing_entrySessions,
    COALESCE(cs.cs_pg03BrandMarketing_entrySessions_ly, 0) AS cs_pg03BrandMarketing_entrySessions_ly,
    COALESCE(cs.cs_pg03BrandMarketing_pspvSessions, 0) AS cs_pg03BrandMarketing_pspvSessions,
    COALESCE(cs.cs_pg03BrandMarketing_pspvSessions_ly, 0) AS cs_pg03BrandMarketing_pspvSessions_ly,
    COALESCE(cs.cs_pg03BrandMarketing_cartStartSessions, 0) AS cs_pg03BrandMarketing_cartStartSessions,
    COALESCE(cs.cs_pg03BrandMarketing_cartStartSessions_ly, 0) AS cs_pg03BrandMarketing_cartStartSessions_ly,
    COALESCE(cs.cs_pg03BrandMarketing_checkoutSessions, 0) AS cs_pg03BrandMarketing_checkoutSessions,
    COALESCE(cs.cs_pg03BrandMarketing_checkoutSessions_ly, 0) AS cs_pg03BrandMarketing_checkoutSessions_ly,
    COALESCE(cs.cs_pg03BrandMarketing_orderSessions, 0) AS cs_pg03BrandMarketing_orderSessions,
    COALESCE(cs.cs_pg03BrandMarketing_orderSessions_ly, 0) AS cs_pg03BrandMarketing_orderSessions_ly,

    COALESCE(cs.cs_pg04CartCheckout_entrySessions, 0) AS cs_pg04CartCheckout_entrySessions,
    COALESCE(cs.cs_pg04CartCheckout_entrySessions_ly, 0) AS cs_pg04CartCheckout_entrySessions_ly,
    COALESCE(cs.cs_pg04CartCheckout_pspvSessions, 0) AS cs_pg04CartCheckout_pspvSessions,
    COALESCE(cs.cs_pg04CartCheckout_pspvSessions_ly, 0) AS cs_pg04CartCheckout_pspvSessions_ly,
    COALESCE(cs.cs_pg04CartCheckout_cartStartSessions, 0) AS cs_pg04CartCheckout_cartStartSessions,
    COALESCE(cs.cs_pg04CartCheckout_cartStartSessions_ly, 0) AS cs_pg04CartCheckout_cartStartSessions_ly,
    COALESCE(cs.cs_pg04CartCheckout_checkoutSessions, 0) AS cs_pg04CartCheckout_checkoutSessions,
    COALESCE(cs.cs_pg04CartCheckout_checkoutSessions_ly, 0) AS cs_pg04CartCheckout_checkoutSessions_ly,
    COALESCE(cs.cs_pg04CartCheckout_orderSessions, 0) AS cs_pg04CartCheckout_orderSessions,
    COALESCE(cs.cs_pg04CartCheckout_orderSessions_ly, 0) AS cs_pg04CartCheckout_orderSessions_ly,

    COALESCE(cs.cs_pg05CoverageNetwork_entrySessions, 0) AS cs_pg05CoverageNetwork_entrySessions,
    COALESCE(cs.cs_pg05CoverageNetwork_entrySessions_ly, 0) AS cs_pg05CoverageNetwork_entrySessions_ly,
    COALESCE(cs.cs_pg05CoverageNetwork_pspvSessions, 0) AS cs_pg05CoverageNetwork_pspvSessions,
    COALESCE(cs.cs_pg05CoverageNetwork_pspvSessions_ly, 0) AS cs_pg05CoverageNetwork_pspvSessions_ly,
    COALESCE(cs.cs_pg05CoverageNetwork_cartStartSessions, 0) AS cs_pg05CoverageNetwork_cartStartSessions,
    COALESCE(cs.cs_pg05CoverageNetwork_cartStartSessions_ly, 0) AS cs_pg05CoverageNetwork_cartStartSessions_ly,
    COALESCE(cs.cs_pg05CoverageNetwork_checkoutSessions, 0) AS cs_pg05CoverageNetwork_checkoutSessions,
    COALESCE(cs.cs_pg05CoverageNetwork_checkoutSessions_ly, 0) AS cs_pg05CoverageNetwork_checkoutSessions_ly,
    COALESCE(cs.cs_pg05CoverageNetwork_orderSessions, 0) AS cs_pg05CoverageNetwork_orderSessions,
    COALESCE(cs.cs_pg05CoverageNetwork_orderSessions_ly, 0) AS cs_pg05CoverageNetwork_orderSessions_ly,

    COALESCE(cs.cs_pg06DealsOffers_entrySessions, 0) AS cs_pg06DealsOffers_entrySessions,
    COALESCE(cs.cs_pg06DealsOffers_entrySessions_ly, 0) AS cs_pg06DealsOffers_entrySessions_ly,
    COALESCE(cs.cs_pg06DealsOffers_pspvSessions, 0) AS cs_pg06DealsOffers_pspvSessions,
    COALESCE(cs.cs_pg06DealsOffers_pspvSessions_ly, 0) AS cs_pg06DealsOffers_pspvSessions_ly,
    COALESCE(cs.cs_pg06DealsOffers_cartStartSessions, 0) AS cs_pg06DealsOffers_cartStartSessions,
    COALESCE(cs.cs_pg06DealsOffers_cartStartSessions_ly, 0) AS cs_pg06DealsOffers_cartStartSessions_ly,
    COALESCE(cs.cs_pg06DealsOffers_checkoutSessions, 0) AS cs_pg06DealsOffers_checkoutSessions,
    COALESCE(cs.cs_pg06DealsOffers_checkoutSessions_ly, 0) AS cs_pg06DealsOffers_checkoutSessions_ly,
    COALESCE(cs.cs_pg06DealsOffers_orderSessions, 0) AS cs_pg06DealsOffers_orderSessions,
    COALESCE(cs.cs_pg06DealsOffers_orderSessions_ly, 0) AS cs_pg06DealsOffers_orderSessions_ly,

    COALESCE(cs.cs_pg07Homepage_entrySessions, 0) AS cs_pg07Homepage_entrySessions,
    COALESCE(cs.cs_pg07Homepage_entrySessions_ly, 0) AS cs_pg07Homepage_entrySessions_ly,
    COALESCE(cs.cs_pg07Homepage_pspvSessions, 0) AS cs_pg07Homepage_pspvSessions,
    COALESCE(cs.cs_pg07Homepage_pspvSessions_ly, 0) AS cs_pg07Homepage_pspvSessions_ly,
    COALESCE(cs.cs_pg07Homepage_cartStartSessions, 0) AS cs_pg07Homepage_cartStartSessions,
    COALESCE(cs.cs_pg07Homepage_cartStartSessions_ly, 0) AS cs_pg07Homepage_cartStartSessions_ly,
    COALESCE(cs.cs_pg07Homepage_checkoutSessions, 0) AS cs_pg07Homepage_checkoutSessions,
    COALESCE(cs.cs_pg07Homepage_checkoutSessions_ly, 0) AS cs_pg07Homepage_checkoutSessions_ly,
    COALESCE(cs.cs_pg07Homepage_orderSessions, 0) AS cs_pg07Homepage_orderSessions,
    COALESCE(cs.cs_pg07Homepage_orderSessions_ly, 0) AS cs_pg07Homepage_orderSessions_ly,

    COALESCE(cs.cs_pg08OrderStatus_entrySessions, 0) AS cs_pg08OrderStatus_entrySessions,
    COALESCE(cs.cs_pg08OrderStatus_entrySessions_ly, 0) AS cs_pg08OrderStatus_entrySessions_ly,
    COALESCE(cs.cs_pg08OrderStatus_pspvSessions, 0) AS cs_pg08OrderStatus_pspvSessions,
    COALESCE(cs.cs_pg08OrderStatus_pspvSessions_ly, 0) AS cs_pg08OrderStatus_pspvSessions_ly,
    COALESCE(cs.cs_pg08OrderStatus_cartStartSessions, 0) AS cs_pg08OrderStatus_cartStartSessions,
    COALESCE(cs.cs_pg08OrderStatus_cartStartSessions_ly, 0) AS cs_pg08OrderStatus_cartStartSessions_ly,
    COALESCE(cs.cs_pg08OrderStatus_checkoutSessions, 0) AS cs_pg08OrderStatus_checkoutSessions,
    COALESCE(cs.cs_pg08OrderStatus_checkoutSessions_ly, 0) AS cs_pg08OrderStatus_checkoutSessions_ly,
    COALESCE(cs.cs_pg08OrderStatus_orderSessions, 0) AS cs_pg08OrderStatus_orderSessions,
    COALESCE(cs.cs_pg08OrderStatus_orderSessions_ly, 0) AS cs_pg08OrderStatus_orderSessions_ly,

    COALESCE(cs.cs_pg09Other_entrySessions, 0) AS cs_pg09Other_entrySessions,
    COALESCE(cs.cs_pg09Other_entrySessions_ly, 0) AS cs_pg09Other_entrySessions_ly,
    COALESCE(cs.cs_pg09Other_pspvSessions, 0) AS cs_pg09Other_pspvSessions,
    COALESCE(cs.cs_pg09Other_pspvSessions_ly, 0) AS cs_pg09Other_pspvSessions_ly,
    COALESCE(cs.cs_pg09Other_cartStartSessions, 0) AS cs_pg09Other_cartStartSessions,
    COALESCE(cs.cs_pg09Other_cartStartSessions_ly, 0) AS cs_pg09Other_cartStartSessions_ly,
    COALESCE(cs.cs_pg09Other_checkoutSessions, 0) AS cs_pg09Other_checkoutSessions,
    COALESCE(cs.cs_pg09Other_checkoutSessions_ly, 0) AS cs_pg09Other_checkoutSessions_ly,
    COALESCE(cs.cs_pg09Other_orderSessions, 0) AS cs_pg09Other_orderSessions,
    COALESCE(cs.cs_pg09Other_orderSessions_ly, 0) AS cs_pg09Other_orderSessions_ly,

    COALESCE(cs.cs_pg10PDPDetail_entrySessions, 0) AS cs_pg10PDPDetail_entrySessions,
    COALESCE(cs.cs_pg10PDPDetail_entrySessions_ly, 0) AS cs_pg10PDPDetail_entrySessions_ly,
    COALESCE(cs.cs_pg10PDPDetail_pspvSessions, 0) AS cs_pg10PDPDetail_pspvSessions,
    COALESCE(cs.cs_pg10PDPDetail_pspvSessions_ly, 0) AS cs_pg10PDPDetail_pspvSessions_ly,
    COALESCE(cs.cs_pg10PDPDetail_cartStartSessions, 0) AS cs_pg10PDPDetail_cartStartSessions,
    COALESCE(cs.cs_pg10PDPDetail_cartStartSessions_ly, 0) AS cs_pg10PDPDetail_cartStartSessions_ly,
    COALESCE(cs.cs_pg10PDPDetail_checkoutSessions, 0) AS cs_pg10PDPDetail_checkoutSessions,
    COALESCE(cs.cs_pg10PDPDetail_checkoutSessions_ly, 0) AS cs_pg10PDPDetail_checkoutSessions_ly,
    COALESCE(cs.cs_pg10PDPDetail_orderSessions, 0) AS cs_pg10PDPDetail_orderSessions,
    COALESCE(cs.cs_pg10PDPDetail_orderSessions_ly, 0) AS cs_pg10PDPDetail_orderSessions_ly,

    COALESCE(cs.cs_pg11PLPBrowse_entrySessions, 0) AS cs_pg11PLPBrowse_entrySessions,
    COALESCE(cs.cs_pg11PLPBrowse_entrySessions_ly, 0) AS cs_pg11PLPBrowse_entrySessions_ly,
    COALESCE(cs.cs_pg11PLPBrowse_pspvSessions, 0) AS cs_pg11PLPBrowse_pspvSessions,
    COALESCE(cs.cs_pg11PLPBrowse_pspvSessions_ly, 0) AS cs_pg11PLPBrowse_pspvSessions_ly,
    COALESCE(cs.cs_pg11PLPBrowse_cartStartSessions, 0) AS cs_pg11PLPBrowse_cartStartSessions,
    COALESCE(cs.cs_pg11PLPBrowse_cartStartSessions_ly, 0) AS cs_pg11PLPBrowse_cartStartSessions_ly,
    COALESCE(cs.cs_pg11PLPBrowse_checkoutSessions, 0) AS cs_pg11PLPBrowse_checkoutSessions,
    COALESCE(cs.cs_pg11PLPBrowse_checkoutSessions_ly, 0) AS cs_pg11PLPBrowse_checkoutSessions_ly,
    COALESCE(cs.cs_pg11PLPBrowse_orderSessions, 0) AS cs_pg11PLPBrowse_orderSessions,
    COALESCE(cs.cs_pg11PLPBrowse_orderSessions_ly, 0) AS cs_pg11PLPBrowse_orderSessions_ly,

    COALESCE(cs.cs_pg12PrivacyLegal_entrySessions, 0) AS cs_pg12PrivacyLegal_entrySessions,
    COALESCE(cs.cs_pg12PrivacyLegal_entrySessions_ly, 0) AS cs_pg12PrivacyLegal_entrySessions_ly,
    COALESCE(cs.cs_pg12PrivacyLegal_pspvSessions, 0) AS cs_pg12PrivacyLegal_pspvSessions,
    COALESCE(cs.cs_pg12PrivacyLegal_pspvSessions_ly, 0) AS cs_pg12PrivacyLegal_pspvSessions_ly,
    COALESCE(cs.cs_pg12PrivacyLegal_cartStartSessions, 0) AS cs_pg12PrivacyLegal_cartStartSessions,
    COALESCE(cs.cs_pg12PrivacyLegal_cartStartSessions_ly, 0) AS cs_pg12PrivacyLegal_cartStartSessions_ly,
    COALESCE(cs.cs_pg12PrivacyLegal_checkoutSessions, 0) AS cs_pg12PrivacyLegal_checkoutSessions,
    COALESCE(cs.cs_pg12PrivacyLegal_checkoutSessions_ly, 0) AS cs_pg12PrivacyLegal_checkoutSessions_ly,
    COALESCE(cs.cs_pg12PrivacyLegal_orderSessions, 0) AS cs_pg12PrivacyLegal_orderSessions,
    COALESCE(cs.cs_pg12PrivacyLegal_orderSessions_ly, 0) AS cs_pg12PrivacyLegal_orderSessions_ly,

    COALESCE(cs.cs_pg13SearchTools_entrySessions, 0) AS cs_pg13SearchTools_entrySessions,
    COALESCE(cs.cs_pg13SearchTools_entrySessions_ly, 0) AS cs_pg13SearchTools_entrySessions_ly,
    COALESCE(cs.cs_pg13SearchTools_pspvSessions, 0) AS cs_pg13SearchTools_pspvSessions,
    COALESCE(cs.cs_pg13SearchTools_pspvSessions_ly, 0) AS cs_pg13SearchTools_pspvSessions_ly,
    COALESCE(cs.cs_pg13SearchTools_cartStartSessions, 0) AS cs_pg13SearchTools_cartStartSessions,
    COALESCE(cs.cs_pg13SearchTools_cartStartSessions_ly, 0) AS cs_pg13SearchTools_cartStartSessions_ly,
    COALESCE(cs.cs_pg13SearchTools_checkoutSessions, 0) AS cs_pg13SearchTools_checkoutSessions,
    COALESCE(cs.cs_pg13SearchTools_checkoutSessions_ly, 0) AS cs_pg13SearchTools_checkoutSessions_ly,
    COALESCE(cs.cs_pg13SearchTools_orderSessions, 0) AS cs_pg13SearchTools_orderSessions,
    COALESCE(cs.cs_pg13SearchTools_orderSessions_ly, 0) AS cs_pg13SearchTools_orderSessions_ly,

    COALESCE(cs.cs_pg14StoreLocator_entrySessions, 0) AS cs_pg14StoreLocator_entrySessions,
    COALESCE(cs.cs_pg14StoreLocator_entrySessions_ly, 0) AS cs_pg14StoreLocator_entrySessions_ly,
    COALESCE(cs.cs_pg14StoreLocator_pspvSessions, 0) AS cs_pg14StoreLocator_pspvSessions,
    COALESCE(cs.cs_pg14StoreLocator_pspvSessions_ly, 0) AS cs_pg14StoreLocator_pspvSessions_ly,
    COALESCE(cs.cs_pg14StoreLocator_cartStartSessions, 0) AS cs_pg14StoreLocator_cartStartSessions,
    COALESCE(cs.cs_pg14StoreLocator_cartStartSessions_ly, 0) AS cs_pg14StoreLocator_cartStartSessions_ly,
    COALESCE(cs.cs_pg14StoreLocator_checkoutSessions, 0) AS cs_pg14StoreLocator_checkoutSessions,
    COALESCE(cs.cs_pg14StoreLocator_checkoutSessions_ly, 0) AS cs_pg14StoreLocator_checkoutSessions_ly,
    COALESCE(cs.cs_pg14StoreLocator_orderSessions, 0) AS cs_pg14StoreLocator_orderSessions,
    COALESCE(cs.cs_pg14StoreLocator_orderSessions_ly, 0) AS cs_pg14StoreLocator_orderSessions_ly,

    COALESCE(cs.cs_pg15SupportHelp_entrySessions, 0) AS cs_pg15SupportHelp_entrySessions,
    COALESCE(cs.cs_pg15SupportHelp_entrySessions_ly, 0) AS cs_pg15SupportHelp_entrySessions_ly,
    COALESCE(cs.cs_pg15SupportHelp_pspvSessions, 0) AS cs_pg15SupportHelp_pspvSessions,
    COALESCE(cs.cs_pg15SupportHelp_pspvSessions_ly, 0) AS cs_pg15SupportHelp_pspvSessions_ly,
    COALESCE(cs.cs_pg15SupportHelp_cartStartSessions, 0) AS cs_pg15SupportHelp_cartStartSessions,
    COALESCE(cs.cs_pg15SupportHelp_cartStartSessions_ly, 0) AS cs_pg15SupportHelp_cartStartSessions_ly,
    COALESCE(cs.cs_pg15SupportHelp_checkoutSessions, 0) AS cs_pg15SupportHelp_checkoutSessions,
    COALESCE(cs.cs_pg15SupportHelp_checkoutSessions_ly, 0) AS cs_pg15SupportHelp_checkoutSessions_ly,
    COALESCE(cs.cs_pg15SupportHelp_orderSessions, 0) AS cs_pg15SupportHelp_orderSessions,
    COALESCE(cs.cs_pg15SupportHelp_orderSessions_ly, 0) AS cs_pg15SupportHelp_orderSessions_ly

  FROM tsr_daily t
  LEFT JOIN orders_daily o
    ON t.event_date = o.date
   AND t.lob = o.lob
   AND t.channel_raw_upper = o.channel_raw_upper
  LEFT JOIN cart_start_plus_daily c
    ON t.event_date = c.date
   AND t.lob = c.lob
   AND t.channel_raw_upper = c.channel_raw_upper
  LEFT JOIN clickstream_daily_pivot cs
    ON t.event_date = cs.event_date
   AND t.lob = cs.lob
   AND t.channel_raw_upper = cs.channel_raw_upper
  ;

END;