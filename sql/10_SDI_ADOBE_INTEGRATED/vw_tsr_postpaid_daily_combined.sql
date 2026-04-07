/* =================================================================================================
FILE: vw_tsr_postpaid_daily_combined.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_tsr_postpaid_daily_combined

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily

PURPOSE:
  Daily combined Postpaid Adobe funnel + orders + cart_start_plus view at date + channel grain.

BUSINESS GRAIN:
  event_date
  + lob
  + last_touch_channel
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_tsr_postpaid_daily_combined` AS

WITH tsr_daily AS (
  SELECT
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS event_date,
    date_yyyymmdd,
    'POSTPAID' AS lob,
    last_touch_channel,
    COALESCE(NULLIF(UPPER(TRIM(last_touch_channel)), ''), 'NONE') AS channel_raw_upper,

    COALESCE(visits_enterprise_prospect_visits, 0) AS entries,
    COALESCE(postpaid_voice_prospect_shop_page_visits, 0) AS pspv_actuals,
    COALESCE(postpaid_voice_prospect_cart_start_visits, 0) AS cart_starts,
    COALESCE(postpaid_voice_prospect_cart_checkout_visits, 0) AS cart_checkout_visits,
    COALESCE(postpaid_voice_prospect_checkout_4_0_review, 0) AS checkout_review_visits,
    COALESCE(postpaid_voice_prospect_orders, 0) AS postpaid_orders_tsr
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
    AND PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
),

orders_daily AS (
  SELECT
    date,
    date_yyyymmdd,
    lob,
    last_touch_channel,
    COALESCE(NULLIF(UPPER(TRIM(last_touch_channel)), ''), 'NONE') AS channel_raw_upper,

    COALESCE(orders_web_unassisted, 0) AS orders_web_unassisted,
    COALESCE(orders_web_assisted, 0) AS orders_web_assisted,
    COALESCE(orders_app_unassisted, 0) AS orders_app_unassisted,
    COALESCE(orders_app_assisted, 0) AS orders_app_assisted,
    COALESCE(orders_web_all, 0) AS orders_web_all,
    COALESCE(orders_app_all, 0) AS orders_app_all,
    COALESCE(orders_fully_unassisted, 0) AS orders_fully_unassisted,
    COALESCE(orders_fully_assisted, 0) AS orders_fully_assisted,
    COALESCE(orders_all, 0) AS orders_all
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily`
),

cart_start_plus_daily AS (
  SELECT
    date,
    lob,
    last_touch_channel,
    COALESCE(NULLIF(UPPER(TRIM(last_touch_channel)), ''), 'NONE') AS channel_raw_upper,
    SUM(cart_start_plus) AS cart_start_plus
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily`
  GROUP BY
    1, 2, 3, 4
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
  COALESCE(o.orders_all, 0) AS orders_all

FROM tsr_daily t
LEFT JOIN orders_daily o
  ON t.event_date = o.date
 AND t.lob = o.lob
 AND t.channel_raw_upper = o.channel_raw_upper
LEFT JOIN cart_start_plus_daily c
  ON t.event_date = c.date
 AND t.lob = c.lob
 AND t.channel_raw_upper = c.channel_raw_upper;