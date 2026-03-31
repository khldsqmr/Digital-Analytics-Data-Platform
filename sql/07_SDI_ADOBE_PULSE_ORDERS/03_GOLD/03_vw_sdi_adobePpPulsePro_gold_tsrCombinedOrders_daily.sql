/* =================================================================================================
FILE: 03_vw_sdi_adobePpPulsePro_gold_tsrCombinedOrders_daily.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobePpPulsePro_gold_tsrCombinedOrders_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily

PURPOSE:
  Combined daily Postpaid TSR + Orders view using:
  - all TSR rows
  - matching orders rows only
  - join on event_date and UPPER(last_touch_channel)

BUSINESS GRAIN:
  event_date + last_touch_channel
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_tsrCombinedOrders_daily`
AS

WITH tsr_daily AS (
  SELECT
    PARSE_DATE('%Y%m%d', date_yyyymmdd) AS event_date,
    date_yyyymmdd,
    'POSTPAID' AS lob,
    last_touch_channel,
    UPPER(last_touch_channel) AS channel_raw_upper,

    COALESCE(visits_enterprise_prospect_visits, 0) AS entries,
    COALESCE(postpaid_voice_prospect_shop_page_visits, 0) AS pspv_actuals,
    COALESCE(postpaid_voice_prospect_orders, 0) AS postpaid_orders_tsr

  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo`
  WHERE (
      visits_enterprise_prospect_visits IS NOT NULL
      OR postpaid_voice_prospect_shop_page_visits IS NOT NULL
      OR postpaid_voice_prospect_orders IS NOT NULL
  )
    AND last_touch_channel IS NOT NULL
    AND PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
),

orders_daily AS (
  SELECT
    date as event_date,
    date_yyyymmdd,
    lob,
    last_touch_channel,
    UPPER(last_touch_channel) AS channel_raw_upper,

    orders_web_unassisted,
    orders_web_assisted,
    orders_app_unassisted,
    orders_app_assisted,
    orders_web_all,
    orders_app_all,
    orders_fully_unassisted,
    orders_fully_assisted,
    orders_all,

    file_load_datetime

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily`
)

SELECT
  t.event_date,
  t.date_yyyymmdd,
  t.lob,
  t.last_touch_channel,

  t.entries,
  t.pspv_actuals,
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

  o.file_load_datetime

FROM tsr_daily t
LEFT JOIN orders_daily o
  ON t.event_date = o.event_date
 AND t.channel_raw_upper = o.channel_raw_upper AND t.lob = o.lob;