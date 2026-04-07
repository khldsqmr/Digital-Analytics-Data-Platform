/* =================================================================================================
FILE: 01_vw_sdi_tsd_bronze_adobeV2_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_adobeV2_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeV2_daily

PURPOSE:
  Canonical Bronze Adobe V2 daily view for the Total Search Dashboard.
  This view standardizes Adobe Postpaid funnel metrics to the reporting grain:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

METRICS INCLUDED:
  - adobe_entries
  - adobe_pspv_actuals
  - adobe_cart_starts
  - adobe_cart_checkout_visits
  - adobe_checkout_review_visits
  - adobe_postpaid_orders_tsr

KEY MODELING NOTES:
  - LOB is standardized to 'Postpaid'
  - Channel is standardized from Adobe last_touch_channel
  - This object is source-close and does not join to other Adobe sources

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeV2_daily`
AS

SELECT
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS event_date,
    'Postpaid' AS lob,
    UPPER(TRIM(last_touch_channel)) AS channel,

    SUM(COALESCE(visits_enterprise_prospect_visits, 0))            AS adobe_entries,
    SUM(COALESCE(postpaid_voice_prospect_shop_page_visits, 0))     AS adobe_pspv_actuals,
    SUM(COALESCE(postpaid_voice_prospect_cart_start_visits, 0))    AS adobe_cart_starts,
    SUM(COALESCE(postpaid_voice_prospect_cart_checkout_visits, 0)) AS adobe_cart_checkout_visits,
    SUM(COALESCE(postpaid_voice_prospect_checkout_4_0_review, 0))  AS adobe_checkout_review_visits,
    SUM(COALESCE(postpaid_voice_prospect_orders, 0))               AS adobe_postpaid_orders_tsr

FROM `prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo`
WHERE last_touch_channel IS NOT NULL
GROUP BY
    event_date,
    lob,
    channel;