/* =================================================================================================
FILE: 01_vw_sdi_adobe_gold_adobeCartStartPlus_daily.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_gold_adobeCartStartPlus_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_adobeCartStartPlus_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week

PURPOSE:
  Final Gold daily fact view for Adobe Postpaid Cart Start Plus data, enriched with:
    - lob
    - daily reporting date
    - qgp_week
    - last touch channel
    - cart_start_plus

BUSINESS GRAIN:
  date
  + lob
  + last_touch_channel

WHY CHANNEL IS INCLUDED:
  This supports:
  - channel-level daily funnel reporting
  - reconciliation against Adobe and clickstream channel-based datasets
  - downstream weekly rollups by channel
  - future channel hierarchy mapping if needed

QGP WEEK LOGIC:
  qgp_week =
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date)

NOTES:
  - lob is fixed as POSTPAID because the source table is the Postpaid Cart Start Plus feed.
  - qgp_week is attached at the daily level so downstream weekly rollups remain consistent with
    the shared QGP logic.
  - This is the final daily reporting layer for Adobe Cart Start Plus.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily` AS

SELECT
  date,
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date) AS qgp_week,
  'POSTPAID' AS lob,
  last_touch_channel,
  cart_start_plus
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_adobeCartStartPlus_daily`;