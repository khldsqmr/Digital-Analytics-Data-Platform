/* =================================================================================================
FILE: 01_vw_sdi_adobe_silver_adobeCartStartPlus_daily.sql
LAYER: Silver View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_silver_adobeCartStartPlus_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_adobeCartStartPlus_daily

PURPOSE:
  Clean Silver daily view for Adobe Postpaid Cart Start Plus data, exposing the business-ready
  fields needed for downstream joins, reporting, and weekly rollups:
    - date
    - last_touch_channel
    - cart_start_plus

BUSINESS GRAIN:
  date
  + last_touch_channel

WHY CHANNEL IS INCLUDED:
  This supports:
  - channel-level funnel reporting
  - reconciliation against other Adobe and clickstream channel-based datasets
  - later weekly rollups by channel
  - downstream mapping into higher-level channel hierarchies if needed

SILVER LOGIC:
  This view:
  - uses the canonical Bronze source
  - keeps only the core business fields needed downstream
  - aggregates cart_start_plus at date + channel grain to guarantee uniqueness

NOTES:
  - date is the canonical reporting date derived from the raw Adobe date field.
  - cart_start_plus is the business-friendly renamed version of event190.
  - This Silver view is intended to serve as the clean daily source for any Gold weekly Cart Start Plus views.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_adobeCartStartPlus_daily` AS

SELECT
  event_date AS date,
  last_touch_channel,
  SUM(cart_start_plus) AS cart_start_plus
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_adobeCartStartPlus_daily`
GROUP BY
  1, 2;