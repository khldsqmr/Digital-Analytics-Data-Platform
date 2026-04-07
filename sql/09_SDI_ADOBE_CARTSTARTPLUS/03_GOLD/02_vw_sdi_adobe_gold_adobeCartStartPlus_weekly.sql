/* =================================================================================================
FILE: 02_vw_sdi_adobe_gold_adobeCartStartPlus_weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_gold_adobeCartStartPlus_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week

PURPOSE:
  Final Gold weekly fact view for Adobe Postpaid Cart Start Plus data using QGP week rollup,
  including:
    - qgp_week
    - lob
    - last touch channel
    - weekly cart_start_plus

BUSINESS GRAIN:
  qgp_week
  + lob
  + last_touch_channel

WHY CHANNEL IS INCLUDED:
  This supports:
  - channel-level weekly funnel reporting
  - trend analysis by channel
  - reconciliation to other weekly Adobe and clickstream outputs
  - future channel hierarchy or tactic mapping downstream if required

QGP WEEK LOGIC:
  qgp_week =
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date)

WEEKLY ROLLUP LOGIC:
  Weekly cart_start_plus is calculated by summing daily cart_start_plus values that share the same
  qgp_week, lob, and last_touch_channel.

NOTES:
  - qgp_week is the final reporting period end date.
  - For quarter tail days, qgp_week may equal quarter_end instead of Saturday, based on the shared
    QGP function.
  - This is the final weekly reporting output for Adobe Cart Start Plus.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_weekly` AS

SELECT
  qgp_week,
  lob,
  last_touch_channel,
  SUM(cart_start_plus) AS cart_start_plus
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily`
GROUP BY
  1, 2, 3;