/* =================================================================================================
FILE: 03_vw_sdi_adobe_gold_adobeCartStartPlus_weeklySum.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_gold_adobeCartStartPlus_weeklySum

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week

PURPOSE:
  Final Gold weekly fact view for Adobe Postpaid Cart Start Plus data using QGP week rollup,
  aggregated to total weekly Postpaid values without channel breakdown, including:
    - qgp_week
    - lob
    - weekly cart_start_plus

BUSINESS GRAIN:
  qgp_week
  + lob

WHY THIS VIEW EXISTS:
  This supports:
  - topline weekly Cart Start Plus trend reporting
  - simpler KPI reporting without channel splits
  - easier joins to other weekly topline funnel datasets
  - executive/summary reporting use cases

QGP WEEK LOGIC:
  qgp_week =
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(date)

WEEKLY ROLLUP LOGIC:
  Weekly cart_start_plus is calculated by summing daily cart_start_plus values that share the same
  qgp_week and lob.

NOTES:
  - qgp_week is the final reporting period end date.
  - For quarter tail days, qgp_week may equal quarter_end instead of Saturday, based on the shared
    QGP function.
  - This is the final weekly topline reporting output for Adobe Cart Start Plus.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_weeklySum` AS

SELECT
  qgp_week,
  lob,
  SUM(cart_start_plus) AS cart_start_plus
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_gold_adobeCartStartPlus_daily`
GROUP BY
  1, 2;