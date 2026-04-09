/* =================================================================================================
FILE: 08_vw_sdi_tsd_bronze_platformSpend_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_platformSpend_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_marketing.agg_day_media_and_outcomes

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_platformSpend_daily

PURPOSE:
  Canonical Bronze platform spend daily view for the Total Search Dashboard.
  This view standardizes Postpaid daily spend from agg_day_media_and_outcomes
  at the source reporting grain:
      event_date + lob + channel_raw

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel_raw

KEY MODELING NOTES:
  - Only POSTPAID LOB is included
  - channel_raw is preserved in uppercase trimmed form for downstream conformance
  - spend is aggregated at day + lob + channel_raw
  - This is a source-close Bronze object and does not yet apply TSD conformed channel mapping

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_platformSpend_daily`
AS

SELECT
    DATE(day) AS event_date,
    UPPER(TRIM(lob)) AS lob,
    UPPER(TRIM(channel_name)) AS channel_raw,
    SUM(COALESCE(spend, 0)) AS spend
FROM `prj-dbi-prd-1.ds_dbi_marketing.agg_day_media_and_outcomes`
WHERE DATE(day) IS NOT NULL
  AND UPPER(TRIM(lob)) = 'POSTPAID'
  AND channel_name IS NOT NULL
GROUP BY
    event_date,
    lob,
    channel_raw;