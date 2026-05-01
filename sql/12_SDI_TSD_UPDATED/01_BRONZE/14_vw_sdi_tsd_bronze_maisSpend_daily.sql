/* =================================================================================================
FILE: 14_vw_sdi_tsd_bronze_maisSpend_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_maisSpend_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_marketing.media_analytics_integrated_summary

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_maisSpend_daily

PURPOSE:
  Canonical Bronze spend daily view for the Total Search Dashboard sourced from
  media_analytics_integrated_summary. This is the complete and authoritative spend
  source covering all SA360 accounts including adMarketplace (~$19M/year for Postpaid).
  Channels are preserved exactly as-is from the source with no mapping applied.

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

LOBs IN SOURCE:
  POSTPAID  -- active, flowing to Silver
  BROADBAND -- present in source, filtered in Silver until ready
  PREPAID   -- present in source, filtered in Silver until ready
  TFB       -- present in source, filtered in Silver until ready

KEY MODELING NOTES:
  - All LOBs brought in at Bronze, LOB filtering happens in Silver
  - Channels preserved as-is after UPPER(TRIM()), no mapping applied
  - No channel mapping at this layer
  - Covers Dec 2022 onwards
  - adMarketplace (~$19M/year) is included here but missing from
    vw_sdi_tsd_bronze_platformSpend_daily (agg table source)
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_maisSpend_daily`
AS
SELECT
    date                                AS event_date,
    UPPER(TRIM(lob))                    AS lob,
    UPPER(TRIM(channel))                AS channel,
    ROUND(SUM(spend), 2)                AS mais_platform_spend
FROM `prj-dbi-prd-1.ds_dbi_marketing.media_analytics_integrated_summary`
WHERE date IS NOT NULL
  AND lob IS NOT NULL
  AND channel IS NOT NULL
  AND spend > 0
  AND spend IS NOT NULL
GROUP BY 1, 2, 3;