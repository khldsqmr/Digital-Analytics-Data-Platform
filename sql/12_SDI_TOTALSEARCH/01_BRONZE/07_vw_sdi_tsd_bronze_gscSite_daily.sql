/* =================================================================================================
FILE: 07_vw_sdi_tsd_bronze_gscSite_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_gscSite_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_gsc_site_daily

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gscSite_daily

PURPOSE:
  Canonical Bronze GSC site-level daily view for the Total Search Dashboard.
  This view deduplicates site-level Google Search Console daily totals for reconciliation
  and validation purposes.

BUSINESS GRAIN:
  One row per:
      account_id
      site_url
      event_date

DEDUPE LOGIC:
  Latest row per:
      account_id + site_url + date_yyyymmdd
  ordered by:
      file_load_datetime DESC,
      filename DESC,
      __insert_date DESC

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_gscSite_daily`
AS

WITH ranked AS (
    SELECT
        raw.account_id,
        raw.account_name,
        raw.site_url,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        DATE(raw.date) AS event_date,

        SAFE_CAST(raw.clicks AS FLOAT64) AS clicks,
        SAFE_CAST(raw.impressions AS FLOAT64) AS impressions,
        SAFE_CAST(raw.sum_position AS FLOAT64) AS sum_position,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        TIMESTAMP(raw.file_load_datetime) AS file_load_datetime,
        raw.filename AS filename,

        ROW_NUMBER() OVER (
            PARTITION BY
                raw.account_id,
                raw.site_url,
                CAST(raw.date_yyyymmdd AS STRING)
            ORDER BY
                TIMESTAMP(raw.file_load_datetime) DESC,
                raw.filename DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_gsc_site_daily` raw
)

SELECT
    account_id,
    account_name,
    site_url,
    date_yyyymmdd,
    event_date,
    clicks,
    impressions,
    sum_position,
    insert_date,
    file_load_datetime,
    filename
FROM ranked
WHERE rn = 1;