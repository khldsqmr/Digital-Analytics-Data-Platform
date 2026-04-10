/* =================================================================================================
FILE: 13_vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_prospect_customer_web_app_da_all_postpaid_apps_visits_da_enterprise_prospect_visits_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily

PURPOSE:
  Canonical Bronze Adobe T-Life App Visits daily view for the Total Search Dashboard.
  This view deduplicates raw Adobe T-Life App Visits and standardizes them to:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

METRICS INCLUDED:
  - adobeTLifeAppVisits

DEDUPE LOGIC:
  Latest row per:
      account_id + date_yyyymmdd + last_touch_channel
  ordered by:
      File_Load_datetime DESC,
      Filename DESC,
      __insert_date DESC

KEY MODELING NOTES:
  - LOB is standardized as UPPER(TRIM('Postpaid'))
  - Channel is standardized from Adobe last_touch_channel
  - NATURAL SEARCH remains source-close in Bronze and is conformed in Silver
  - Dedupe is applied before outputting the reporting grain to avoid snapshot double counting
  - visits from source is standardized as adobeTLifeAppVisits

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily`
AS

WITH ranked AS (
    SELECT
        raw.account_id,
        raw.account_name,
        PARSE_DATE('%Y%m%d', CAST(raw.date_yyyymmdd AS STRING)) AS event_date,
        CAST(raw.date_yyyymmdd AS STRING) AS date_yyyymmdd,
        UPPER(TRIM(raw.last_touch_channel)) AS channel,

        SAFE_CAST(raw.visits AS FLOAT64) AS adobeTLifeAppVisits,
        raw.segments_id,

        SAFE_CAST(raw.__insert_date AS INT64) AS insert_date,
        TIMESTAMP(raw.File_Load_datetime) AS file_load_datetime,
        raw.Filename AS filename,

        ROW_NUMBER() OVER (
            PARTITION BY
                raw.account_id,
                CAST(raw.date_yyyymmdd AS STRING),
                UPPER(TRIM(raw.last_touch_channel))
            ORDER BY
                TIMESTAMP(raw.File_Load_datetime) DESC,
                raw.Filename DESC,
                SAFE_CAST(raw.__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_prospect_customer_web_app_da_all_postpaid_apps_visits_da_enterprise_prospect_visits_tmo` raw
    WHERE raw.date_yyyymmdd IS NOT NULL
      AND raw.last_touch_channel IS NOT NULL
      AND raw.visits IS NOT NULL
)

SELECT
    event_date,
    UPPER(TRIM('Postpaid')) AS lob,
    channel,
    adobeTLifeAppVisits
FROM ranked
WHERE rn = 1;