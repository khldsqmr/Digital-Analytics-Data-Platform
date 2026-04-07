/* =================================================================================================
FILE: 03_vw_sdi_tsd_bronze_adobeCartStartPlus_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_bronze_adobeCartStartPlus_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_cs_day_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily

PURPOSE:
  Canonical Bronze Adobe Cart Start Plus daily view for the Total Search Dashboard.
  This view standardizes the Adobe Cart Start Plus metric to the reporting grain:
      event_date + lob + channel

BUSINESS GRAIN:
  One row per:
      event_date
      lob
      channel

SOURCE LOGIC:
  - Cart Start Plus is sourced from event190
  - Latest record is selected per:
      date_yyyymmdd + UPPER(TRIM(last_touch_channel))
  - Latest record determination uses:
      File_Load_datetime DESC,
      Filename DESC,
      __insert_date DESC

KEY MODELING NOTES:
  - LOB is standardized to 'Postpaid'
  - Channel is standardized from last_touch_channel
  - Deduplication is applied before outputting the reporting grain

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_bronze_adobeCartStartPlus_daily`
AS

WITH ranked AS (
    SELECT
        account_id,
        account_name,
        PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS event_date,
        CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
        UPPER(TRIM(last_touch_channel)) AS channel,
        SAFE_CAST(event190 AS FLOAT64) AS adobe_cart_start_plus,
        segments_id,
        SAFE_CAST(__insert_date AS INT64) AS __insert_date,
        TIMESTAMP(File_Load_datetime) AS file_load_datetime,
        Filename,
        ROW_NUMBER() OVER (
            PARTITION BY
                CAST(date_yyyymmdd AS STRING),
                UPPER(TRIM(last_touch_channel))
            ORDER BY
                TIMESTAMP(File_Load_datetime) DESC,
                Filename DESC,
                SAFE_CAST(__insert_date AS INT64) DESC
        ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.adobe_cs_day_tmo`
    WHERE event190 IS NOT NULL
      AND last_touch_channel IS NOT NULL
)

SELECT
    event_date,
    'Postpaid' AS lob,
    channel,
    adobe_cart_start_plus
FROM ranked
WHERE rn = 1;