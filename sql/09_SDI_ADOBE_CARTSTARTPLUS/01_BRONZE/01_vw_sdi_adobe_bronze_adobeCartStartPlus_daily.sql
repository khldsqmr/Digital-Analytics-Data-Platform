/* =================================================================================================
FILE: 01_vw_sdi_adobe_bronze_adobeCartStartPlus_daily.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_bronze_adobeCartStartPlus_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_improvado_master.adobe_cs_day_tmo

PURPOSE:
  Canonical Bronze daily view for Adobe Postpaid Cart Start Plus data, standardizing the source into
  a clean daily structure with:
    - canonical event date
    - last touch channel
    - cart_start_plus metric
    - raw source lineage fields

BUSINESS GRAIN:
  event_date
  + last_touch_channel

WHY THIS BRONZE VIEW EXISTS:
  This view:
  - converts the raw Adobe date field into a canonical DATE column
  - renames event190 into a business-friendly metric name: cart_start_plus
  - standardizes last_touch_channel formatting
  - preserves important source lineage fields for traceability
  - deduplicates multiple source rows for the same date + channel using latest file logic

DEDUP LOGIC:
  For each:
    - date_yyyymmdd
    - last_touch_channel
  keep only the latest record based on:
    1. File_Load_datetime descending
    2. Filename descending
    3. __insert_date descending

NOTES:
  - event190 is being treated as cart_start_plus based on source table naming and file naming convention.
  - This Bronze view preserves source-level metadata for auditability and downstream validation.
  - This is the canonical daily Adobe Cart Start Plus source before Silver aggregation.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_adobeCartStartPlus_daily` AS

WITH ranked AS (
  SELECT
    account_id,
    account_name,
    PARSE_DATE('%Y%m%d', CAST(date_yyyymmdd AS STRING)) AS event_date,
    CAST(date_yyyymmdd AS STRING) AS date_yyyymmdd,
    last_touch_channel,
    SAFE_CAST(event190 AS FLOAT64) AS cart_start_plus,
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
)

SELECT
  account_id,
  account_name,
  event_date,
  date_yyyymmdd,
  UPPER(TRIM(last_touch_channel)) AS last_touch_channel,
  cart_start_plus,
  segments_id,
  __insert_date,
  file_load_datetime,
  Filename
FROM ranked
WHERE rn = 1;