/* =================================================================================================
FILE:         04_sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly.sql   (Databricks port)
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly.
  Called as part of the weekly refresh.

  Aggregates daily platform spend from agg_day_media_and_outcomes to weekly grain.
  week_sun_sat = week-ending Saturday (Sun-Sat week).
  All date attributes (quarter, week_type, etc.) resolved downstream by joining
  to sdi_vw_dashboardPulseTms_dim_qgp_calendar on week_sun_sat = qgp_date.

  Platform spend is actuals only -- no forecast column available from this source.

CHANNEL GROUPS (standard vocabulary):
  'Paid Search' | 'Paid Social' | 'Programmatic' | 'Other'
  (Organic Search and Direct are Adobe-only traffic attribution categories)

LOB CANONICAL VALUES:
  'POSTPAID'  -- source: 'POSTPAID'
  'BROADBAND' -- source: 'HSI', 'BROADBAND'
  Other LOBs passed through as-is (can be expanded via canonical mapping in Silver/Gold)

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - SAFE_CAST(x AS FLOAT64)      -> TRY_CAST(x AS DOUBLE)
  - EXTRACT(DAYOFWEEK FROM x)    -> unchanged; Databricks DAYOFWEEK is Sunday=1..Saturday=7, identical to BigQuery
  - DATE_ADD(x, INTERVAL n DAY)  -> date_add(x, n)  -- Databricks date_add takes a plain integer day count
  - OPTIONS(strict_mode=false)   -> dropped; BQ-scripting-only setting, no data-semantic effect, no Databricks equivalent
  - CREATE TABLE ... PARTITION BY x CLUSTER BY y ... OPTIONS(description=...)
                                  -> CREATE TABLE ... CLUSTER BY (x, y, z) COMMENT '...'
  - CREATE OR REPLACE PROCEDURE ... BEGIN...END -> needs explicit LANGUAGE SQL clause

⚠ STILL OPEN: <raw_catalog>.<raw_schema> below -- the Databricks address for the raw
  agg_day_media_and_outcomes source table. Everything else in this pipeline now deploys to
  prdrzranalytics.lab42, but that's PulseTMS's own target catalog/schema, not necessarily where
  this particular raw source lives -- please confirm.

CHANGE LOG:
  - Removed week_start_sun column (not used downstream; QGP calendar is authoritative).
  - LOB canonical mapping applied at Bronze to align with MFC and Gold unified schema.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly
  USING DELTA
  CLUSTER BY (week_sun_sat, lob, channel_group)
  COMMENT 'PulseTMS Bronze — Platform spend aggregated to weekly grain. One row per week_sun_sat x lob x channel_group. Clustered by week_sun_sat, lob, channel_group. Actuals only — no forecast available from this source. Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_platformSpend_weekly.'
  AS
  WITH Mapped AS (
    SELECT
      -- Roll each calendar day forward to its week-ending Saturday
      -- DAYOFWEEK: Sun=1, Mon=2, ..., Sat=7; for a Tuesday (3): 7-3=4 days forward -> Saturday
      date_add(raw.day, 7 - EXTRACT(DAYOFWEEK FROM raw.day)) AS week_sun_sat,

      -- LOB canonical mapping to align with MFC and Gold unified schema
      CASE UPPER(TRIM(raw.lob))
        WHEN 'POSTPAID'  THEN 'POSTPAID'
        WHEN 'HSI'       THEN 'BROADBAND'
        WHEN 'BROADBAND' THEN 'BROADBAND'
        ELSE UPPER(TRIM(raw.lob))
      END                                                                   AS lob,

      -- Channel group mapping to standard vocabulary
      CASE
        WHEN raw.channel_name IN (
          'Paid Search: Brand', 'Paid Search: Non-Brand',
          'Paid Search: PLAs',  'Performance Max'
        )                                                                   THEN 'Paid Search'
        WHEN raw.channel_name = 'Social Network - Campaign'                 THEN 'Paid Social'
        WHEN raw.channel_name IN (
          'Programmatic Display', 'Online Video', 'Over The Top',
          'Display', 'Streaming Radio', 'Content Syndication'
        )                                                                   THEN 'Programmatic'
        ELSE                                                                     'Other'
      END                                                                   AS channel_group,

      TRY_CAST(raw.spend AS DOUBLE)                                         AS spend

    FROM <raw_catalog>.<raw_schema>.agg_day_media_and_outcomes raw
    WHERE raw.lob          IS NOT NULL
      AND raw.channel_name IS NOT NULL
  )
  SELECT
    week_sun_sat,
    lob,
    channel_group,
    SUM(spend) AS spend
  FROM Mapped
  GROUP BY week_sun_sat, lob, channel_group;

END;