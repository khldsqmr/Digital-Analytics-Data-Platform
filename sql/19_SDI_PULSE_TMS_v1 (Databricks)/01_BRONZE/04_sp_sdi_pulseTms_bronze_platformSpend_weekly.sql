/* =================================================================================================
FILE:         04_sp_sdi_pulseTms_bronze_platformSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_bronze_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_bronze_platformSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.

  Aggregates daily platform spend from agg_day_media_and_outcomes to weekly grain.
  week_sun_sat = week-ending Saturday (Sun–Sat week).
  All date attributes (quarter, week_type, etc.) resolved downstream by joining
  to vw_sdi_pulseTms_dim_qgp_calendar on week_sun_sat = qgp_date.

  Platform spend is actuals only — no forecast column available from this source.

CHANNEL GROUPS (standard vocabulary):
  'Paid Search' | 'Paid Social' | 'Programmatic' | 'Other'
  (Organic Search and Direct are Adobe-only traffic attribution categories)

LOB CANONICAL VALUES:
  'POSTPAID'  — source: 'POSTPAID'
  'BROADBAND' — source: 'HSI', 'BROADBAND'
  Other LOBs passed through as-is (can be expanded via canonical mapping in Silver/Gold)

CHANGE LOG:
  - Removed week_start_sun column (not used downstream; QGP calendar is authoritative).
  - LOB canonical mapping applied at Bronze to align with MFC and Gold unified schema.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_platformSpend_weekly`()
OPTIONS (strict_mode = false)
BEGIN

  CREATE OR REPLACE TABLE
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_pulseTms_bronze_platformSpend_weekly`
  PARTITION BY week_sun_sat
  CLUSTER BY lob, channel_group
  OPTIONS (
    description = 'PulseTMS Bronze — Platform spend aggregated to weekly grain. One row per week_sun_sat x lob x channel_group. Partitioned by week_sun_sat, clustered by lob and channel_group. Actuals only — no forecast available from this source. Refreshed weekly via sp_sdi_pulseTms_bronze_platformSpend_weekly.'
  )
  AS
  WITH Mapped AS (
    SELECT
      -- Roll each calendar day forward to its week-ending Saturday
      DATE_ADD(raw.day, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM raw.day)) DAY) AS week_sun_sat,

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

      SAFE_CAST(raw.spend AS FLOAT64)                                       AS spend

    FROM `prj-dbi-prd-1.ds_dbi_marketing.agg_day_media_and_outcomes` raw
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