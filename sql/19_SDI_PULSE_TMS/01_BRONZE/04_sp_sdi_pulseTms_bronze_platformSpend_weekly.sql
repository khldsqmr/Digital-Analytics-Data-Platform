/* =================================================================================================
FILE:         04_sp_sdi_pulseTms_bronze_platformSpend_weekly.sql
LAYER:        Stored Procedure
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
PROCEDURE:    sp_sdi_pulseTms_bronze_platformSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_pulseTms_bronze_platformSpend_weekly.
  Called by 00_call_all_sp_pulseTms.sql as part of the weekly refresh.
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
    description = 'PulseTMS Bronze — Platform spend aggregated to weekly grain. One row per week_sun_sat x lob x channel_group. Partitioned by week_sun_sat, clustered by lob and channel_group. Refreshed weekly via sp_sdi_pulseTms_bronze_platformSpend_weekly.'
  )
  AS
  WITH Mapped AS (
    SELECT
      DATE_ADD(raw.day, INTERVAL (7 - EXTRACT(DAYOFWEEK FROM raw.day)) DAY) AS week_sun_sat,
      SAFE_CAST(UPPER(TRIM(raw.lob)) AS STRING)                             AS lob,
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
    WHERE raw.lob IS NOT NULL AND raw.channel_name IS NOT NULL
  )
  SELECT
    week_sun_sat,
    lob,
    channel_group,
    SUM(spend)                                                              AS spend,
    DATE_TRUNC(week_sun_sat, WEEK(SUNDAY))                                  AS week_start_sun
  FROM Mapped
  GROUP BY week_sun_sat, lob, channel_group;

END;