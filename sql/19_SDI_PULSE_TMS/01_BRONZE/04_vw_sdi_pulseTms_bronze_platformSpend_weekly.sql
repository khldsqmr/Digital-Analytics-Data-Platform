/* =================================================================================================
FILE:         04_vw_sdi_pulseTms_bronze_platformSpend_weekly.sql
LAYER:        Bronze View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW NAME:    vw_sdi_pulseTms_bronze_platformSpend_weekly

RAW SOURCE:
  prj-dbi-prd-1.ds_dbi_marketing.agg_day_media_and_outcomes

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_platformSpend_weekly

PURPOSE:
  Source-close Bronze view for Platform spend data in the PulseTMS pipeline.
  Reads from agg_day_media_and_outcomes which is a daily-grain table.

  This Bronze:
    1. Aggregates daily spend to week-ending Saturday (week_sun_sat) grain.
    2. Applies channel group mapping to align with the PulseTMS standard taxonomy.
    3. Passes week_sun_sat through as-is for Silver to align to the QGP calendar dim.

  No QGP boundary logic applied here — handled in Silver via join to
  vw_sdi_pulseTms_dim_qgp_calendar.

BUSINESS GRAIN:
  One row per:
    week_sun_sat × lob × channel_group

DATE CONVENTION:
  week_sun_sat = the Saturday ending the Sun–Sat week containing each day.
  Derived as: day + (7 - DAYOFWEEK(day)) days, where DAYOFWEEK Sun=1, Sat=7.
  Aligns to NORMAL QGP dates. BOUNDARY_STUB / BOUNDARY_FIRST handled in Silver.

LOB VALUES (all in scope):
  POSTPAID, BROADBAND, PREPAID, TFB

CHANNEL GROUP MAPPING:
  channel_name (source)             → channel_group (output)
  ──────────────────────────────────────────────────────────
  Paid Search: Brand                → Paid Search
  Paid Search: Non-Brand            → Paid Search
  Paid Search: PLAs                 → Paid Search
  Performance Max                   → Paid Search
  Social Network - Campaign         → Paid Social
  Programmatic Display              → Programmatic
  Online Video                      → Programmatic  (OLV — programmatically served)
  Over The Top                      → Programmatic
  Display                           → Programmatic
  Streaming Radio                   → Programmatic
  Content Syndication               → Programmatic
  All other channel_name values     → Other
  (includes: Offline, Affiliate, Direct Mail, Super Bowl, Out Of Home,
   Local TV, Cable TV, SL TV, Live Sports TV, Broadcast TV, Direct TV,
   Podcast, Print, B2B, Retail Store, Email - Campaign, Email - Organic,
   Tuesdays, On Device, SMS, Direct, Session Refresh, Referring Domains,
   Natural Search, Social Network - Natural, Other Campaigns,
   ? TfB _efl_ _sln_ ?)

METRICS:
  spend — SUM of daily spend aggregated to weekly grain

BUSINESS RULES:
  - SUM(spend) grouped by week_sun_sat × lob × channel_group.
  - Rows with NULL lob or NULL channel_name are excluded.
  - week_sun_sat derived from daily date using DAYOFWEEK arithmetic.

DOWNSTREAM:
  07_vw_sdi_pulseTms_silver_platformSpend_weekly
================================================================================================= */

CREATE OR REPLACE VIEW
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_pulseTms_bronze_platformSpend_weekly`
AS

WITH

-- ---------------------------------------------------------------------------
-- STEP 1: Apply channel group mapping and derive week-ending Saturday
-- ---------------------------------------------------------------------------
Mapped AS (
  SELECT
    -- Derive week-ending Saturday from daily date
    -- DAYOFWEEK: Sun=1, Mon=2, ..., Sat=7 → add (7 - DAYOFWEEK) days
    DATE_ADD(
      raw.day,
      INTERVAL (7 - EXTRACT(DAYOFWEEK FROM raw.day)) DAY
    )                                                                   AS week_sun_sat,

    SAFE_CAST(UPPER(TRIM(raw.lob)) AS STRING)                           AS lob,

    -- Channel group mapping: platform channel_name → PulseTMS standard taxonomy
    CASE
      WHEN raw.channel_name IN (
        'Paid Search: Brand',
        'Paid Search: Non-Brand',
        'Paid Search: PLAs',
        'Performance Max'
      )                                                                 THEN 'Paid Search'

      WHEN raw.channel_name = 'Social Network - Campaign'               THEN 'Paid Social'

      WHEN raw.channel_name IN (
        'Programmatic Display',
        'Online Video',
        'Over The Top',
        'Display',
        'Streaming Radio',
        'Content Syndication'
      )                                                                 THEN 'Programmatic'

      -- Everything else → Other:
      -- Offline, Affiliate, Direct Mail, Super Bowl, Out Of Home,
      -- Local TV, Cable TV, SL TV, Live Sports TV, Broadcast TV, Direct TV,
      -- Podcast, Print, B2B, Retail Store, Email - Campaign, Email - Organic,
      -- Tuesdays, On Device, SMS, Direct, Session Refresh, Referring Domains,
      -- Natural Search, Social Network - Natural, Other Campaigns,
      -- ? TfB _efl_ _sln_ ?
      ELSE                                                                'Other'
    END                                                                 AS channel_group,

    SAFE_CAST(raw.spend AS FLOAT64)                                     AS spend

  FROM `prj-dbi-prd-1.ds_dbi_marketing.agg_day_media_and_outcomes` raw

  WHERE raw.lob          IS NOT NULL
    AND raw.channel_name IS NOT NULL
)

-- ---------------------------------------------------------------------------
-- STEP 2: Aggregate daily rows to weekly grain
-- ---------------------------------------------------------------------------
SELECT
  week_sun_sat,
  lob,
  channel_group,
  SUM(spend)                                                            AS spend,
  -- Audit: Sunday that opened this week
  DATE_TRUNC(week_sun_sat, WEEK(SUNDAY))                               AS week_start_sun
FROM Mapped
GROUP BY
  week_sun_sat,
  lob,
  channel_group
;