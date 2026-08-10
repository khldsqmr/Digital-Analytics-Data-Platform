/* =================================================================================================
FILE:         07_sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly.
  Unions three separate biddable-media raw sources (Programmatic, Paid Social, Paid Search)
  into one weekly Bronze table. Deliberately unfiltered landing zone -- every LOB, every
  platform, every row (including 'Archived') is kept here. Scope narrowing to POSTPAID/
  BROADBAND happens at Gold, not here -- see LOB SCOPE note below for why.

  This is a distinct source from platformSpend, not a replacement -- both will coexist as
  separate toggle-able data_source values in Gold (PLATFORM_SPEND_CHANNEL and
  BIDDABLE_SPEND_CHANNEL), same qgp_date x lob x channel_group shape.

SOURCES (channel_group is a constant per source, not a column in any of them):
  Programmatic : prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr
                 Daily grain. Spend column confirmed as `spend` (not budget/platform_budget/
                 total_costs/advertiser_cost_usd/partner_cost/ttd_cost_usd -- those are
                 inconsistently populated per-DSP or wildly mis-scaled budget caps, confirmed
                 via live GROUP BY DSP this session). LOB column confirmed as `lob`, not
                 `lob_value` (lob_value is null for the majority of rows in every category and
                 sometimes comma-joined). Sub-channel: DSP (Amazon DSP, The Trade Desk, DV360,
                 Blis, Google Ads). Table name suggests current-year-only retention (confirmed:
                 min/max date both fall in 2026, one distinct year) -- no history to pull from
                 here if YoY is ever needed for this source specifically.
  Paid Social  : prd_dbi_analytics.improvado.mrt_paidsocial_pivot
                 Daily grain (has a pre-rolled Monday-anchored week_date column too, but this
                 Bronze uses the daily `date` column with the same roll-forward logic as the
                 other two sources instead, for one consistent week-ending convention across
                 all three unioned sources). Spend column: `spend`, single and unambiguous.
                 Sub-channel: channel (Meta, Pinterest, TikTok, Snapchat, LinkedIn, X).
  Paid Search  : prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily
                 Already a Databricks Gold-layer table (unlike the other two, still in
                 prd_dbi_analytics.improvado) -- but still daily grain, so it's aggregated down
                 like the other two. Spend column is named `cost`, not `spend`. Sub-channel:
                 ad_platform (Google, Bing). `serving_status` deliberately NOT filtered on --
                 it's null for the majority of rows, and a campaign's current status shouldn't
                 erase spend that already happened on a given historical date.

LOB SCOPE (deliberately NOT filtered here):
  Bronze lands every LOB value each source has (Postpaid, HSI, Prepaid, TFB, Metro, TMoney,
  Fiber, Archived, and any null). Only UPPER(TRIM()) casing/whitespace cleanup is applied --
  no semantic value mapping (e.g. HSI -> BROADBAND) happens at this layer, matching MFC's own
  precedent where that mapping lives at Gold, not Bronze. The POSTPAID/BROADBAND-only scope
  Khalid wants in Gold is applied there, not here -- this table is meant to remain a full,
  reusable landing zone in case a future consumer wants a LOB this pipeline doesn't surface.
  'Archived' needs no special handling as a result: it simply won't survive Gold's LOB filter,
  same as Prepaid/TFB/Metro/Fiber/TMoney.

WEEK CONVERSION:
  All three sources are daily grain. week_sun_sat = date_add(date, 7 - DAYOFWEEK(date)), the
  same roll-forward-to-Saturday logic used for platformSpend and adobeFunnel Bronze.

GRAIN:
  week_sun_sat x lob x channel_group x platform
  `platform` is the unified sub-channel column (DSP / channel / ad_platform, source-dependent)
  -- captured now even though only the channel_group-level rollup is being built into Silver/
  Gold this round, so a future BIDDABLE_SPEND_GRANULAR (mirroring MFC_SPEND_GRANULAR) doesn't
  need a Bronze rebuild, only new Silver/Gold work.

REMINDER FOR SILVER (do not repeat the platformSpend bug):
  This Bronze's natural-week rollup means a week straddling a quarter boundary will land as
  ONE row keyed at that week's real Saturday (the BOUNDARY_FIRST date), containing spend from
  both quarters. Silver MUST include the same bf second-join quarter-boundary proration
  platformSpend Silver and adobeFunnel Silver already have, or BOUNDARY_STUB will come back
  null and BOUNDARY_FIRST will be silently overstated -- confirmed as a real bug once already
  this session, not a hypothetical one.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly
  USING DELTA
  CLUSTER BY (week_sun_sat, lob, channel_group)
  COMMENT 'PulseTMS Bronze — Biddable spend (Programmatic + Paid Social + Paid Search), unfiltered landing. One row per week_sun_sat x lob x channel_group x platform. Every LOB and platform value is kept as-is (casing normalized only) — no LOB scope narrowing at this layer, that happens at Gold. Distinct from platformSpend; both coexist as separate Gold data_source values. Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.'
  AS
  WITH

  ProgrammaticMapped AS (
    SELECT
      date_add(raw.date, 7 - EXTRACT(DAYOFWEEK FROM raw.date))  AS week_sun_sat,
      UPPER(TRIM(raw.lob))                                      AS lob,
      'Programmatic'                                            AS channel_group,
      raw.DSP                                                   AS platform,
      TRY_CAST(raw.spend AS DOUBLE)                              AS spend
    FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
  ),

  PaidSocialMapped AS (
    SELECT
      date_add(raw.date, 7 - EXTRACT(DAYOFWEEK FROM raw.date))  AS week_sun_sat,
      UPPER(TRIM(raw.lob))                                      AS lob,
      'Paid Social'                                             AS channel_group,
      raw.channel                                               AS platform,
      TRY_CAST(raw.spend AS DOUBLE)                              AS spend
    FROM prd_dbi_analytics.improvado.mrt_paidsocial_pivot raw
  ),

  PaidSearchMapped AS (
    SELECT
      date_add(raw.date, 7 - EXTRACT(DAYOFWEEK FROM raw.date))  AS week_sun_sat,
      UPPER(TRIM(raw.lob))                                      AS lob,
      'Paid Search'                                             AS channel_group,
      raw.ad_platform                                           AS platform,
      TRY_CAST(raw.cost AS DOUBLE)                               AS spend
    FROM prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily raw
  ),

  AllSources AS (
    SELECT * FROM ProgrammaticMapped
    UNION ALL
    SELECT * FROM PaidSocialMapped
    UNION ALL
    SELECT * FROM PaidSearchMapped
  )

  SELECT
    week_sun_sat,
    lob,
    channel_group,
    platform,
    SUM(spend) AS spend
  FROM AllSources
  GROUP BY week_sun_sat, lob, channel_group, platform;

END;