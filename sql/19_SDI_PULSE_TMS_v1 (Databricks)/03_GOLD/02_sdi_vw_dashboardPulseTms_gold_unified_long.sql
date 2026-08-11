/* =================================================================================================
FILE:         03_sdi_vw_dashboardPulseTms_gold_unified_long.sql   (Databricks port)
LAYER:        Gold View
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_unified_long

PURPOSE:
  Final unified Gold view for the PulseTMS pipeline.
  Single Tableau data source for all PulseTMS reporting.

  Pure pass-through view — zero computation here.
  All heavy processing (proration, WoW/YoY, CVR, channel allocation, metric definitions) lives
  in Silver SPs. This view simply assembles Silver outputs via named CTEs and stacks them with
  UNION ALL.

STRUCTURE (CTEs currently active):
  CTE 1 — AdobeVolume    : Adobe funnel volume metrics + inline CVR columns (ADOBE_VOLUME)
  CTE 2 — MfcChannel     : MFC spend at lob x channel_group grain (MFC_SPEND_CHANNEL)
  CTE 3 — MfcGranular    : MFC spend at finest grain (MFC_SPEND_GRANULAR)
  CTE 4 — PlatformSpend  : Platform spend at lob x channel_group grain (PLATFORM_SPEND_CHANNEL)
  CTE 5 — UpvForecast    : UPV forecast channel-allocated (UPV_FORECAST)
  CTE 6 — QgpScorecard   : QGP scorecard metrics, no lob/channel_group dimension (QGP_SCORECARD)
  CTE 7 — BiddableSpend  : Programmatic+Paid Social+Paid Search combined, lob x channel_group
                           grain (BIDDABLE_SPEND_CHANNEL) -- coexists with, not a replacement
                           for, PLATFORM_SPEND_CHANNEL
  Final SELECT: UNION ALL of all seven CTEs above

QGP_SCORECARD NOTE:
  Unlike every other source here, QGP has no lob or channel_group dimension — it's enterprise-
  wide KPIs (Activations BOPIS, Store Traffic, VR Calls/Chats, VR Postpaid Activations, 3
  Digital % metrics), not channel-specific spend or volume. lob and channel_group are both NULL
  for every QGP_SCORECARD row, same treatment as ADOBE and UPV_FORECAST get for lob. metric_type
  is 'QGP_ACTUAL' or 'QGP_TARGET', passed straight through from Silver.

DATA SOURCE VALUES:
  'ADOBE'                  — Adobe volume + CVR rows; lob = NULL
  'MFC_SPEND_CHANNEL'      — MFC spend at lob x channel_group; includes All Channels rollup
  'MFC_SPEND_GRANULAR'     — MFC spend at finest grain; mfc_* columns populated
  'PLATFORM_SPEND_CHANNEL' — Platform spend at lob x channel_group; POSTPAID + BROADBAND
  'BIDDABLE_SPEND_CHANNEL' — Biddable spend (Programmatic+Paid Social+Paid Search) at
                             lob x channel_group; POSTPAID + BROADBAND. Separate, coexisting
                             source from PLATFORM_SPEND_CHANNEL, not a replacement for it.
  'UPV_FORECAST'           — UPV forecast channel-allocated; lob = NULL
  'QGP_SCORECARD'          — QGP scorecard Actual/Target metric pairs; lob = NULL, channel_group = NULL

  IMPORTANT: MFC contributes two sets of rows (CHANNEL + GRANULAR).
  Always filter on data_source before summing spend to avoid double-counting.

CHANNEL GROUPS (standard vocabulary, shared across ADOBE/MFC/PLATFORM/BIDDABLE/UPV_FORECAST):
  'All Channels' | 'Paid Search' | 'Paid Social' | 'Organic Search' |
  'Direct' | 'Programmatic' | 'Other'
  Plus, PLATFORM_SPEND_CHANNEL only: 'iSpot National TV' | 'iSpot OTT' | 'Affiliate' —
  paid-media channels with no Adobe-tracked action equivalent (no on-site attribution for
  linear/streaming TV or affiliate referrals the way there is for clickable digital channels).
  BIDDABLE_SPEND_CHANNEL only ever populates 'All Channels', 'Paid Search', 'Paid Social',
  'Programmatic' -- a subset of the shared vocabulary, not an addition to it, since that's the
  literal scope of its three raw sources.
  Note: Organic Search and Direct exist in ADOBE and UPV_FORECAST only — spend has no concept
  of "organic" or "direct" traffic, since both are unpaid by definition, but UPV_FORECAST's
  channel split is derived directly from Adobe's own prior-year channel mix, so it inherits
  Adobe's full channel vocabulary including these two. QGP_SCORECARD rows have
  channel_group = NULL. This asymmetry (Adobe/Forecast-only vs. Platform-only groups) is
  expected, not a join gap — see PLATFORM_SPEND_CHANNEL's own Bronze header for the full
  reasoning.

LOB CANONICAL VALUES:
  'POSTPAID'  — MFC: CONSUMER POSTPAID / POSTPAID; Platform: POSTPAID; Biddable: POSTPAID
  'BROADBAND' — MFC: HSI / BROADBAND; Platform: BROADBAND; Biddable: HSI (renamed in this
                view's BiddableSpend CTE -- Silver carries the raw, un-canonicalized value)
  'TFB'       — MFC: TFB / TBG (TBG is legacy)
  NULL        — ADOBE, UPV_FORECAST, QGP_SCORECARD (no LOB dimension)

METRIC_TYPE VALUES:
  'ADOBE_VOLUME'       — raw Adobe funnel metrics (upv*, cartstart*, orders*)
  'MFC_SPEND_ACTUAL'   — MFC actual spend
  'MFC_SPEND_FORECAST' — MFC forecast spend
  'PLATFORM_SPEND'     — Platform actual spend (actuals only, no forecast column in this source)
  'BIDDABLE_SPEND'     — Biddable actual spend (actuals only, no forecast column in this source)
  'UPV_FORECAST'       — UPV forecast (upvForecast | upvWebAppForecast)
                         allocation_ratio column shows channel split source
  'QGP_ACTUAL'         — QGP scorecard actual value
  'QGP_TARGET'         — QGP scorecard target/plan value

COLUMN SCHEMA:
  data_source            — source and grain identifier
  qgp_date               — QGP period-end date (Saturday or quarter-end non-Saturday)
  week_type              — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter            — display string e.g. '2026 Q1'
  days_in_period         — 7 for NORMAL; <7 for BOUNDARY_STUB; remainder for BOUNDARY_FIRST
  is_complete_period     — TRUE when qgp_date <= current_date()
  lob                    — canonical LOB (NULL for ADOBE, UPV_FORECAST, QGP_SCORECARD)
  channel_group          — standard channel group (NULL for QGP_SCORECARD)
  metric_name            — camelCase metric identifier
  metric_type            — see METRIC_TYPE VALUES above
  metric_value           — volume/spend/forecast/actual/target value
  metric_value_ly        — prior year value
  wow_numerator          — NULL for BOUNDARY_STUB rows
  wow_denominator        — NULL for BOUNDARY_STUB rows
  wow_pct                — NULL for BOUNDARY_STUB or zero denominator
  yoy_numerator          — NULL for BOUNDARY_STUB rows
  yoy_denominator        — NULL for BOUNDARY_STUB rows
  yoy_pct                — NULL for BOUNDARY_STUB or zero denominator
  max_date               — most recent qgp_date with non-NULL metric_value
  adobe_cvr_value        — pre-computed weekly CVR; ADOBE only; NULL elsewhere
  adobe_cvr_numerator    — CVR numerator; ADOBE only; NULL elsewhere
  adobe_cvr_denominator  — CVR denominator; ADOBE only; NULL elsewhere
  mfc_channel            — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_tactic             — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_message_type       — MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_agency             — MFC_SPEND_GRANULAR only; NULL elsewhere
  allocation_ratio       — UPV_FORECAST only; channel share from prior year same quarter
                           NULL for all other data_source values

DOWNSTREAM:
  Tableau — direct connection to this view

FUTURE SOURCES:
  Add a new named CTE above the final SELECT following the template at the bottom,
  then add one UNION ALL line. No schema changes needed for existing Tableau calculations.

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - FLOAT64 -> DOUBLE. Everything else in this view is CAST/CASE/UNION ALL with no BQ-only
    syntax, so it's otherwise a direct translation.

CHANGE LOG:
  - Added CTE 5 UpvForecast: UPV forecast channel-allocated (data_source = 'UPV_FORECAST').
  - Added allocation_ratio column to schema (NULL for all non-UPV_FORECAST rows).
  - Added CTE 6 QgpScorecard: QGP scorecard Actual/Target pairs (data_source = 'QGP_SCORECARD'),
    lob and channel_group both NULL for this source -- see QGP_SCORECARD NOTE above.
  - Commented out CTE 4 (PlatformSpend) and CTE 5 (UpvForecast), and their UNION ALL lines,
    per your request to keep only Adobe/MFC/QGP active for now. Not deleted.
  - Uncommented CTE 4 (PlatformSpend) and its UNION ALL line now that
    sdi_sp_dashboardPulseTms_silver_platformSpend_weekly exists. No lob filter here (unlike
    gold_unified_wide's Platform CTE, which is POSTPAID-only) -- both POSTPAID and BROADBAND
    flow through, since full LOB detail is this view's whole purpose. Channel_group vocabulary
    updated in header to include Platform's three unique groups (iSpot National TV, iSpot OTT,
    Affiliate) alongside the shared Adobe/MFC/Platform vocabulary.
  - Uncommented CTE 5 (UpvForecast) and its UNION ALL line now that
    sdi_sp_dashboardPulseTms_silver_upvForecast_weekly exists (ported from BQ). No changes to
    the CTE body itself -- it already read from the correct table name. Header's CHANNEL GROUPS
    note updated: UPV_FORECAST inherits Adobe's full channel vocabulary, including Direct and
    Organic Search, since its channel split is derived from Adobe's own prior-year mix.
  - Added CTE 7 (BiddableSpend) and its UNION ALL line now that
    sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly exists. Appended at the end rather
    than inserted between CTE 4 and CTE 5, to avoid renumbering CTEs 5/6. Applies the same
    HSI -> BROADBAND rename MFC's own CTEs already do (Silver carries the raw value), filtered
    to just the 2 raw values that map to POSTPAID/BROADBAND -- full LOB detail from Bronze
    (Prepaid/TFB/Metro/Fiber/Archived/TMoney) intentionally excluded here, matching the same
    POSTPAID+BROADBAND scope as PLATFORM_SPEND_CHANNEL. Coexists with, does not replace,
    PLATFORM_SPEND_CHANNEL -- both are separate, toggle-able data_source values in Gold now.
  - Fixed a stale header comment ("PlatformSpend and UpvForecast commented out above") left
    over from before those two were uncommented in earlier turns -- both have been active for
    several turns now; the comment was simply never updated to match.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
AS

WITH

-- =============================================================================
-- CTE 1: ADOBE VOLUME METRICS
--        upv*, cartstart*, orders* at qgp_date x channel_group x metric_name
--        lob = NULL — Adobe has no LOB dimension
--        metric_type = 'ADOBE_VOLUME'
-- =============================================================================
AdobeVolume AS (
  SELECT
    'ADOBE'                                                               AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CAST(NULL AS STRING)                                                  AS lob,
    s.channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    s.adobe_cvr_value,
    s.adobe_cvr_numerator,
    s.adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS DOUBLE)                                                  AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly s
  WHERE s.metric_type = 'ADOBE_VOLUME'
),

-- =============================================================================
-- CTE 2: MFC SPEND — CHANNEL GRAIN
--        mfcSpendActual + mfcSpendForecast at lob x channel_group
--        Includes All Channels rollup per lob
--        metric_type = 'MFC_SPEND_ACTUAL' or 'MFC_SPEND_FORECAST'
-- =============================================================================
MfcChannel AS (
  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                                                   AS lob,
    s.channel_group,
    s.metric_name,
    CASE s.metric_name
      WHEN 'mfcSpendActual'   THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
    END                                                                   AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS DOUBLE)                                                  AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s
  WHERE s.data_source = 'MFC_SPEND_CHANNEL'
),

-- =============================================================================
-- CTE 3: MFC SPEND — GRANULAR GRAIN
--        mfcSpendActual + mfcSpendForecast at finest grain
--        mfc_* dimension columns populated
--        metric_type = 'MFC_SPEND_ACTUAL' or 'MFC_SPEND_FORECAST'
-- =============================================================================
MfcGranular AS (
  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                                                   AS lob,
    s.channel_group,
    s.metric_name,
    CASE s.metric_name
      WHEN 'mfcSpendActual'   THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
    END                                                                   AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_denominator,
    s.channel                                                             AS mfc_channel,
    s.tactic                                                              AS mfc_tactic,
    s.message_type                                                        AS mfc_message_type,
    s.agency                                                              AS mfc_agency,
    CAST(NULL AS DOUBLE)                                                  AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s
  WHERE s.data_source = 'MFC_SPEND_GRANULAR'
),

-- =============================================================================
-- CTE 4: PLATFORM SPEND
--        Platform (paid media) spend at lob x channel_group grain, POSTPAID +
--        BROADBAND both present (no lob filter here -- that's what distinguishes
--        this view from gold_unified_wide, which is POSTPAID-only for Platform).
--        metric_type = 'PLATFORM_SPEND'
-- =============================================================================
PlatformSpend AS (
  SELECT
    'PLATFORM_SPEND_CHANNEL'                                              AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    s.lob,
    s.channel_group,
    s.metric_name,
    'PLATFORM_SPEND'                                                      AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS DOUBLE)                                                  AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly s
),

-- =============================================================================
-- CTE 5: UPV FORECAST
--        UPV forecast channel-allocated via prior-year same-quarter Adobe ratios
--        metric_type = 'UPV_FORECAST'
-- =============================================================================
UpvForecast AS (
  SELECT
    'UPV_FORECAST'                                                        AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CAST(NULL AS STRING)                                                  AS lob,
    s.channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    s.allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly s
),

-- =============================================================================
-- CTE 6: QGP SCORECARD
--        10 Actual/Target metric pairs (Activations BOPIS, Activations New+AAL No
--        Assistance, Store Traffic, VR Calls, VR Chats, VR Postpaid Activations,
--        3 Digital % metrics) at qgp_date x metric_name x metric_type
--        lob = NULL, channel_group = NULL — no dimension on this source, see
--        QGP_SCORECARD NOTE in the file header
--        metric_type = 'QGP_ACTUAL' or 'QGP_TARGET', passed through from Silver
-- =============================================================================
QgpScorecard AS (
  SELECT
    'QGP_SCORECARD'                                                       AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CAST(NULL AS STRING)                                                  AS lob,
    CAST(NULL AS STRING)                                                  AS channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS DOUBLE)                                                  AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly s
),

-- =============================================================================
-- CTE 7: BIDDABLE SPEND
--        Programmatic + Paid Social + Paid Search combined, at lob x channel_group
--        grain, POSTPAID + BROADBAND both present (no lob filter beyond the raw
--        value IN check below -- HSI is renamed to BROADBAND here since Silver
--        carries the raw, un-canonicalized value, matching how MFC does this same
--        rename in its own CTEs above). Coexists with, does not replace,
--        PLATFORM_SPEND_CHANNEL -- same shape, different (and separate) source.
--        metric_type = 'BIDDABLE_SPEND'
-- =============================================================================
BiddableSpend AS (
  SELECT
    'BIDDABLE_SPEND_CHANNEL'                                              AS data_source,
    CAST(s.qgp_date AS DATE)                                              AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    CASE s.lob
      WHEN 'POSTPAID' THEN 'POSTPAID'
      WHEN 'HSI'      THEN 'BROADBAND'
      ELSE s.lob
    END                                                                   AS lob,
    s.channel_group,
    s.metric_name,
    'BIDDABLE_SPEND'                                                      AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                                              AS max_date,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                                                  AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                                                  AS mfc_channel,
    CAST(NULL AS STRING)                                                  AS mfc_tactic,
    CAST(NULL AS STRING)                                                  AS mfc_message_type,
    CAST(NULL AS STRING)                                                  AS mfc_agency,
    CAST(NULL AS DOUBLE)                                                  AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly s
  WHERE s.lob IN ('POSTPAID', 'HSI')   -- the 2 raw values that canonicalize to POSTPAID/BROADBAND;
                                         -- excludes Prepaid/TFB/Metro/Fiber/Archived/TMoney, which
                                         -- Bronze deliberately left in for other potential consumers
)

-- =============================================================================
-- FINAL: Stack active CTEs
-- =============================================================================
SELECT * FROM AdobeVolume
UNION ALL SELECT * FROM MfcChannel
UNION ALL SELECT * FROM MfcGranular
UNION ALL SELECT * FROM PlatformSpend
UNION ALL SELECT * FROM UpvForecast
UNION ALL SELECT * FROM QgpScorecard
UNION ALL SELECT * FROM BiddableSpend

/*
  =============================================================================
  FUTURE SOURCES — add a new named CTE above following this template,
  then add one UNION ALL line in the final SELECT above.
  No schema changes needed for existing Tableau calculations.
  =============================================================================
  NewSource AS (
    SELECT
      '<SOURCE_NAME>'           AS data_source,
      CAST(s.qgp_date AS DATE)  AS qgp_date,
      s.week_type,
      s.qgp_quarter,
      s.days_in_period,
      s.is_complete_period,
      s.lob,
      s.channel_group,
      s.metric_name,
      '<METRIC_TYPE>'           AS metric_type,
      s.metric_value,
      s.metric_value_ly,
      s.wow_numerator,
      s.wow_denominator,
      s.wow_pct,
      s.yoy_numerator,
      s.yoy_denominator,
      s.yoy_pct,
      CAST(s.max_date AS DATE)  AS max_date,
      CAST(NULL AS DOUBLE)      AS adobe_cvr_value,
      CAST(NULL AS DOUBLE)      AS adobe_cvr_numerator,
      CAST(NULL AS DOUBLE)      AS adobe_cvr_denominator,
      CAST(NULL AS STRING)      AS mfc_channel,
      CAST(NULL AS STRING)      AS mfc_tactic,
      CAST(NULL AS STRING)      AS mfc_message_type,
      CAST(NULL AS STRING)      AS mfc_agency,
      CAST(NULL AS DOUBLE)      AS allocation_ratio
    FROM prdrzranalytics.lab42.<silver_table> s
  )
  =============================================================================
*/
;