/* =================================================================================================
FILE:         03_sdi_vw_dashboardPulseTms_gold_unified_long.sql   (Databricks port)
LAYER:        Gold View
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_unified_long

PURPOSE:
  Final unified Gold view for the PulseTMS pipeline.
  Single Tableau data source for all PulseTMS reporting.

  Pure pass-through view - zero computation here.
  All heavy processing (proration, WoW/YoY, CVR, channel allocation, metric definitions) lives
  in Silver SPs. This view simply assembles Silver outputs via named CTEs and stacks them with
  UNION ALL.

STRUCTURE (CTEs currently active):
  CTE 1 - AdobeVolume    : Adobe funnel volume metrics + inline CVR columns (ADOBE_VOLUME)
  CTE 2 - MfcChannel     : MFC spend at lob x channel_group grain (MFC_SPEND_CHANNEL)
  CTE 3 - MfcGranular    : MFC spend at finest grain (MFC_SPEND_GRANULAR)
  CTE 4 - PlatformSpend  : Platform spend at lob x channel_group grain (PLATFORM_SPEND_CHANNEL)
  CTE 5 - UpvForecast    : UPV forecast channel-allocated (UPV_FORECAST)
  CTE 6 - QgpScorecard   : QGP scorecard metrics, no channel_group dimension (QGP_SCORECARD)
  CTE 7 - BiddableSpend  : Programmatic+Paid Social+Paid Search combined, lob x channel_group
                           grain (BIDDABLE_SPEND_CHANNEL) -- coexists with, not a replacement
                           for, PLATFORM_SPEND_CHANNEL
  Final SELECT: UNION ALL of all seven CTEs above

QGP_SCORECARD NOTE:
  Unlike every other source here, QGP has no channel_group dimension - it's enterprise-wide
  KPIs (Activations BOPIS, Store Traffic, VR Calls/Chats, VR Postpaid Activations, 3 Digital %
  metrics), not channel-specific spend or volume. channel_group is NULL for every QGP_SCORECARD
  row, shared with ADOBE and UPV_FORECAST. lob (the display value) is 'Postpaid + Broadband' on
  QGP_SCORECARD as of this version -- see LOB / TRUE_LOB SPLIT below for why this changed from a
  literal NULL, and for true_lob's per-metric mapping, which is where QGP's real LOB story
  actually lives now. metric_type is 'QGP_ACTUAL' or 'QGP_TARGET', passed straight through from
  Silver.

DATA SOURCE VALUES:
  'ADOBE'                  - Adobe volume + CVR rows
  'MFC_SPEND_CHANNEL'      - MFC spend at lob x channel_group; includes All Channels rollup
  'MFC_SPEND_GRANULAR'     - MFC spend at finest grain; mfc_* columns populated
  'PLATFORM_SPEND_CHANNEL' - Platform spend at lob x channel_group; POSTPAID + BROADBAND
  'BIDDABLE_SPEND_CHANNEL' - Biddable spend (Programmatic+Paid Social+Paid Search) at
                             lob x channel_group; POSTPAID + BROADBAND. Separate, coexisting
                             source from PLATFORM_SPEND_CHANNEL, not a replacement for it.
  'UPV_FORECAST'           - UPV forecast channel-allocated
  'QGP_SCORECARD'          - QGP scorecard Actual/Target metric pairs; channel_group = NULL

  IMPORTANT: MFC contributes two sets of rows (CHANNEL + GRANULAR).
  Always filter on data_source before summing spend to avoid double-counting.

CHANNEL GROUPS (standard vocabulary, shared across ADOBE/MFC/PLATFORM/BIDDABLE/UPV_FORECAST):
  'All Channels' | 'Paid Search' | 'Paid Social' | 'Organic Search' |
  'Direct' | 'Programmatic' | 'Other'
  Plus, PLATFORM_SPEND_CHANNEL only: 'iSpot National TV' | 'iSpot OTT' | 'Affiliate' -
  paid-media channels with no Adobe-tracked action equivalent (no on-site attribution for
  linear/streaming TV or affiliate referrals the way there is for clickable digital channels).
  BIDDABLE_SPEND_CHANNEL only ever populates 'All Channels', 'Paid Search', 'Paid Social',
  'Programmatic' -- a subset of the shared vocabulary, not an addition to it, since that's the
  literal scope of its three raw sources.
  Note: Organic Search and Direct exist in ADOBE and UPV_FORECAST only - spend has no concept
  of "organic" or "direct" traffic, since both are unpaid by definition, but UPV_FORECAST's
  channel split is derived directly from Adobe's own prior-year channel mix, so it inherits
  Adobe's full channel vocabulary including these two. QGP_SCORECARD rows have
  channel_group = NULL. This asymmetry (Adobe/Forecast-only vs. Platform-only groups) is
  expected, not a join gap - see PLATFORM_SPEND_CHANNEL's own Bronze header for the full
  reasoning.

LOB / TRUE_LOB SPLIT (read this before using either column):
  Two separate LOB columns exist on every row, because they answer two different questions:

    lob       - the DISPLAY / business-facing value. What a LOB filter or dropdown on the
                dashboard should show. Allowed to be a broadcast label rather than a literal
                filterable dimension where the underlying source has no real per-LOB split
                (ADOBE, UPV_FORECAST) -- see CANONICAL LOB VALUES below.
    true_lob  - the LITERAL value that actually exists as a real, independently-filterable
                dimension in that source's own data. NULL wherever no such literal value
                exists or is confirmed yet. This is the column to trust for anything that
                needs to know "is this row genuinely scoped to one LOB or not" -- lob alone
                cannot answer that for ADOBE/UPV_FORECAST/QGP_SCORECARD.

  CANONICAL LOB VALUES (the lob column):
    'POSTPAID'  - MFC: CONSUMER POSTPAID / POSTPAID; Platform: POSTPAID; Biddable: POSTPAID
    'BROADBAND' - MFC: HSI / BROADBAND; Platform: BROADBAND; Biddable: HSI (renamed in this
                  view's BiddableSpend CTE -- Silver carries the raw, un-canonicalized value)
    'TFB'       - MFC: TFB / TBG (TBG is legacy)
    'Postpaid + Broadband' - ADOBE, UPV_FORECAST, and (as of this version) QGP_SCORECARD. On
                  ADOBE, a static label on every row, not derived per metric_name or summed
                  from anything -- upv/cartstart/orders are unique-visitor/event counts, which
                  can't be summed across a dimension without double-counting. metric_name still
                  carries the real Postpaid/Hsi/Byod/Total distinction unchanged; this is a
                  separate, coarser label sitting alongside it. UPV_FORECAST inherits the same
                  label since it forecasts upvTotalAdobe specifically. QGP_SCORECARD's lob was
                  changed from a literal NULL to this same label in this version, per Khalid's
                  confirmation -- see CHANGE LOG. Deliberately mixed-case, unlike every other
                  lob value in this pipeline, per Khalid's explicit preference. TFB and Metro
                  to follow later as Adobe starts tracking them (unclear yet whether as their
                  own values or folded into this one; UPV_FORECAST and QGP_SCORECARD would
                  presumably follow whatever Adobe's scheme becomes).

  TRUE_LOB VALUES (the true_lob column):
    ADOBE       - NULL for every row, pending. Khalid has confirmed at least one component
                  (Eligibility Checks Completed -> Broadband) but the full upv*/cartstart*/
                  orders* -> Postpaid vs Broadband vs Byod mapping is not yet finalized, so
                  nothing is guessed here. Once confirmed, this should become a real per-
                  metric_name CASE the same shape as QGP_SCORECARD's below, not a broadcast
                  label -- Adobe's metric_name already distinguishes Postpaid/Hsi/Byod
                  component metrics, which is exactly what true_lob should reflect.
    UPV_FORECAST - NULL for every row, mirrors ADOBE's unresolved state since it forecasts
                  upvTotalAdobe specifically and should inherit whatever ADOBE's true_lob
                  mapping becomes once that's finalized.
    MFC_SPEND_CHANNEL / GRANULAR, PLATFORM_SPEND_CHANNEL, BIDDABLE_SPEND_CHANNEL - identical
                  to that row's own lob value. These sources already carry a real, independently
                  filterable LOB dimension, so display and true agree completely; true_lob is a
                  direct copy, not a separate derivation.
    QGP_SCORECARD - real per-metric_name mapping, since the 10 QGP metrics genuinely differ in
                  LOB scope even though they share one display lob label:
                    activationsBopis                                       -> 'POSTPAID'
                    activationsNewAalNoAssistance                          -> 'POSTPAID'
                    vrPostpaidActivations                                  -> 'POSTPAID'
                    digitalPctPhoneNewActsNoAssistPlusAssist               -> 'POSTPAID'
                    digitalPctConsumerPostpaidActivationsTotalInclAssisted -> 'POSTPAID'
                    digitalPctNoAssistanceActivations                      -> 'POSTPAID'
                    digitalPctAssistanceActivations                        -> 'POSTPAID'
                    storeTraffic                                           -> NULL (retail-wide,
                                                                              no LOB concept at all)
                    vrCalls                                                -> NULL (UNCONFIRMED --
                                                                              appendix scope text
                                                                              reads "All / Postpaid",
                                                                              ambiguous, not guessed)
                    vrChats                                                -> NULL (same as vrCalls)

  NULL (both lob and true_lob) never happens now for QGP_SCORECARD's lob column specifically
  (see CHANGE LOG) -- but true_lob is still frequently and legitimately NULL across multiple
  sources, per the mapping above. Don't treat a NULL true_lob as a bug; check this section
  first.

METRIC_TYPE VALUES:
  'ADOBE_VOLUME'       - raw Adobe funnel metrics (upv*, cartstart*, orders*)
  'MFC_SPEND_ACTUAL'   - MFC actual spend
  'MFC_SPEND_FORECAST' - MFC forecast spend
  'PLATFORM_SPEND'     - Platform actual spend (actuals only, no forecast column in this source)
  'BIDDABLE_SPEND'     - Biddable actual spend (actuals only, no forecast column in this source)
  'UPV_FORECAST'       - UPV forecast (upvForecast | upvWebAppForecast)
                         allocation_ratio column shows channel split source
  'QGP_ACTUAL'         - QGP scorecard actual value
  'QGP_TARGET'         - QGP scorecard target/plan value

COLUMN SCHEMA:
  data_source            - source and grain identifier
  qgp_date               - QGP period-end date (Saturday or quarter-end non-Saturday)
  week_type              - 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter            - display string e.g. '2026 Q1'
  days_in_period         - 7 for NORMAL; <7 for BOUNDARY_STUB; remainder for BOUNDARY_FIRST
  is_complete_period     - TRUE when qgp_date <= current_date()
  lob                    - display/business-facing LOB label -- see LOB / TRUE_LOB SPLIT above
  true_lob               - literal, independently-filterable LOB value, NULL where none is
                           confirmed yet -- see LOB / TRUE_LOB SPLIT above. NEW in this version.
  channel_group          - standard channel group (NULL for QGP_SCORECARD)
  metric_name            - camelCase metric identifier
  metric_type            - see METRIC_TYPE VALUES above
  metric_value            - volume/spend/forecast/actual/target value
  metric_value_ly        - prior year value
  wow_numerator          - NULL for BOUNDARY_STUB rows
  wow_denominator        - NULL for BOUNDARY_STUB rows
  wow_pct                - NULL for BOUNDARY_STUB or zero denominator
  yoy_numerator          - NULL for BOUNDARY_STUB rows
  yoy_denominator        - NULL for BOUNDARY_STUB rows
  yoy_pct                - NULL for BOUNDARY_STUB or zero denominator
  max_date               - most recent qgp_date with non-NULL metric_value
  adobe_cvr_value        - pre-computed weekly CVR; ADOBE only; NULL elsewhere
  adobe_cvr_numerator    - CVR numerator; ADOBE only; NULL elsewhere
  adobe_cvr_denominator  - CVR denominator; ADOBE only; NULL elsewhere
  mfc_channel            - MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_tactic             - MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_message_type       - MFC_SPEND_GRANULAR only; NULL elsewhere
  mfc_agency             - MFC_SPEND_GRANULAR only; NULL elsewhere
  allocation_ratio       - UPV_FORECAST only; channel share from prior year same quarter
                           NULL for all other data_source values

DOWNSTREAM:
  Tableau - direct connection to this view.
  sdi_vw_dashboardPulseTms_gold_metricAnnotated_long - joins this view against the appendix
  bridge table for Genie/agentic-SQL consumption; unaffected by this version's changes, the
  bridge joins on data_source/metric_name/lob (the display column), not true_lob.

FUTURE SOURCES:
  Add a new named CTE above the final SELECT following the template at the bottom,
  then add one UNION ALL line. No other schema changes needed for existing Tableau calculations.

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - FLOAT64 -> DOUBLE. Everything else in this view is CAST/CASE/UNION ALL with no BQ-only
    syntax, so it's otherwise a direct translation.

CHANGE LOG:
  - Added CTE 5 UpvForecast: UPV forecast channel-allocated (data_source = 'UPV_FORECAST').
  - Added allocation_ratio column to schema (NULL for all non-UPV_FORECAST rows).
  - Added CTE 6 QgpScorecard: QGP scorecard Actual/Target pairs (data_source = 'QGP_SCORECARD').
  - Uncommented CTE 4 (PlatformSpend) and CTE 5 (UpvForecast) once their Silver procedures
    existed. No lob filter in this view (unlike gold_unified_wide's Platform CTE, which is
    POSTPAID-only) -- both POSTPAID and BROADBAND flow through, since full LOB detail is this
    view's whole purpose.
  - Added CTE 7 (BiddableSpend) once its Silver procedure existed. Applies the same
    HSI -> BROADBAND rename MFC's own CTEs already do, filtered to just the 2 raw values that
    map to POSTPAID/BROADBAND.
  - Added true_lob column to every CTE (new in this version). ADOBE and UPV_FORECAST set it to
    NULL pending Khalid's confirmation of the full per-metric LOB mapping (Eligibility Checks
    Completed -> Broadband is confirmed, the rest is not, so nothing is guessed). MFC_SPEND_
    CHANNEL/GRANULAR, PLATFORM_SPEND_CHANNEL, and BIDDABLE_SPEND_CHANNEL set true_lob equal to
    their own lob value (display and true already agree, real dimension either way).
    QGP_SCORECARD gets a real per-metric_name CASE -- see LOB / TRUE_LOB SPLIT above for the
    full mapping and the two metrics (vrCalls, vrChats) left NULL/unconfirmed rather than
    guessed.
  - CHANGED (non-additive): QgpScorecard's lob column changed from a literal NULL to
    'Postpaid + Broadband', per Khalid's explicit confirmation this session. This is a real
    value change on live, already-queried rows, not a new column -- flagged here because,
    unlike every other change in this file, it can silently break any existing Tableau calc or
    filter that tested ISNULL([Lob]) to identify QGP rows specifically. Confirm no such logic
    exists before this ships; see the true_lob design discussion for the full reasoning.
  - Fixed a stale header comment ("PlatformSpend and UpvForecast commented out above") left
    over from before those two were uncommented in earlier turns.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
AS

WITH

-- =============================================================================
-- CTE 1: ADOBE VOLUME METRICS
--        upv*, cartstart*, orders* at qgp_date x channel_group x metric_name
--        lob = 'Postpaid + Broadband' - a static label on every Adobe row, not derived
--        per metric_name. true_lob = NULL, pending Khalid's confirmation of the full
--        per-metric_name LOB mapping -- see LOB / TRUE_LOB SPLIT in the file header.
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
    'Postpaid + Broadband'                                                AS lob,
    CAST(NULL AS STRING)                                                  AS true_lob,   -- pending confirmation, see file header
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
-- CTE 2: MFC SPEND - CHANNEL GRAIN
--        mfcSpendActual + mfcSpendForecast at lob x channel_group
--        Includes All Channels rollup per lob
--        true_lob mirrors lob exactly -- this source already has a real, independently
--        filterable LOB dimension, so display and true agree.
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
    CASE s.lob_mfc                                                                       -- true_lob: identical mapping to lob, duplicated
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'                                           -- because Spark SQL can't reference an
      WHEN 'POSTPAID'          THEN 'POSTPAID'                                           -- earlier SELECT-list alias by name
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                                                   AS true_lob,
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
-- CTE 3: MFC SPEND - GRANULAR GRAIN
--        mfcSpendActual + mfcSpendForecast at finest grain
--        mfc_* dimension columns populated
--        true_lob mirrors lob exactly, same reasoning as MfcChannel above.
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
    CASE s.lob_mfc                                                                       -- true_lob: identical mapping to lob, duplicated
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                                                   AS true_lob,
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
--        true_lob mirrors lob exactly -- real dimension either way.
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
    s.lob                                                                 AS true_lob,   -- identical to lob, real dimension
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
--        lob = 'Postpaid + Broadband', matching Adobe's lob treatment since this
--        forecasts upvTotalAdobe specifically. true_lob = NULL, mirroring ADOBE's own
--        unresolved state -- see LOB / TRUE_LOB SPLIT in the file header.
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
    'Postpaid + Broadband'                                                AS lob,
    CAST(NULL AS STRING)                                                  AS true_lob,   -- mirrors ADOBE's unresolved state, see file header
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
--        channel_group = NULL - no dimension on this source, see QGP_SCORECARD NOTE
--        in the file header.
--        lob = 'Postpaid + Broadband' -- CHANGED this version from a literal NULL, per
--        Khalid's confirmation. This is the one NON-ADDITIVE change in this file -- see
--        CHANGE LOG for the full reasoning and the check to run before deploying.
--        true_lob = real per-metric_name mapping -- see LOB / TRUE_LOB SPLIT in the
--        file header for the full table and reasoning behind each value, including the
--        two (vrCalls, vrChats) deliberately left NULL/unconfirmed.
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
    'Postpaid + Broadband'                                                AS lob,          -- CHANGED from CAST(NULL AS STRING), see file header CHANGE LOG
    CASE s.metric_name
      WHEN 'activationsBopis'                                       THEN 'POSTPAID'
      WHEN 'activationsNewAalNoAssistance'                          THEN 'POSTPAID'
      WHEN 'vrPostpaidActivations'                                  THEN 'POSTPAID'
      WHEN 'digitalPctPhoneNewActsNoAssistPlusAssist'               THEN 'POSTPAID'
      WHEN 'digitalPctConsumerPostpaidActivationsTotalInclAssisted' THEN 'POSTPAID'
      WHEN 'digitalPctNoAssistanceActivations'                      THEN 'POSTPAID'
      WHEN 'digitalPctAssistanceActivations'                        THEN 'POSTPAID'
      WHEN 'storeTraffic'                                           THEN CAST(NULL AS STRING)  -- retail-wide, no LOB concept at all
      WHEN 'vrCalls'                                                THEN CAST(NULL AS STRING)  -- UNCONFIRMED: appendix scope text reads "All / Postpaid", ambiguous
      WHEN 'vrChats'                                                THEN CAST(NULL AS STRING)  -- UNCONFIRMED: same as vrCalls
      ELSE CAST(NULL AS STRING)
    END                                                                   AS true_lob,
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
--        true_lob mirrors lob exactly -- real dimension either way.
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
    CASE s.lob                                                                          -- true_lob: identical mapping to lob, duplicated
      WHEN 'POSTPAID' THEN 'POSTPAID'
      WHEN 'HSI'      THEN 'BROADBAND'
      ELSE s.lob
    END                                                                   AS true_lob,
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
-- Column position must match exactly across all 7 CTEs for UNION ALL to align
-- correctly -- Spark SQL matches by position, not name. true_lob sits between
-- lob and channel_group in every CTE above.
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
  FUTURE SOURCES - add a new named CTE above following this template,
  then add one UNION ALL line in the final SELECT above. true_lob is included
  in the template -- set it equal to lob if the new source has a real per-row LOB
  dimension, or NULL if it doesn't (or isn't confirmed yet), following the same
  reasoning as every CTE above.
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
      <true_lob expression>     AS true_lob,
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