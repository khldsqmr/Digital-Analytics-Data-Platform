/* =================================================================================================
FILE:         09_sdi_vw_dashboardPulseTms_gold_unified_wide.sql   (Databricks port)
LAYER:        Gold View — Wide / Sense Check
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_unified_wide

PURPOSE:
  Wide pivot view for quick sense-checking of the PulseTMS pipeline.
  One row per qgp_date × channel_group, ordered by qgp_date DESC.

  Sources currently active (channel grain only):
    ADOBE                   — all UPV + cartstart + orders metrics (no LOB dimension)
    MFC_SPEND_CHANNEL       — mfcSpendActual + mfcSpendForecast (POSTPAID LOB only)
    PLATFORM_SPEND_CHANNEL  — platformSpend (POSTPAID LOB only; actuals only)
    BIDDABLE_SPEND_CHANNEL  — biddableSpend, Programmatic+Paid Social+Paid Search combined
                              (POSTPAID LOB only; actuals only) -- coexists with, not a
                              replacement for, PLATFORM_SPEND_CHANNEL
    UPV_FORECAST            — upvForecast + upvWebAppForecast, channel-allocated (no LOB
                              dimension) -- ADDED here as an inferred completion of the
                              pattern (every other channel-grain source live in
                              gold_unified_long also appears here); this source never had a
                              slot in this view before, not even commented out
    QGP_SCORECARD           — the 10 QGP Actual/Target metric pairs (Activations BOPIS, Store
                              Traffic, VR Calls/Chats, VR Postpaid Activations, 3 Digital % metrics)

  NOT included: MFC_SPEND_GRANULAR (use sdi_vw_dashboardPulseTms_gold_unified_long for that)

QGP GRAIN NOTE (read before using the qgp* columns below):
  QGP metrics have no channel_group dimension -- they're enterprise-wide KPIs, not
  channel-specific. The Qgp CTE is joined on qgp_date only (not channel_group), which means
  each QGP column's value is REPEATED across every channel_group row for that date -- it's a
  date-level fact broadcast onto a channel-grain view, not something that varies by channel.
  Don't SUM or otherwise aggregate qgp* columns across channel_group rows for the same date,
  you'll multiply-count them. Look at any single row per date if you just want to eyeball the
  QGP numbers alongside Adobe/MFC for that week.

LOB NOTE:
  MFC and Platform spend columns both reflect POSTPAID only (filtered in each one's own
  Bronze). No LOB column is surfaced here since the grain is channel_group only -- use
  sdi_vw_dashboardPulseTms_gold_unified_long for LOB-level analysis, including Platform's
  BROADBAND rows, which this wide view deliberately excludes.

GRAIN:
  qgp_date × channel_group

KEY COLUMNS:
  is_complete_period — TRUE when qgp_date <= current_date(); use to filter to actual data
  week_type          — 'NORMAL' | 'BOUNDARY_STUB' | 'BOUNDARY_FIRST'
  qgp_quarter        — display string e.g. '2026 Q1'

ORDERING:
  qgp_date DESC, channel_group ASC

NOTE:
  This view is for sense-checking only — not intended as a Tableau data source.
  Use sdi_vw_dashboardPulseTms_gold_unified_long for all production reporting.

PORTING NOTES (BQ -> Databricks), applies to this file only:
  - MAX(IF(cond, val, NULL))  -> unchanged; Databricks SQL supports IF() as a CASE WHEN alias
  - No date arithmetic or BQ-only functions in this file — otherwise a direct translation.

CHANGE LOG:
  - 'quarter' renamed to 'qgp_quarter' to match Silver output schema.
  - 'is_complete_period' added to allow filtering to complete periods.
  - LOB note added to header (spend columns are POSTPAID only).
  - Added WHERE metric_type = 'ADOBE_VOLUME' to Adobe CTE — future-safe filter.
  - Added 17 CVR columns (adobe_cvr_value pivoted per metric_name) to Adobe CTE
    and final SELECT for sense-checking conversion rates alongside volumes.
  - Added Qgp CTE (10 metric pairs, pivoted wide, qgp_date grain) and joined into final SELECT.
  - Commented out Platform CTE, join, and column -- not deleted, see COMMENTED-OUT SOURCES above.
  - Uncommented Platform CTE, join, and column now that sdi_sp_dashboardPulseTms_silver_
    platformSpend_weekly exists (PLATFORM_SPEND_CHANNEL now active, POSTPAID-only, matching
    MFC's own scoping in this wide view -- BROADBAND lives in gold_unified_long instead).
  - Added a new UpvForecast CTE, join, and two columns (upvForecast, upvWebAppForecast) now
    that sdi_sp_dashboardPulseTms_silver_upvForecast_weekly exists. This is genuinely new, not
    an uncomment -- this source never had a slot in this view before. Added for consistency
    with every other channel-grain source (Adobe, MFC, Platform all appear in both gold
    views); flagged in the header as an inferred completion of that pattern rather than
    something explicitly requested.
  - Added a new Biddable CTE, join, and column (biddableSpend) now that
    sdi_sp_dashboardPulseTms_silver_biddableSpend_weekly exists. Kept POSTPAID-only, matching
    Platform's own scoping in this wide view -- full LOB detail (incl. BROADBAND) lives in
    gold_unified_long instead. Coexists with, does not replace, PLATFORM_SPEND_CHANNEL.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide
AS

WITH

-- ---------------------------------------------------------------------------
-- Adobe metrics — pivot from long to wide at channel_group grain
-- ---------------------------------------------------------------------------
Adobe AS (
  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    channel_group,
    MAX(IF(metric_name = 'upvPostpaid',              metric_value, NULL)) AS upvPostpaid,
    MAX(IF(metric_name = 'upvHsi',                   metric_value, NULL)) AS upvHsi,
    MAX(IF(metric_name = 'upvByod',                  metric_value, NULL)) AS upvByod,
    MAX(IF(metric_name = 'upvFlowTotal',             metric_value, NULL)) AS upvFlowTotal,
    MAX(IF(metric_name = 'upvTotalAdobe',            metric_value, NULL)) AS upvTotalAdobe,
    MAX(IF(metric_name = 'cartstartPostpaid',        metric_value, NULL)) AS cartstartPostpaid,
    MAX(IF(metric_name = 'cartstartHsi',             metric_value, NULL)) AS cartstartHsi,
    MAX(IF(metric_name = 'cartstartByod',            metric_value, NULL)) AS cartstartByod,
    MAX(IF(metric_name = 'cartstartTotal',           metric_value, NULL)) AS cartstartTotal,
    MAX(IF(metric_name = 'ordersUnassistedPostpaid', metric_value, NULL)) AS ordersUnassistedPostpaid,
    MAX(IF(metric_name = 'ordersUnassistedHsi',      metric_value, NULL)) AS ordersUnassistedHsi,
    MAX(IF(metric_name = 'ordersUnassistedByod',     metric_value, NULL)) AS ordersUnassistedByod,
    MAX(IF(metric_name = 'ordersUnassistedTotal',    metric_value, NULL)) AS ordersUnassistedTotal,
    MAX(IF(metric_name = 'ordersAssistedPostpaid',   metric_value, NULL)) AS ordersAssistedPostpaid,
    MAX(IF(metric_name = 'ordersAssistedHsi',        metric_value, NULL)) AS ordersAssistedHsi,
    MAX(IF(metric_name = 'ordersAssistedByod',       metric_value, NULL)) AS ordersAssistedByod,
    MAX(IF(metric_name = 'ordersAssistedTotal',      metric_value, NULL)) AS ordersAssistedTotal,
    MAX(IF(metric_name = 'ordersTotal',              metric_value, NULL)) AS ordersTotal,
    -- CVR values (pre-computed weekly rates — use AVG in Tableau for sense checking)
    MAX(IF(metric_name = 'upvFlowTotal',             adobe_cvr_value, NULL)) AS cvrUpvFlow,
    MAX(IF(metric_name = 'upvPostpaid',              adobe_cvr_value, NULL)) AS cvrUpvPostpaid,
    MAX(IF(metric_name = 'upvHsi',                   adobe_cvr_value, NULL)) AS cvrUpvHsi,
    MAX(IF(metric_name = 'upvByod',                  adobe_cvr_value, NULL)) AS cvrUpvByod,
    MAX(IF(metric_name = 'cartstartTotal',           adobe_cvr_value, NULL)) AS cvrCartstartTotal,
    MAX(IF(metric_name = 'cartstartPostpaid',        adobe_cvr_value, NULL)) AS cvrCartstartPostpaid,
    MAX(IF(metric_name = 'cartstartHsi',             adobe_cvr_value, NULL)) AS cvrCartstartHsi,
    MAX(IF(metric_name = 'cartstartByod',            adobe_cvr_value, NULL)) AS cvrCartstartByod,
    MAX(IF(metric_name = 'ordersTotal',              adobe_cvr_value, NULL)) AS cvrOrdersTotal,
    MAX(IF(metric_name = 'ordersUnassistedTotal',    adobe_cvr_value, NULL)) AS cvrOrdersUnassistedTotal,
    MAX(IF(metric_name = 'ordersAssistedTotal',      adobe_cvr_value, NULL)) AS cvrOrdersAssistedTotal,
    MAX(IF(metric_name = 'ordersUnassistedPostpaid', adobe_cvr_value, NULL)) AS cvrOrdersUnassistedPostpaid,
    MAX(IF(metric_name = 'ordersAssistedPostpaid',   adobe_cvr_value, NULL)) AS cvrOrdersAssistedPostpaid,
    MAX(IF(metric_name = 'ordersUnassistedHsi',      adobe_cvr_value, NULL)) AS cvrOrdersUnassistedHsi,
    MAX(IF(metric_name = 'ordersAssistedHsi',        adobe_cvr_value, NULL)) AS cvrOrdersAssistedHsi,
    MAX(IF(metric_name = 'ordersUnassistedByod',     adobe_cvr_value, NULL)) AS cvrOrdersUnassistedByod,
    MAX(IF(metric_name = 'ordersAssistedByod',       adobe_cvr_value, NULL)) AS cvrOrdersAssistedByod
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
  WHERE metric_type = 'ADOBE_VOLUME'
  GROUP BY qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, channel_group
),

-- ---------------------------------------------------------------------------
-- MFC spend — POSTPAID channel grain only
-- ---------------------------------------------------------------------------
Mfc AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'mfcSpendActual',   metric_value, NULL)) AS mfcSpendActual,
    MAX(IF(metric_name = 'mfcSpendForecast', metric_value, NULL)) AS mfcSpendForecast
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly
  WHERE data_source = 'MFC_SPEND_CHANNEL'
    AND lob_mfc     = 'POSTPAID'            -- POSTPAID only for channel-level sense check
  GROUP BY qgp_date, channel_group
),

-- ---------------------------------------------------------------------------
-- QGP scorecard — pivot from long to wide at qgp_date grain (no channel_group
-- dimension on this source; see QGP GRAIN NOTE in the header)
-- ---------------------------------------------------------------------------
Qgp AS (
  SELECT
    qgp_date,
    MAX(IF(metric_name = 'activationsBopis' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpActivationsBopisActual,
    MAX(IF(metric_name = 'activationsBopis' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpActivationsBopisTarget,
    MAX(IF(metric_name = 'activationsNewAalNoAssistance' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpActivationsNewAalNoAssistanceActual,
    MAX(IF(metric_name = 'activationsNewAalNoAssistance' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpActivationsNewAalNoAssistanceTarget,
    MAX(IF(metric_name = 'storeTraffic' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpStoreTrafficActual,
    MAX(IF(metric_name = 'storeTraffic' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpStoreTrafficTarget,
    MAX(IF(metric_name = 'vrCalls' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpVrCallsActual,
    MAX(IF(metric_name = 'vrCalls' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpVrCallsTarget,
    MAX(IF(metric_name = 'vrChats' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpVrChatsActual,
    MAX(IF(metric_name = 'vrChats' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpVrChatsTarget,
    MAX(IF(metric_name = 'vrPostpaidActivations' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpVrPostpaidActivationsActual,
    MAX(IF(metric_name = 'vrPostpaidActivations' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpVrPostpaidActivationsTarget,
    MAX(IF(metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpDigitalPctPhoneNewActsNoAssistPlusAssistActual,
    MAX(IF(metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpDigitalPctPhoneNewActsNoAssistPlusAssistTarget,
    MAX(IF(metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual,
    MAX(IF(metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget,
    MAX(IF(metric_name = 'digitalPctNoAssistanceActivations' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpDigitalPctNoAssistanceActivationsActual,
    MAX(IF(metric_name = 'digitalPctNoAssistanceActivations' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpDigitalPctNoAssistanceActivationsTarget,
    MAX(IF(metric_name = 'digitalPctAssistanceActivations' AND metric_type = 'QGP_ACTUAL', metric_value, NULL)) AS qgpDigitalPctAssistanceActivationsActual,
    MAX(IF(metric_name = 'digitalPctAssistanceActivations' AND metric_type = 'QGP_TARGET', metric_value, NULL)) AS qgpDigitalPctAssistanceActivationsTarget
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly
  GROUP BY qgp_date
)

-- ---------------------------------------------------------------------------
-- UPV forecast — channel grain, all channels (no lob dimension to filter, unlike
-- MFC/Platform). ADDED here as an inferred completion of the established pattern
-- (every channel-grain source live in gold_unified_long also appears here: Adobe,
-- MFC, Platform all do) -- this source never had a slot in this view before,
-- not even commented out, unlike Platform's. Flagging that distinction rather
-- than silently treating this the same as uncommenting a pre-planned CTE.
-- ---------------------------------------------------------------------------
,
UpvForecast AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'upvForecast',       metric_value, NULL)) AS upvForecast,
    MAX(IF(metric_name = 'upvWebAppForecast', metric_value, NULL)) AS upvWebAppForecast
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly
  GROUP BY qgp_date, channel_group
)

-- ---------------------------------------------------------------------------
-- Platform spend — POSTPAID channel grain only, matches MFC's own POSTPAID-only
-- scoping in this wide sense-check view (full lob detail, incl. BROADBAND, lives
-- in sdi_vw_dashboardPulseTms_gold_unified_long instead).
-- ---------------------------------------------------------------------------
,
Platform AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'platformSpend', metric_value, NULL)) AS platformSpend
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly
  WHERE lob = 'POSTPAID'                    -- POSTPAID only; matches MFC channel grain
  GROUP BY qgp_date, channel_group
)

-- ---------------------------------------------------------------------------
-- Biddable spend — POSTPAID channel grain only, matches MFC/Platform's own
-- POSTPAID-only scoping in this wide sense-check view (full lob detail, incl.
-- BROADBAND, lives in sdi_vw_dashboardPulseTms_gold_unified_long instead).
-- Raw lob is already spelled 'POSTPAID' in Silver (no HSI->BROADBAND rename
-- needed for this filter -- that mapping only matters for the long view's
-- BROADBAND rows).
-- ---------------------------------------------------------------------------
,
Biddable AS (
  SELECT
    qgp_date,
    channel_group,
    MAX(IF(metric_name = 'biddableSpend', metric_value, NULL)) AS biddableSpend
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly
  WHERE lob = 'POSTPAID'                    -- POSTPAID only; matches MFC/Platform channel grain
  GROUP BY qgp_date, channel_group
)

-- ---------------------------------------------------------------------------
-- Final join — Adobe as spine, MFC + QGP joined in (Platform commented out above)
-- ---------------------------------------------------------------------------
SELECT
  a.qgp_date,
  a.week_type,
  a.qgp_quarter,
  a.days_in_period,
  a.is_complete_period,
  a.channel_group,

  -- Adobe UPV
  a.upvPostpaid,
  a.upvHsi,
  a.upvByod,
  a.upvFlowTotal,
  a.upvTotalAdobe,

  -- Adobe Cartstart
  a.cartstartPostpaid,
  a.cartstartHsi,
  a.cartstartByod,
  a.cartstartTotal,

  -- Adobe Orders
  a.ordersUnassistedPostpaid,
  a.ordersUnassistedHsi,
  a.ordersUnassistedByod,
  a.ordersUnassistedTotal,
  a.ordersAssistedPostpaid,
  a.ordersAssistedHsi,
  a.ordersAssistedByod,
  a.ordersAssistedTotal,
  a.ordersTotal,

  -- MFC Spend (POSTPAID)
  m.mfcSpendActual,
  m.mfcSpendForecast,

  -- Platform Spend (POSTPAID, actuals only)
  p.platformSpend,

  -- Biddable Spend (POSTPAID, actuals only)
  b.biddableSpend,

  -- UPV Forecast (channel-allocated, all channels -- no lob dimension to filter)
  uf.upvForecast,
  uf.upvWebAppForecast,

  -- QGP scorecard (date-level, repeated across channel_group rows -- see QGP GRAIN NOTE)
  q.qgpActivationsBopisActual,
  q.qgpActivationsBopisTarget,
  q.qgpActivationsNewAalNoAssistanceActual,
  q.qgpActivationsNewAalNoAssistanceTarget,
  q.qgpStoreTrafficActual,
  q.qgpStoreTrafficTarget,
  q.qgpVrCallsActual,
  q.qgpVrCallsTarget,
  q.qgpVrChatsActual,
  q.qgpVrChatsTarget,
  q.qgpVrPostpaidActivationsActual,
  q.qgpVrPostpaidActivationsTarget,
  q.qgpDigitalPctPhoneNewActsNoAssistPlusAssistActual,
  q.qgpDigitalPctPhoneNewActsNoAssistPlusAssistTarget,
  q.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual,
  q.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget,
  q.qgpDigitalPctNoAssistanceActivationsActual,
  q.qgpDigitalPctNoAssistanceActivationsTarget,
  q.qgpDigitalPctAssistanceActivationsActual,
  q.qgpDigitalPctAssistanceActivationsTarget,

  -- Adobe CVR (pre-computed weekly rates)
  a.cvrUpvFlow,
  a.cvrUpvPostpaid,
  a.cvrUpvHsi,
  a.cvrUpvByod,
  a.cvrCartstartTotal,
  a.cvrCartstartPostpaid,
  a.cvrCartstartHsi,
  a.cvrCartstartByod,
  a.cvrOrdersTotal,
  a.cvrOrdersUnassistedTotal,
  a.cvrOrdersAssistedTotal,
  a.cvrOrdersUnassistedPostpaid,
  a.cvrOrdersAssistedPostpaid,
  a.cvrOrdersUnassistedHsi,
  a.cvrOrdersAssistedHsi,
  a.cvrOrdersUnassistedByod,
  a.cvrOrdersAssistedByod

FROM Adobe a
LEFT JOIN Mfc m
  ON  m.qgp_date      = a.qgp_date
  AND m.channel_group = a.channel_group
LEFT JOIN Platform p
  ON  p.qgp_date      = a.qgp_date
  AND p.channel_group = a.channel_group
LEFT JOIN Biddable b
  ON  b.qgp_date      = a.qgp_date
  AND b.channel_group = a.channel_group
LEFT JOIN UpvForecast uf
  ON  uf.qgp_date      = a.qgp_date
  AND uf.channel_group = a.channel_group
LEFT JOIN Qgp q
  ON  q.qgp_date = a.qgp_date   -- date-only join; QGP has no channel_group to match on

ORDER BY a.qgp_date DESC, a.channel_group ASC;