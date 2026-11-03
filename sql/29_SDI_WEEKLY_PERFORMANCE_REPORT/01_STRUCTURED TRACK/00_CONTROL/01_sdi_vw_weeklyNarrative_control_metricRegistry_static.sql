/* =================================================================================================
FILE:           01_sdi_vw_weeklyNarrative_control_metricRegistry_static.sql
PLATFORM:       Databricks
LAYER:          Control (static configuration)
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_weeklyNarrative_control_metricRegistry_static

SOURCE LINEAGE:
  This project (weeklyNarrative) is separate from PulseTMS. It does not recompute any metric;
  it reads finished numbers from the PulseTMS Gold layer. Every registry row therefore names
  its upstream explicitly:
    source_project = 'PULSE_TMS'
    source_object  = 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long'
  metric_key is prefixed with the source project (pulseTms.*) so keys stay unique when other
  upstream projects are added later.

KEY NAMING:
  metric_key = pulseTms.<PulseTMS wide-view column name> wherever the wide view has one
  (sdi_vw_dashboardPulseTms_gold_unified_wide), so the report, the dashboards and Tableau all
  use the same names: pulseTms.upvPostpaid, pulseTms.mfcSpendActualPostpaid,
  pulseTms.mfcSpendForecastBroadband, pulseTms.biddableSpendFiber, pulseTms.qgpVrCallsActual.
  Rows with no wide-view column follow the same pattern (pulseTms.mfcSpendActualTfb).
  The Gold MATCH keys are separate: PulseTMS Gold long stores MFC as metric_name =
  mfcSpendActual + true_lob = POSTPAID, and that is what data_source / metric_name /
  metric_type / source_lob below point at. Words like "Gold" in this project's own object names
  (e.g. sdi_tbl_weeklyNarrative_gold_*) refer to THIS project's layers, not PulseTMS.

PURPOSE:
  The bridge between the PulseTMS Gold numbers (Track A) and the words used in the weekly
  narrative report (Track B).

  One row per reportable metric slice. Each row says:
    - WHERE the number lives in PulseTMS Gold
        (data_source + metric_name + metric_type + source_lob + channel_group)
    - WHAT it is called in the report (display_name, metric_lob, aliases)
    - WHAT it is compared against (target_metric_key). Target means the forecast for MFC
      spend and UPV, and the QGP target for QGP scorecard metrics.
    - WHETHER channel-level driver rows should also be captured for the "why"

LINE OF BUSINESS (two columns):
  metric_lob  The LOB(s) the metric COVERS, written out (no TOTAL bucket):
                POSTPAID                        one LOB
                POSTPAID + BROADBAND            several LOBs, joined with + in the fixed
                                                order POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO
                NULL                            no LOB (total site UPV, store traffic, VR calls)
              LOB codes: POSTPAID | BROADBAND | BYOD | FIBER | TFB | METRO
  source_lob  The LOB value(s) to READ from PulseTMS Gold true_lob:
                NULL                     source has no LOB (ADOBE puts it in metric_name:
                                         upvPostpaid, upvHsi, upvByod; UPV_FORECAST; some QGP)
                [POSTPAID]               one LOB
                [POSTPAID, BROADBAND]    several LOBs, SUMMED by the snapshot (MFC total, because
                                         PulseTMS Gold long has no combined row); WoW / YoY are
                                         recomputed from summed numerators and denominators.
                                         Additive units only.

  Current report scope = Postpaid + Broadband. Scaling plan:
    TFB    TFB rows already exist in PulseTMS MFC Gold. Pre-built here as is_active = FALSE
           (pulseTms.mfcSpendActualTfb, pulseTms.mfcSpendForecastTfb); flip to TRUE when the
           report adds TFB. Add TFB to the MFC total source_lob only if the total should include it.
    METRO  Not in PulseTMS Gold yet (Biddable excludes Metro). When PulseTMS adds it, add rows
           with metric_lob = METRO; no procedure change is needed.

WHY A VIEW:
  Static configuration, changed only through code review + redeploy, same pattern as the
  hardcoded ChannelList in the UPV Forecast Silver. If non-engineers ever need to edit
  aliases / owners / tolerances from an app, convert this to a Delta table at that point.

  Because a view keeps no history, every downstream snapshot stamps registry_version.
  BUMP registry_version ON EVERY CHANGE to this file.

METRIC ROLE (metric_role):
  ACTUAL  The metric the report writes about (UPV, MFC spend, QGP activations).
  TARGET  A forecast or QGP target (UPV forecast, MFC forecast spend, QGP targets). Shown in
          the report NEXT TO its actual: the snapshot puts it on the actual's row as
          target_week_value, vs_target_pct and qtd_target_total. Not written up on its own,
          so it never shows up as its own highlight ("UPV forecast up 5%").
  Each ACTUAL points at its TARGET through target_metric_key.

GOLD MATCHING RULES (used by the facts snapshot procedure):
  - data_source, metric_name                  exact match
  - metric_type, channel_group                NULL-safe match (<=>)
  - source_lob vs Gold true_lob               NULL = Gold true_lob IS NULL; otherwise Gold
                                              true_lob must be one of the source_lob values
      ADOBE / UPV_FORECAST rows have true_lob = NULL in PulseTMS Gold
      QGP_SCORECARD rows have channel_group = NULL in PulseTMS Gold
  - Drivers: when include_channel_drivers = TRUE, every other channel_group for the same
    metric slice is captured where channel_group LIKE driver_channel_like.
      '%'        all other channels (Adobe, MFC)
      '% - All'  Biddable channel totals only, so platform rows are never double counted
                 against their own channel totals (see Biddable Silver contract)

NAMING CONVENTION (applies to every weeklyNarrative object):
  Objects sdi_<tbl|vw|sp>_weeklyNarrative_<layer>_<purpose>_<cadence>
          layer   bronze | silver | gold | control | validation | orchestration
          cadence gold = wide | long; static config = static; orchestration / run logs =
                  perRun; bronze / silver = daily | weekly
          Current objects:
            sdi_vw_weeklyNarrative_control_metricRegistry_static
            sdi_sp_weeklyNarrative_gold_factsHistory_long      (writes the table below)
            sdi_tbl_weeklyNarrative_gold_factsHistory_long
            sdi_vw_weeklyNarrative_gold_factsLatest_long
            sdi_vw_weeklyNarrative_gold_whatMoved_long
            sdi_sp_weeklyNarrative_orchestration_factsHistoryBackfill_perRun
  Codes   UPPER_SNAKE. Any value used to filter, join or branch logic:
          metric_lob, source_lob, unit, source_project, row_role, run_type (and PulseTMS codes
          kept as-is: data_source, metric_type, week_type).
          e.g. POSTPAID, POSTPAID + BROADBAND, PCT, PULSE_TMS, ACTUAL, TARGET.
  Labels  Human-readable text for the report, in their own columns:
          display_name    Sentence case (used inside sentences: MFC spend, Postpaid)
          Filter on the code, write with the label. Never compare on a label.
  Keys    camelCase identifiers, matching PulseTMS metric and column names:
          metric_key = pulseTms.<PulseTMS wide-view column name>.
  Aliases lowercase, because they are matched against lowercased narrative text.
  Upstream values copied from PulseTMS are never re-cased (e.g. validation status),
  so they always match the PulseTMS objects they came from.

UNITS:
  COUNT | USD | PCT
  PCT metrics are stored as ratios in PulseTMS Gold (e.g. 0.31). Downstream compares them in
  percentage POINTS, never as a percent of a percent.

OPEN ITEMS (fill in, then bump registry_version):
  - aliases are a starter list; extend them from the words actually used in past reports
  - higher_is_better is NULL where direction is not obviously good or bad (spend, VR calls)
  - BYOD is its own metric_lob; confirm whether the report treats it as part of Postpaid

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
(
  registry_version         COMMENT 'Version of this registry definition (yyyy-mm-dd.n). Bumped on every change. Stamped onto every facts snapshot so past weeks can be traced to the registry they used.',
  metric_key               COMMENT 'Primary key. Stable identifier for one reportable metric slice, prefixed with the source project and named after the PulseTMS wide-view column where one exists, e.g. pulseTms.upvTotalAdobe, pulseTms.mfcSpendActualPostpaid. Never reuse or rename a key once snapshots exist.',
  display_name             COMMENT 'Display label (sentence case) of the metric, used inside narrative sentences and on review screens. Acronyms stay as written (UPV, MFC, QGP, BYOD).',
  metric_lob               COMMENT 'The LOB(s) this metric covers, in UPPER_SNAKE: one LOB (POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO), or several joined with + in that fixed order when the metric combines them (e.g. POSTPAID + BROADBAND for the MFC total). NULL = the metric has no LOB (e.g. total site UPV, store traffic, virtual retail calls).',
  sort_order               COMMENT 'Display order within the report. Reference-only rows use 900+ so they sort last.',
  metric_role              COMMENT 'ACTUAL = the metric the report writes about. TARGET = a forecast or QGP target; it is snapshotted and shown NEXT TO its actual (target_week_value, vs_target_pct, QTD vs target on the actual row), but never written up as a metric of its own.',
  source_project           COMMENT 'Code of the upstream project that owns the source numbers: PULSE_TMS for every row today (same code PulseTMS validation uses). This project (weeklyNarrative) never recomputes these metrics; it only reads them.',
  source_object            COMMENT 'Fully qualified upstream object the numbers are read from. Today always prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long (PulseTMS Gold). A new upstream project gets its own value here and its own adapter in the facts snapshot procedure.',
  data_source              COMMENT 'Source match key. Exact value of data_source in the source_object (PulseTMS Gold): ADOBE, MFC_SPEND_CHANNEL, BIDDABLE_SPEND_CHANNEL, QGP_SCORECARD, UPV_FORECAST.',
  metric_name              COMMENT 'Source match key. Exact value of metric_name in the source_object (PulseTMS Gold). For ADOBE the LOB is part of the metric_name (upvPostpaid, upvHsi, upvByod).',
  metric_type              COMMENT 'Source match key, NULL-safe. Exact value of metric_type in PulseTMS Gold (ADOBE_VOLUME, MFC_SPEND_ACTUAL, MFC_SPEND_FORECAST, BIDDABLE_SPEND, QGP_ACTUAL, QGP_TARGET, UPV_FORECAST).',
  source_lob               COMMENT 'The LOB value(s) to read from the source, matched against PulseTMS Gold true_lob. NULL = the source has no LOB for this metric (ADOBE, UPV_FORECAST, some QGP; the LOB is in metric_name instead). One value, e.g. [POSTPAID] = that single LOB. Several values, e.g. [POSTPAID, BROADBAND] = the snapshot SUMS them (used for MFC, whose Gold long has no combined row); only for additive units (COUNT, USD).',
  channel_group            COMMENT 'Source match key, NULL-safe. PulseTMS Gold headline channel_group, normally All Channels. NULL for QGP_SCORECARD, which has no channel dimension.',
  unit                     COMMENT 'COUNT | USD | PCT. PCT metrics are ratios in PulseTMS Gold (0.31 = 31%) and are compared in percentage points, never as a percent of a percent.',
  higher_is_better         COMMENT 'Direction of good performance. TRUE = increase is good, FALSE = decrease is good, NULL = neutral or context dependent (spend, VR calls). Used to word moves as positive or negative.',
  target_metric_key        COMMENT 'metric_key of the target this metric is compared against for the same week and channel_group. Target = the forecast for MFC spend and UPV, and the QGP target for QGP scorecard metrics. NULL when the metric has no target.',
  include_channel_drivers  COMMENT 'TRUE = also snapshot every other channel_group for this slice as ACTUAL_DRIVER rows, so the narrative can say which channels drove a move.',
  driver_channel_like      COMMENT 'SQL LIKE pattern restricting which channel_group values become ACTUAL_DRIVER rows. % = all other channels. % - All = Biddable channel totals only, so platform rows are never added to their own totals.',
  aliases                  COMMENT 'Lowercase words and phrases people use for this metric in narratives and past reports. Used by Track B to tag text to metric_key.',
  owner_team               COMMENT 'Code of the team accountable for the metric and its explanation: SDI.',
  is_active                COMMENT 'FALSE = not snapshotted. Used for retired metrics and for pre-built rows waiting on a scope change (e.g. TFB).',
  notes                    COMMENT 'Free-text definition notes, caveats and known source issues for this metric.'
)
COMMENT 'Weekly narrative report: static control view. One row per reportable metric slice. Maps report language (display names, aliases, metric_lob) to exact metric slices of an upstream source (today only PulseTMS Gold, sdi_vw_dashboardPulseTms_gold_unified_long, recorded in source_project and source_object), plus source LOB matching (single or summed), targets (forecast or QGP target) and driver rules. Changed only through code review and redeploy; bump registry_version on every change.'
AS

SELECT
  '2026-10-29.13' AS registry_version,
  r.*

FROM VALUES

  /* ===============================================================================================
     TRAFFIC / FUNNEL  (PulseTMS ADOBE, All Channels headline, channel drivers)
     =============================================================================================== */

  ( 'pulseTms.upvTotalAdobe', 'UPV (total Adobe)', 'POSTPAID + BROADBAND', 10, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvTotalAdobe', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, 'pulseTms.upvForecast', TRUE, '%',
    ARRAY('upv', 'traffic', 'site traffic', 'visitors'), 'SDI', TRUE,
    'Headline traffic metric. Compared against UPV forecast (All Channels only).' ),

  ( 'pulseTms.upvFlowTotal', 'UPV in flow', 'POSTPAID + BROADBAND', 20, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvFlowTotal', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('flow upv', 'flow traffic', 'flow visitors'), 'SDI', TRUE,
    'Postpaid + HSI + BYOD flows.' ),

  ( 'pulseTms.upvPostpaid', 'UPV Postpaid', 'POSTPAID', 30, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvPostpaid', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid upv', 'postpaid traffic', 'postpaid visitors'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.upvHsi', 'UPV HSI', 'BROADBAND', 40, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvHsi', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi upv', 'hsi traffic', 'broadband traffic', 'home internet traffic'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.upvByod', 'UPV BYOD', 'BYOD', 45, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvByod', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod upv', 'byod traffic', 'bring your own device traffic'), 'SDI', TRUE,
    'BYOD kept as its own metric_lob. Confirm whether the report treats BYOD as part of Postpaid.' ),

  ( 'pulseTms.cartstartTotal', 'Cart starts', 'POSTPAID + BROADBAND', 50, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartTotal', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('cart starts', 'cartstarts', 'carts'), 'SDI', TRUE,
    'Postpaid + HSI + BYOD (built in Adobe Silver).' ),

  ( 'pulseTms.cartstartPostpaid', 'Cart starts Postpaid', 'POSTPAID', 51, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartPostpaid', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid cart starts', 'postpaid carts'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.cartstartHsi', 'Cart starts HSI', 'BROADBAND', 52, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartHsi', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi cart starts', 'broadband cart starts'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.cartstartByod', 'Cart starts BYOD', 'BYOD', 53, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartByod', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod cart starts'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersTotal', 'Orders (total)', 'POSTPAID + BROADBAND', 60, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersTotal', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('orders', 'total orders', 'web orders'), 'SDI', TRUE,
    'Assisted + unassisted, Postpaid + HSI + BYOD.' ),

  ( 'pulseTms.ordersUnassistedTotal', 'Orders unassisted', 'POSTPAID + BROADBAND', 70, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedTotal', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('unassisted orders', 'self-serve orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersUnassistedPostpaid', 'Orders unassisted Postpaid', 'POSTPAID', 71, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedPostpaid', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid unassisted orders', 'postpaid orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersUnassistedHsi', 'Orders unassisted HSI', 'BROADBAND', 72, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedHsi', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi unassisted orders', 'hsi orders', 'broadband orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersUnassistedByod', 'Orders unassisted BYOD', 'BYOD', 73, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedByod', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod unassisted orders', 'byod orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedTotal', 'Orders assisted', 'POSTPAID + BROADBAND', 75, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedTotal', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('assisted orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedPostpaid', 'Orders assisted Postpaid', 'POSTPAID', 76, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedPostpaid', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid assisted orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedHsi', 'Orders assisted HSI', 'BROADBAND', 77, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedHsi', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi assisted orders', 'broadband assisted orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedByod', 'Orders assisted BYOD', 'BYOD', 78, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedByod', 'ADOBE_VOLUME', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod assisted orders'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),


  /* ===============================================================================================
     FORECAST TARGET  (shown next to UPV actual, not written up on its own)
     =============================================================================================== */

  ( 'pulseTms.upvForecast', 'UPV forecast', 'POSTPAID + BROADBAND', 900, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'UPV_FORECAST', 'upvForecast', 'UPV_FORECAST', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('upv forecast', 'traffic forecast'), 'SDI', TRUE,
    'Target for pulseTms.upvTotalAdobe. Only All Channels is populated today (see UPV Forecast Silver).' ),


  /* ===============================================================================================
     MFC SPEND  (actual vs MFC forecast; per LOB + Postpaid + Broadband rollup; TFB pre-built, inactive)
     =============================================================================================== */

  ( 'pulseTms.mfcSpendActualTotal', 'MFC spend, Postpaid + Broadband', 'POSTPAID + BROADBAND', 100, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', ARRAY('POSTPAID', 'BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastTotal', TRUE, '%',
    ARRAY('media spend', 'total spend', 'mfc spend', 'flowchart spend'), 'SDI', TRUE,
    'LOB rollup of POSTPAID + BROADBAND, same definition as mfcSpendActualTotal in the PulseTMS wide view. Add TFB / METRO to source_lob only when the report scope expands.' ),

  ( 'pulseTms.mfcSpendForecastTotal', 'MFC forecast spend, Postpaid + Broadband', 'POSTPAID + BROADBAND', 905, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', ARRAY('POSTPAID', 'BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('planned spend', 'spend plan', 'spend forecast'), 'SDI', TRUE,
    'Drivers kept so channel-level actual vs forecast can be compared.' ),

  ( 'pulseTms.mfcSpendActualPostpaid', 'MFC spend, Postpaid', 'POSTPAID', 101, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', ARRAY('POSTPAID'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastPostpaid', TRUE, '%',
    ARRAY('postpaid spend', 'postpaid media spend'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendForecastPostpaid', 'MFC forecast spend, Postpaid', 'POSTPAID', 910, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', ARRAY('POSTPAID'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid forecast spend', 'postpaid spend plan'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendActualBroadband', 'MFC spend, Broadband', 'BROADBAND', 102, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', ARRAY('BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastBroadband', TRUE, '%',
    ARRAY('broadband spend', 'hsi spend'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendForecastBroadband', 'MFC forecast spend, Broadband', 'BROADBAND', 920, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', ARRAY('BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('broadband forecast spend', 'hsi spend plan'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendActualTfb', 'MFC spend, TFB', 'TFB', 103, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', ARRAY('TFB'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastTfb', TRUE, '%',
    ARRAY('tfb spend', 'business spend', 't-mobile for business spend'), 'SDI', FALSE,
    'Pre-built for the TFB scope expansion. TFB rows already exist in PulseTMS MFC Gold; set is_active = TRUE (and bump registry_version) to start reporting.' ),

  ( 'pulseTms.mfcSpendForecastTfb', 'MFC forecast spend, TFB', 'TFB', 925, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', ARRAY('TFB'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('tfb forecast spend', 'tfb spend plan'), 'SDI', FALSE,
    'Activate together with pulseTms.mfcSpendActualTfb.' ),


  /* ===============================================================================================
     BIDDABLE SPEND  (ALL + per LOB)
     =============================================================================================== */

  ( 'pulseTms.biddableSpendTotal', 'Biddable spend, all LOBs', 'POSTPAID + BROADBAND + FIBER', 120, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', ARRAY('ALL'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('biddable spend', 'biddable', 'search social programmatic spend'), 'SDI', TRUE,
    'PulseTMS Biddable ALL = POSTPAID + BROADBAND + FIBER. Drivers restricted to channel totals (% - All).' ),

  ( 'pulseTms.biddableSpendPostpaid', 'Biddable spend, Postpaid', 'POSTPAID', 121, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', ARRAY('POSTPAID'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('postpaid biddable spend'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.biddableSpendBroadband', 'Biddable spend, Broadband', 'BROADBAND', 122, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', ARRAY('BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('broadband biddable spend', 'hsi biddable spend'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.biddableSpendFiber', 'Biddable spend, Fiber', 'FIBER', 123, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', ARRAY('FIBER'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('fiber spend', 'fiber biddable spend'), 'SDI', TRUE,
    'Fiber currently comes from Paid Search only; YoY may be NULL.' ),


  /* ===============================================================================================
     QGP SCORECARD  (actual vs QGP target; channel_group is NULL in PulseTMS Gold)
     =============================================================================================== */

  ( 'pulseTms.qgpActivationsBopisActual', 'Digital phone activations (BOPIS + non-BOPIS)', 'POSTPAID', 200, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsBopis', 'QGP_ACTUAL', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpActivationsBopisTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('bopis', 'activations', 'digital activations', 'phone activations'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpActivationsBopisTarget', 'Digital phone activations (BOPIS + non-BOPIS) QGP target', 'POSTPAID', 930, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsBopis', 'QGP_TARGET', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('bopis target'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpActivationsNewAalNoAssistanceActual', 'Activations, New + AAL, no assistance', 'POSTPAID', 210, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsNewAalNoAssistance', 'QGP_ACTUAL', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpActivationsNewAalNoAssistanceTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('new + aal', 'aal activations', 'bts activations'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpActivationsNewAalNoAssistanceTarget', 'Activations, New + AAL, no assistance QGP target', 'POSTPAID', 940, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsNewAalNoAssistance', 'QGP_TARGET', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('new + aal target'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual', 'Digital % of postpaid activations', 'POSTPAID', 220, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_ACTUAL', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'PCT', TRUE, 'pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('digital %', 'digital share', 'digital mix'), 'SDI', TRUE,
    'Ratio metric. Compare in percentage points (unit = PCT).' ),

  ( 'pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget', 'Digital % of postpaid activations QGP target', 'POSTPAID', 950, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_TARGET', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'PCT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('digital % target'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpStoreTrafficActual', 'Store traffic', CAST(NULL AS STRING), 300, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'storeTraffic', 'QGP_ACTUAL', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpStoreTrafficTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('store traffic', 'foot traffic', 'retail traffic'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpStoreTrafficTarget', 'Store traffic QGP target', CAST(NULL AS STRING), 960, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'storeTraffic', 'QGP_TARGET', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('store traffic target'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrCallsActual', 'Virtual retail calls', CAST(NULL AS STRING), 400, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrCalls', 'QGP_ACTUAL', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), 'pulseTms.qgpVrCallsTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('vr calls', 'virtual retail calls', 'inbound calls'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrCallsTarget', 'Virtual retail calls QGP target', CAST(NULL AS STRING), 970, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrCalls', 'QGP_TARGET', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('vr calls target'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrChatsActual', 'Virtual retail chats', CAST(NULL AS STRING), 410, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrChats', 'QGP_ACTUAL', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), 'pulseTms.qgpVrChatsTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('vr chats', 'virtual retail chats', 'chats'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrChatsTarget', 'Virtual retail chats QGP target', CAST(NULL AS STRING), 980, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrChats', 'QGP_TARGET', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('vr chats target'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrPostpaidActivationsActual', 'Virtual retail postpaid activations', 'POSTPAID', 420, 'ACTUAL',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrPostpaidActivations', 'QGP_ACTUAL', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpVrPostpaidActivationsTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('vr activations', 'virtual retail activations'), 'SDI', TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrPostpaidActivationsTarget', 'Virtual retail postpaid activations QGP target', 'POSTPAID', 990, 'TARGET',
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrPostpaidActivations', 'QGP_TARGET', ARRAY('POSTPAID'), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('vr activations target'), 'SDI', TRUE,
    CAST(NULL AS STRING) )

AS r(
  metric_key,
  display_name,
  metric_lob,
  sort_order,
  metric_role,
  source_project,
  source_object,
  data_source,
  metric_name,
  metric_type,
  source_lob,
  channel_group,
  unit,
  higher_is_better,
  target_metric_key,
  include_channel_drivers,
  driver_channel_like,
  aliases,
  owner_team,
  is_active,
  notes
)
;