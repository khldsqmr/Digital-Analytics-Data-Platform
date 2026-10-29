/* =================================================================================================
FILE:           01_sdi_vw_weeklyNarrative_control_metricRegistry.sql
PLATFORM:       Databricks
LAYER:          Control (static configuration)
CATALOG.SCHEMA: prdrzranalytics.lab42
VIEW:           sdi_vw_weeklyNarrative_control_metricRegistry

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
  metric_type / true_lob below point at. Words like "Gold" in this project's own object names
  (e.g. sdi_tbl_weeklyNarrative_gold_*) refer to THIS project's layers, not PulseTMS.

PURPOSE:
  The bridge between the PulseTMS Gold numbers (Track A) and the words used in the weekly
  narrative report (Track B).

  One row per reportable metric slice. Each row says:
    - WHERE the number lives in PulseTMS Gold
        (data_source + metric_name + metric_type + true_lob or lob_rollup + channel_group)
    - WHAT it is called in the report (display_name, report_section, report_lob, aliases)
    - WHAT it is compared against (target_metric_key). Target means the forecast for MFC
      spend and UPV, and the QGP target for QGP scorecard metrics.
    - WHETHER channel-level driver rows should also be captured for the "why"

LINE OF BUSINESS (report_lob):
  PulseTMS stores LOB differently per source:
    ADOBE            inside metric_name (upvPostpaid, upvHsi, upvByod); true_lob is NULL
    MFC / Biddable   true_lob column (POSTPAID, BROADBAND, FIBER, TFB, Biddable ALL)
    QGP              true_lob derived per metric (POSTPAID or NULL)
  report_lob puts every row on ONE vocabulary so the report can tell a per-LOB story:
    POSTPAID | BROADBAND | BYOD | FIBER | TFB | METRO | TOTAL | NOT_LOB_SPECIFIC
  report_lob_label carries the matching display text (Postpaid, Broadband, ...).

  Current report scope = Postpaid + Broadband. Scaling plan:
    TFB    TFB rows already exist in PulseTMS MFC Gold. Pre-built here as is_active = FALSE
           (pulseTms.mfcSpendActualTfb, pulseTms.mfcSpendForecastTfb); flip to TRUE when the report adds TFB. Add TFB to the
           MFC total lob_rollup only if the total should include it.
    METRO  Not in PulseTMS Gold yet (Biddable excludes Metro). When PulseTMS adds it, add rows
           with report_lob = METRO; no procedure change is needed.

LOB ROLLUPS (lob_rollup):
  PulseTMS Gold long has no combined Postpaid + Broadband row for MFC (the PulseTMS wide view
  sums it). lob_rollup = [POSTPAID, BROADBAND] makes the snapshot SUM those true_lob rows,
  recomputing WoW / YoY from summed numerators and denominators. Additive units only.

WHY A VIEW:
  Static configuration, changed only through code review + redeploy, same pattern as the
  hardcoded ChannelList in the UPV Forecast Silver. If non-engineers ever need to edit
  aliases / owners / tolerances from an app, convert this to a Delta table at that point.

  Because a view keeps no history, every downstream snapshot stamps registry_version.
  BUMP registry_version ON EVERY CHANGE to this file.

ROW ROLES:
  is_headline = TRUE   Metric that can appear in the report.
  is_headline = FALSE  Reference-only row (forecast / target). Snapshotted so headline rows
                       can be compared against it, never reported on its own.

GOLD MATCHING RULES (used by the facts snapshot procedure):
  - data_source, metric_name                  exact match
  - metric_type, channel_group                NULL-safe match (<=>)
  - true_lob                                  NULL-safe match, or membership in lob_rollup
      ADOBE / UPV_FORECAST rows have true_lob = NULL in PulseTMS Gold
      QGP_SCORECARD rows have channel_group = NULL in PulseTMS Gold
  - Drivers: when include_channel_drivers = TRUE, every other channel_group for the same
    metric slice is captured where channel_group LIKE driver_channel_like.
      '%'        all other channels (Adobe, MFC)
      '% - All'  Biddable channel totals only, so platform rows are never double counted
                 against their own channel totals (see Biddable Silver contract)

NAMING CONVENTION (applies to every weeklyNarrative object):
  Codes   UPPER_SNAKE. Any value used to filter, join or branch logic:
          report_lob, unit, source_project, row_role, run_type (and PulseTMS codes kept as-is:
          data_source, metric_type, true_lob, week_type).
          e.g. POSTPAID, BROADBAND, NOT_LOB_SPECIFIC, PCT, PULSE_TMS, HEADLINE.
  Labels  Human-readable text for the report, in their own columns:
          report_section, report_lob_label  Title Case (used as headings: Virtual Retail, Postpaid)
          display_name                      Sentence case (used inside sentences: MFC spend, Postpaid)
          Filter on the code, write with the label. Never compare on a label.
  Keys    camelCase identifiers, matching PulseTMS metric and column names:
          metric_key = pulseTms.<PulseTMS wide-view column name>.
  Aliases lowercase, because they are matched against lowercased narrative text.
  Upstream values copied from PulseTMS are never re-cased (source_lob, validation status),
  so they always match the PulseTMS objects they came from.

UNITS:
  COUNT | USD | PCT
  PCT metrics are stored as ratios in PulseTMS Gold (e.g. 0.31). Downstream compares them in
  percentage POINTS, never as a percent of a percent.

OPEN ITEMS (fill in, then bump registry_version):
  - owner_team is NULL everywhere until owners are confirmed
  - aliases are a starter list; extend them from the words actually used in past reports
  - higher_is_better is NULL where direction is not obviously good or bad (spend, VR calls)
  - BYOD is its own report_lob; confirm whether the report treats it as part of Postpaid

================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry
(
  registry_version         COMMENT 'Version of this registry definition (yyyy-mm-dd.n). Bumped on every change. Stamped onto every facts snapshot so past weeks can be traced to the registry they used.',
  metric_key               COMMENT 'Primary key. Stable identifier for one reportable metric slice, prefixed with the source project and named after the PulseTMS wide-view column where one exists, e.g. pulseTms.upvTotalAdobe, pulseTms.mfcSpendActualPostpaid. Never reuse or rename a key once snapshots exist.',
  display_name             COMMENT 'Display label (sentence case) of the metric, used inside narrative sentences and on review screens. Acronyms stay as written (UPV, MFC, QGP, BYOD).',
  report_section           COMMENT 'Display label (Title Case) of the report section: Traffic, Funnel, Spend, Activations, Retail, Virtual Retail.',
  report_lob               COMMENT 'Code (UPPER_SNAKE) of the report-facing line of business, one vocabulary across every source: POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO, TOTAL (combined across the LOBs in that metric scope, see notes), NOT_LOB_SPECIFIC (metric has no LOB split, e.g. store traffic). Lets the report group a per-LOB story across traffic, spend and activations even though PulseTMS stores LOB in different places per source.',
  report_lob_label         COMMENT 'Display label of report_lob for the narrative text: Postpaid, Broadband, BYOD, Fiber, TFB, Metro, Total, Not LOB specific. Filter and join on report_lob; write with report_lob_label.',
  sort_order               COMMENT 'Display order within the report. Reference-only rows use 900+ so they sort last.',
  is_headline              COMMENT 'TRUE = metric can appear in the report. FALSE = reference-only row (forecast or QGP target) used only as a comparison target.',
  source_project           COMMENT 'Code of the upstream project that owns the source numbers: PULSE_TMS for every row today (same code PulseTMS validation uses). This project (weeklyNarrative) never recomputes these metrics; it only reads them.',
  source_object            COMMENT 'Fully qualified upstream object the numbers are read from. Today always prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long (PulseTMS Gold). A new upstream project gets its own value here and its own adapter in the facts snapshot procedure.',
  data_source              COMMENT 'Source match key. Exact value of data_source in the source_object (PulseTMS Gold): ADOBE, MFC_SPEND_CHANNEL, BIDDABLE_SPEND_CHANNEL, QGP_SCORECARD, UPV_FORECAST.',
  metric_name              COMMENT 'Source match key. Exact value of metric_name in the source_object (PulseTMS Gold). For ADOBE the LOB is part of the metric_name (upvPostpaid, upvHsi, upvByod).',
  metric_type              COMMENT 'Source match key, NULL-safe. Exact value of metric_type in PulseTMS Gold (ADOBE_VOLUME, MFC_SPEND_ACTUAL, MFC_SPEND_FORECAST, BIDDABLE_SPEND, QGP_ACTUAL, QGP_TARGET, UPV_FORECAST).',
  true_lob                 COMMENT 'Source match key, NULL-safe. PulseTMS Gold true_lob for a single-LOB slice (POSTPAID, BROADBAND, FIBER, TFB, ALL). NULL for sources with no LOB in PulseTMS Gold (ADOBE, UPV_FORECAST, some QGP metrics) and for lob_rollup rows.',
  lob_rollup               COMMENT 'When set, the slice is the SUM of these PulseTMS Gold true_lob values (e.g. [POSTPAID, BROADBAND]) instead of a single true_lob. Used where PulseTMS Gold long has no combined row (MFC). Additive units only (COUNT, USD); never set for PCT. Extend this list when the report scope adds TFB or METRO.',
  channel_group            COMMENT 'Source match key, NULL-safe. PulseTMS Gold headline channel_group, normally All Channels. NULL for QGP_SCORECARD, which has no channel dimension.',
  unit                     COMMENT 'COUNT | USD | PCT. PCT metrics are ratios in PulseTMS Gold (0.31 = 31%) and are compared in percentage points, never as a percent of a percent.',
  higher_is_better         COMMENT 'Direction of good performance. TRUE = increase is good, FALSE = decrease is good, NULL = neutral or context dependent (spend, VR calls). Used to word moves as positive or negative.',
  target_metric_key        COMMENT 'metric_key of the target this metric is compared against for the same week and channel_group. Target = the forecast for MFC spend and UPV, and the QGP target for QGP scorecard metrics. NULL when the metric has no target.',
  include_channel_drivers  COMMENT 'TRUE = also snapshot every other channel_group for this slice as DRIVER rows, so the narrative can say which channels drove a move.',
  driver_channel_like      COMMENT 'SQL LIKE pattern restricting which channel_group values become DRIVER rows. % = all other channels. % - All = Biddable channel totals only, so platform rows are never added to their own totals.',
  aliases                  COMMENT 'Lowercase words and phrases people use for this metric in narratives and past reports. Used by Track B to tag text to metric_key.',
  owner_team               COMMENT 'Team accountable for the metric and its explanation. NULL until confirmed.',
  is_active                COMMENT 'FALSE = not snapshotted. Used for retired metrics and for pre-built rows waiting on a scope change (e.g. TFB).',
  notes                    COMMENT 'Free-text definition notes, caveats and known source issues for this metric.'
)
COMMENT 'Weekly narrative report: static control view. One row per reportable metric slice. Maps report language (display names, aliases, sections, report_lob) to exact metric slices of an upstream source (today only PulseTMS Gold, sdi_vw_dashboardPulseTms_gold_unified_long, recorded in source_project and source_object), plus LOB rollups, targets (forecast or QGP target) and driver rules. Changed only through code review and redeploy; bump registry_version on every change.'
AS

SELECT
  '2026-10-29.7' AS registry_version,
  r.*

FROM VALUES

  /* ===============================================================================================
     TRAFFIC / FUNNEL  (PulseTMS ADOBE, All Channels headline, channel drivers)
     =============================================================================================== */

  ( 'pulseTms.upvTotalAdobe', 'UPV (total Adobe)', 'Traffic', 'TOTAL', 'Total', 10, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvTotalAdobe', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, 'pulseTms.upvForecast', TRUE, '%',
    ARRAY('upv', 'traffic', 'site traffic', 'visitors'), CAST(NULL AS STRING), TRUE,
    'Headline traffic metric. Compared against UPV forecast (All Channels only).' ),

  ( 'pulseTms.upvFlowTotal', 'UPV in flow', 'Traffic', 'TOTAL', 'Total', 20, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvFlowTotal', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('flow upv', 'flow traffic', 'flow visitors'), CAST(NULL AS STRING), TRUE,
    'Postpaid + HSI + BYOD flows.' ),

  ( 'pulseTms.upvPostpaid', 'UPV Postpaid', 'Traffic', 'POSTPAID', 'Postpaid', 30, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvPostpaid', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid upv', 'postpaid traffic', 'postpaid visitors'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.upvHsi', 'UPV HSI', 'Traffic', 'BROADBAND', 'Broadband', 40, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvHsi', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi upv', 'hsi traffic', 'broadband traffic', 'home internet traffic'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.upvByod', 'UPV BYOD', 'Traffic', 'BYOD', 'BYOD', 45, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'upvByod', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod upv', 'byod traffic', 'bring your own device traffic'), CAST(NULL AS STRING), TRUE,
    'BYOD kept as its own report_lob. Confirm whether the report treats BYOD as part of Postpaid.' ),

  ( 'pulseTms.cartstartTotal', 'Cart starts', 'Funnel', 'TOTAL', 'Total', 50, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartTotal', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('cart starts', 'cartstarts', 'carts'), CAST(NULL AS STRING), TRUE,
    'Postpaid + HSI + BYOD (built in Adobe Silver).' ),

  ( 'pulseTms.cartstartPostpaid', 'Cart starts Postpaid', 'Funnel', 'POSTPAID', 'Postpaid', 51, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartPostpaid', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid cart starts', 'postpaid carts'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.cartstartHsi', 'Cart starts HSI', 'Funnel', 'BROADBAND', 'Broadband', 52, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartHsi', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi cart starts', 'broadband cart starts'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.cartstartByod', 'Cart starts BYOD', 'Funnel', 'BYOD', 'BYOD', 53, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'cartstartByod', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod cart starts'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersTotal', 'Orders (total)', 'Funnel', 'TOTAL', 'Total', 60, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersTotal', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('orders', 'total orders', 'web orders'), CAST(NULL AS STRING), TRUE,
    'Assisted + unassisted, Postpaid + HSI + BYOD.' ),

  ( 'pulseTms.ordersUnassistedTotal', 'Orders unassisted', 'Funnel', 'TOTAL', 'Total', 70, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedTotal', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('unassisted orders', 'self-serve orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersUnassistedPostpaid', 'Orders unassisted Postpaid', 'Funnel', 'POSTPAID', 'Postpaid', 71, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedPostpaid', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid unassisted orders', 'postpaid orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersUnassistedHsi', 'Orders unassisted HSI', 'Funnel', 'BROADBAND', 'Broadband', 72, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedHsi', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi unassisted orders', 'hsi orders', 'broadband orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersUnassistedByod', 'Orders unassisted BYOD', 'Funnel', 'BYOD', 'BYOD', 73, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersUnassistedByod', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod unassisted orders', 'byod orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedTotal', 'Orders assisted', 'Funnel', 'TOTAL', 'Total', 75, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedTotal', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('assisted orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedPostpaid', 'Orders assisted Postpaid', 'Funnel', 'POSTPAID', 'Postpaid', 76, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedPostpaid', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid assisted orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedHsi', 'Orders assisted HSI', 'Funnel', 'BROADBAND', 'Broadband', 77, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedHsi', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('hsi assisted orders', 'broadband assisted orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.ordersAssistedByod', 'Orders assisted BYOD', 'Funnel', 'BYOD', 'BYOD', 78, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'ADOBE', 'ordersAssistedByod', 'ADOBE_VOLUME', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), TRUE, '%',
    ARRAY('byod assisted orders'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),


  /* ===============================================================================================
     FORECAST REFERENCE  (not reported on its own)
     =============================================================================================== */

  ( 'pulseTms.upvForecast', 'UPV forecast', 'Traffic', 'TOTAL', 'Total', 900, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'UPV_FORECAST', 'upvForecast', 'UPV_FORECAST', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('upv forecast', 'traffic forecast'), CAST(NULL AS STRING), TRUE,
    'Target for pulseTms.upvTotalAdobe. Only All Channels is populated today (see UPV Forecast Silver).' ),


  /* ===============================================================================================
     MFC SPEND  (actual vs MFC forecast; per LOB + Postpaid + Broadband rollup; TFB pre-built, inactive)
     =============================================================================================== */

  ( 'pulseTms.mfcSpendActualTotal', 'MFC spend, Postpaid + Broadband', 'Spend', 'TOTAL', 'Total', 100, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', CAST(NULL AS STRING), ARRAY('POSTPAID', 'BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastTotal', TRUE, '%',
    ARRAY('media spend', 'total spend', 'mfc spend', 'flowchart spend'), CAST(NULL AS STRING), TRUE,
    'LOB rollup of POSTPAID + BROADBAND, same definition as mfcSpendActualTotal in the PulseTMS wide view. Add TFB / METRO to lob_rollup only when the report scope expands.' ),

  ( 'pulseTms.mfcSpendForecastTotal', 'MFC forecast spend, Postpaid + Broadband', 'Spend', 'TOTAL', 'Total', 905, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', CAST(NULL AS STRING), ARRAY('POSTPAID', 'BROADBAND'), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('planned spend', 'spend plan', 'spend forecast'), CAST(NULL AS STRING), TRUE,
    'Drivers kept so channel-level actual vs forecast can be compared.' ),

  ( 'pulseTms.mfcSpendActualPostpaid', 'MFC spend, Postpaid', 'Spend', 'POSTPAID', 'Postpaid', 101, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastPostpaid', TRUE, '%',
    ARRAY('postpaid spend', 'postpaid media spend'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendForecastPostpaid', 'MFC forecast spend, Postpaid', 'Spend', 'POSTPAID', 'Postpaid', 910, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('postpaid forecast spend', 'postpaid spend plan'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendActualBroadband', 'MFC spend, Broadband', 'Spend', 'BROADBAND', 'Broadband', 102, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', 'BROADBAND', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastBroadband', TRUE, '%',
    ARRAY('broadband spend', 'hsi spend'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendForecastBroadband', 'MFC forecast spend, Broadband', 'Spend', 'BROADBAND', 'Broadband', 920, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', 'BROADBAND', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('broadband forecast spend', 'hsi spend plan'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.mfcSpendActualTfb', 'MFC spend, TFB', 'Spend', 'TFB', 'TFB', 103, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendActual', 'MFC_SPEND_ACTUAL', 'TFB', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), 'pulseTms.mfcSpendForecastTfb', TRUE, '%',
    ARRAY('tfb spend', 'business spend', 't-mobile for business spend'), CAST(NULL AS STRING), FALSE,
    'Pre-built for the TFB scope expansion. TFB rows already exist in PulseTMS MFC Gold; set is_active = TRUE (and bump registry_version) to start reporting.' ),

  ( 'pulseTms.mfcSpendForecastTfb', 'MFC forecast spend, TFB', 'Spend', 'TFB', 'TFB', 925, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'MFC_SPEND_CHANNEL', 'mfcSpendForecast', 'MFC_SPEND_FORECAST', 'TFB', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '%',
    ARRAY('tfb forecast spend', 'tfb spend plan'), CAST(NULL AS STRING), FALSE,
    'Activate together with pulseTms.mfcSpendActualTfb.' ),


  /* ===============================================================================================
     BIDDABLE SPEND  (ALL + per LOB)
     =============================================================================================== */

  ( 'pulseTms.biddableSpendTotal', 'Biddable spend, all LOBs', 'Spend', 'TOTAL', 'Total', 120, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', 'ALL', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('biddable spend', 'biddable', 'search social programmatic spend'), CAST(NULL AS STRING), TRUE,
    'PulseTMS Biddable ALL = POSTPAID + BROADBAND + FIBER. Drivers restricted to channel totals (% - All).' ),

  ( 'pulseTms.biddableSpendPostpaid', 'Biddable spend, Postpaid', 'Spend', 'POSTPAID', 'Postpaid', 121, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('postpaid biddable spend'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.biddableSpendBroadband', 'Biddable spend, Broadband', 'Spend', 'BROADBAND', 'Broadband', 122, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', 'BROADBAND', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('broadband biddable spend', 'hsi biddable spend'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.biddableSpendFiber', 'Biddable spend, Fiber', 'Spend', 'FIBER', 'Fiber', 123, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'BIDDABLE_SPEND_CHANNEL', 'biddableSpend', 'BIDDABLE_SPEND', 'FIBER', CAST(NULL AS ARRAY<STRING>), 'All Channels',
    'USD', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), TRUE, '% - All',
    ARRAY('fiber spend', 'fiber biddable spend'), CAST(NULL AS STRING), TRUE,
    'Fiber currently comes from Paid Search only; YoY may be NULL.' ),


  /* ===============================================================================================
     QGP SCORECARD  (actual vs QGP target; channel_group is NULL in PulseTMS Gold)
     =============================================================================================== */

  ( 'pulseTms.qgpActivationsBopisActual', 'Digital phone activations (BOPIS + non-BOPIS)', 'Activations', 'POSTPAID', 'Postpaid', 200, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsBopis', 'QGP_ACTUAL', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpActivationsBopisTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('bopis', 'activations', 'digital activations', 'phone activations'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpActivationsBopisTarget', 'Digital phone activations (BOPIS + non-BOPIS) QGP target', 'Activations', 'POSTPAID', 'Postpaid', 930, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsBopis', 'QGP_TARGET', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('bopis target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpActivationsNewAalNoAssistanceActual', 'Activations, New + AAL, no assistance', 'Activations', 'POSTPAID', 'Postpaid', 210, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsNewAalNoAssistance', 'QGP_ACTUAL', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpActivationsNewAalNoAssistanceTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('new + aal', 'aal activations', 'bts activations'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpActivationsNewAalNoAssistanceTarget', 'Activations, New + AAL, no assistance QGP target', 'Activations', 'POSTPAID', 'Postpaid', 940, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'activationsNewAalNoAssistance', 'QGP_TARGET', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('new + aal target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual', 'Digital % of postpaid activations', 'Activations', 'POSTPAID', 'Postpaid', 220, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_ACTUAL', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'PCT', TRUE, 'pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('digital %', 'digital share', 'digital mix'), CAST(NULL AS STRING), TRUE,
    'Ratio metric. Compare in percentage points (unit = PCT).' ),

  ( 'pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget', 'Digital % of postpaid activations QGP target', 'Activations', 'POSTPAID', 'Postpaid', 950, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_TARGET', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'PCT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('digital % target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpStoreTrafficActual', 'Store traffic', 'Retail', 'NOT_LOB_SPECIFIC', 'Not LOB specific', 300, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'storeTraffic', 'QGP_ACTUAL', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpStoreTrafficTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('store traffic', 'foot traffic', 'retail traffic'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpStoreTrafficTarget', 'Store traffic QGP target', 'Retail', 'NOT_LOB_SPECIFIC', 'Not LOB specific', 960, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'storeTraffic', 'QGP_TARGET', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('store traffic target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrCallsActual', 'Virtual retail calls', 'Virtual Retail', 'NOT_LOB_SPECIFIC', 'Not LOB specific', 400, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrCalls', 'QGP_ACTUAL', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), 'pulseTms.qgpVrCallsTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('vr calls', 'virtual retail calls', 'inbound calls'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrCallsTarget', 'Virtual retail calls QGP target', 'Virtual Retail', 'NOT_LOB_SPECIFIC', 'Not LOB specific', 970, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrCalls', 'QGP_TARGET', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('vr calls target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrChatsActual', 'Virtual retail chats', 'Virtual Retail', 'NOT_LOB_SPECIFIC', 'Not LOB specific', 410, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrChats', 'QGP_ACTUAL', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), 'pulseTms.qgpVrChatsTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('vr chats', 'virtual retail chats', 'chats'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrChatsTarget', 'Virtual retail chats QGP target', 'Virtual Retail', 'NOT_LOB_SPECIFIC', 'Not LOB specific', 980, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrChats', 'QGP_TARGET', CAST(NULL AS STRING), CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', CAST(NULL AS BOOLEAN), CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('vr chats target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrPostpaidActivationsActual', 'Virtual retail postpaid activations', 'Virtual Retail', 'POSTPAID', 'Postpaid', 420, TRUE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrPostpaidActivations', 'QGP_ACTUAL', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, 'pulseTms.qgpVrPostpaidActivationsTarget', FALSE, CAST(NULL AS STRING),
    ARRAY('vr activations', 'virtual retail activations'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) ),

  ( 'pulseTms.qgpVrPostpaidActivationsTarget', 'Virtual retail postpaid activations QGP target', 'Virtual Retail', 'POSTPAID', 'Postpaid', 990, FALSE,
    'PULSE_TMS', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long',
    'QGP_SCORECARD', 'vrPostpaidActivations', 'QGP_TARGET', 'POSTPAID', CAST(NULL AS ARRAY<STRING>), CAST(NULL AS STRING),
    'COUNT', TRUE, CAST(NULL AS STRING), FALSE, CAST(NULL AS STRING),
    ARRAY('vr activations target'), CAST(NULL AS STRING), TRUE,
    CAST(NULL AS STRING) )

AS r(
  metric_key,
  display_name,
  report_section,
  report_lob,
  report_lob_label,
  sort_order,
  is_headline,
  source_project,
  source_object,
  data_source,
  metric_name,
  metric_type,
  true_lob,
  lob_rollup,
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