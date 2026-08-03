/* =================================================================================================
FILE:  test_qgp_end_to_end_raw_bronze_silver_gold.sql
PURPOSE:
  Cross-layer validation query for all 10 PulseTMS QGP metrics. Independently recomputes each
  metric straight off the raw source (deduped the same way Bronze does), then compares that
  ground truth against what Bronze, Silver, and Gold each actually hold for the same week --
  one row per week, all 4 layers x 10 metrics x 2 sides (actual/target) as columns, delta =
  Gold minus Raw for every one of them. A non-zero delta means something diverged somewhere in
  the pipeline between raw and Gold; a NULL on one side where another has a value usually means
  a week/grain misalignment (see the ASSUMPTION note in Silver's own header about week_ending
  vs. qgp_date alignment) rather than a numeric mismatch.

  Every predicate below is copied verbatim from sdi_sp_dashboardPulseTms_silver_qgp_weekly.sql's
  MetricsWide CTE -- the Raw and Bronze layers here are NOT independent reinventions of the
  logic, they're the exact same confirmed filters applied one layer earlier, specifically so
  this script proves the pipeline reproduces itself correctly rather than just agreeing with
  its own logic restated differently.
================================================================================================= */

WITH

-- ===========================================================================
-- LAYER 0: Raw source, deduped exactly the way Bronze's own procedure does
-- (Page in the partition, latest InsertDateTime, PublishKey as tiebreaker).
-- Without this dedup step, the raw layer would double-count the confirmed
-- Page-duplicate rows (Store Traffic, Digital Transformation NEW Outcomes 2,
-- etc.) and every delta below would show a false ~2x mismatch that has
-- nothing to do with the pipeline itself.
-- ===========================================================================
RawDeduped AS (
  SELECT *
  FROM prdrzrlakehouse.qgp_restricted.qgpweeklyview
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY WeekEnding, MetricID, DateContext, MetricType, Page
    ORDER BY InsertDateTime DESC, PublishKey DESC
  ) = 1
),

-- ===========================================================================
-- LAYER 1: RAW -- independently recomputed off RawDeduped using the exact
-- same 10-metric predicates Silver uses, just against raw column names.
-- ===========================================================================
RawWide AS (
SELECT
      raw.WeekEnding,

      -- --- activationsBopis --------------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS'
                AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'   -- corrected from 'digital', see CHANGE LOG #3
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) IN (
                  UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
                  UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
                )
           THEN raw.Amount END)                                              AS activationsBopis_actual,
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'   -- corrected from 'digital', see CHANGE LOG #3
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital')
           THEN raw.Amount END)                                              AS activationsBopis_qgp,

      -- --- activationsNewAalNoAssistance --------------------------------------
      -- Re-scoped to the BTS-family MetricID trio, see header CHANGE LOG #8 for the full
      -- reasoning. Same shape as activationsBopis: sum two Actuals-side component IDs for the
      -- actual, a separate pre-aggregated ID for the QGP/target side.
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS'
                AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) IN (
                  UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),
                  UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital')
                )
           THEN raw.Amount END)                                              AS activationsNewAalNoAssistance_actual,
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped')
           THEN raw.Amount END)                                              AS activationsNewAalNoAssistance_qgp,

      -- --- storeTraffic --------------------------------------------------------
      SUM(CASE WHEN LOWER(TRIM(raw.MetricName)) = 'store traffic (excl store-in-store)'
                AND UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.Page)) = 'BRANDED RETAIL TOTAL'          -- ADDED, see CHANGE LOG #7
                AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
           THEN raw.Amount END)                                              AS storeTraffic_actual,
      SUM(CASE WHEN LOWER(TRIM(raw.MetricName)) = 'store traffic (excl store-in-store)'
                AND UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.Page)) = 'BRANDED RETAIL TOTAL'          -- ADDED, see CHANGE LOG #7
           THEN raw.Amount END)                                              AS storeTraffic_qgp,

      -- --- vrCalls ---------------------------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.Page)) = 'HERO - CORE POSTPAID KP2'      -- ADDED, see CHANGE LOG #7
                AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
           THEN raw.Amount END)                                              AS vrCalls_actual,
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
                AND UPPER(TRIM(raw.DisplayMetricType)) = 'TARGET'         -- corrected from 'QGP', see CHANGE LOG #4
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL'
           THEN raw.Amount END)                                              AS vrCalls_qgp,

      -- --- vrChats ---------------------------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('VRChatsinclHSIAutomatedManual')
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.Page)) = 'HERO - CORE POSTPAID KP2'      -- ADDED, see CHANGE LOG #7
                AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
           THEN raw.Amount END)                                              AS vrChats_actual,
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('VRChatsinclHSIAutomatedManual')
                AND UPPER(TRIM(raw.DisplayMetricType)) = 'TARGET'         -- corrected from 'QGP', see CHANGE LOG #4
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL'
           THEN raw.Amount END)                                              AS vrChats_qgp,

      -- --- vrPostpaidActivations ---------------------------------------------------
      SUM(CASE WHEN CONTAINS(LOWER(TRIM(raw.MetricName)), 'postpaid activations')
                AND UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') -- ⚠ UNCONFIRMED casing, see Bronze header
                AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL OUTCOMES 1'
                AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           THEN raw.Amount END)                                              AS vrPostpaidActivations_actual,
      -- DateContext=Normal added back here vs. your pasted calc, see CHANGE LOG #1
      SUM(CASE WHEN CONTAINS(LOWER(TRIM(raw.MetricName)), 'postpaid activations')
                AND UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') -- ⚠ UNCONFIRMED casing, see Bronze header
                AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL OUTCOMES 1'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           THEN raw.Amount END)                                              AS vrPostpaidActivations_qgp,

      -- --- digitalPctPhoneNewActsNoAssistPlusAssist -------------------------------
      SUM(CASE WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(raw.MetricType)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
                AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
           THEN raw.Amount END)                                              AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
      -- QGP built from the pattern, not directly pasted, see CHANGE LOG #2
      SUM(CASE WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
           THEN raw.Amount END)                                              AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

      -- --- digitalPctConsumerPostpaidActivationsTotalInclAssisted -----------------
      -- Re-scoped to the resolved MetricID/Page, see header CHANGE LOG #9 for the full
      -- reasoning. Unlike activationsBopis/activationsNewAalNoAssistance, actual and QGP read
      -- the SAME MetricID here -- only MetricType differs, matching storeTraffic's pattern.
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(raw.Page)) = 'IT SUMMARY'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
                AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
           THEN raw.Amount END)                                              AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
      SUM(CASE WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
                AND UPPER(TRIM(raw.Page)) = 'IT SUMMARY'
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
           THEN raw.Amount END)                                              AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

      -- --- digitalPctNoAssistanceActivations ---------------------------------------
      SUM(CASE WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(raw.MetricType)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
           THEN raw.Amount END)                                              AS digitalPctNoAssistanceActivations_actual,
      -- CONFIRMED via Bronze diagnostic: this MetricID has zero metric_type='QGP' rows in the
      -- source at all -- not a filter bug, genuinely no Target/plan value. See CHANGE LOG #5.
      CAST(NULL AS DOUBLE)                                                 AS digitalPctNoAssistanceActivations_qgp,

      -- --- digitalPctAssistanceActivations ------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(raw.MetricType)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
                AND UPPER(TRIM(raw.MetricID)) = UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
           THEN raw.Amount END)                                              AS digitalPctAssistanceActivations_actual,
      -- CONFIRMED via Bronze diagnostic: this MetricID has zero metric_type='QGP' rows in the
      -- source at all -- not a filter bug, genuinely no Target/plan value. See CHANGE LOG #5.
      CAST(NULL AS DOUBLE)                                                 AS digitalPctAssistanceActivations_qgp

    FROM RawDeduped raw
    GROUP BY raw.WeekEnding
),

-- ===========================================================================
-- LAYER 2: BRONZE -- identical predicates, reading from the actual Bronze
-- table. If Bronze's own dedup (see its header) did its job, this should
-- exactly equal RawWide for every metric, every week.
-- ===========================================================================
BronzeWide AS (
SELECT
      b.week_ending,

      -- --- activationsBopis --------------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'   -- corrected from 'digital', see CHANGE LOG #3
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) IN (
                  UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
                  UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
                )
           THEN b.amount END)                                              AS activationsBopis_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'   -- corrected from 'digital', see CHANGE LOG #3
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital')
           THEN b.amount END)                                              AS activationsBopis_qgp,

      -- --- activationsNewAalNoAssistance --------------------------------------
      -- Re-scoped to the BTS-family MetricID trio, see header CHANGE LOG #8 for the full
      -- reasoning. Same shape as activationsBopis: sum two Actuals-side component IDs for the
      -- actual, a separate pre-aggregated ID for the QGP/target side.
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) IN (
                  UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),
                  UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital')
                )
           THEN b.amount END)                                              AS activationsNewAalNoAssistance_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped')
           THEN b.amount END)                                              AS activationsNewAalNoAssistance_qgp,

      -- --- storeTraffic --------------------------------------------------------
      SUM(CASE WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
                AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'BRANDED RETAIL TOTAL'          -- ADDED, see CHANGE LOG #7
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS storeTraffic_actual,
      SUM(CASE WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'BRANDED RETAIL TOTAL'          -- ADDED, see CHANGE LOG #7
           THEN b.amount END)                                              AS storeTraffic_qgp,

      -- --- vrCalls ---------------------------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'HERO - CORE POSTPAID KP2'      -- ADDED, see CHANGE LOG #7
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS vrCalls_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'         -- corrected from 'QGP', see CHANGE LOG #4
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
           THEN b.amount END)                                              AS vrCalls_qgp,

      -- --- vrChats ---------------------------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'HERO - CORE POSTPAID KP2'      -- ADDED, see CHANGE LOG #7
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS vrChats_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'         -- corrected from 'QGP', see CHANGE LOG #4
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
           THEN b.amount END)                                              AS vrChats_qgp,

      -- --- vrPostpaidActivations ---------------------------------------------------
      SUM(CASE WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'postpaid activations')
                AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') -- ⚠ UNCONFIRMED casing, see Bronze header
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
                AND b.is_future = FALSE
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS vrPostpaidActivations_actual,
      -- DateContext=Normal added back here vs. your pasted calc, see CHANGE LOG #1
      SUM(CASE WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'postpaid activations')
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') -- ⚠ UNCONFIRMED casing, see Bronze header
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS vrPostpaidActivations_qgp,

      -- --- digitalPctPhoneNewActsNoAssistPlusAssist -------------------------------
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
      -- QGP built from the pattern, not directly pasted, see CHANGE LOG #2
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
           THEN b.amount END)                                              AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

      -- --- digitalPctConsumerPostpaidActivationsTotalInclAssisted -----------------
      -- Re-scoped to the resolved MetricID/Page, see header CHANGE LOG #9 for the full
      -- reasoning. Unlike activationsBopis/activationsNewAalNoAssistance, actual and QGP read
      -- the SAME MetricID here -- only MetricType differs, matching storeTraffic's pattern.
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.page)) = 'IT SUMMARY'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.page)) = 'IT SUMMARY'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
           THEN b.amount END)                                              AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

      -- --- digitalPctNoAssistanceActivations ---------------------------------------
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
           THEN b.amount END)                                              AS digitalPctNoAssistanceActivations_actual,
      -- CONFIRMED via Bronze diagnostic: this MetricID has zero metric_type='QGP' rows in the
      -- source at all -- not a filter bug, genuinely no Target/plan value. See CHANGE LOG #5.
      CAST(NULL AS DOUBLE)                                                 AS digitalPctNoAssistanceActivations_qgp,

      -- --- digitalPctAssistanceActivations ------------------------------------------
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
           THEN b.amount END)                                              AS digitalPctAssistanceActivations_actual,
      -- CONFIRMED via Bronze diagnostic: this MetricID has zero metric_type='QGP' rows in the
      -- source at all -- not a filter bug, genuinely no Target/plan value. See CHANGE LOG #5.
      CAST(NULL AS DOUBLE)                                                 AS digitalPctAssistanceActivations_qgp

    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_qgp_weekly b
    GROUP BY b.week_ending
),

-- ===========================================================================
-- LAYER 3: SILVER -- pulled directly from the Silver table (already
-- unpivoted long; pivoted back to wide here for comparison).
-- ===========================================================================
SilverWide AS (
    SELECT
      qgp_date AS wk,
      MAX(CASE WHEN metric_name = 'activationsBopis' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS activationsBopis_actual,
      MAX(CASE WHEN metric_name = 'activationsBopis' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS activationsBopis_qgp,
      MAX(CASE WHEN metric_name = 'activationsNewAalNoAssistance' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS activationsNewAalNoAssistance_actual,
      MAX(CASE WHEN metric_name = 'activationsNewAalNoAssistance' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS activationsNewAalNoAssistance_qgp,
      MAX(CASE WHEN metric_name = 'storeTraffic' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS storeTraffic_actual,
      MAX(CASE WHEN metric_name = 'storeTraffic' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS storeTraffic_qgp,
      MAX(CASE WHEN metric_name = 'vrCalls' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS vrCalls_actual,
      MAX(CASE WHEN metric_name = 'vrCalls' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS vrCalls_qgp,
      MAX(CASE WHEN metric_name = 'vrChats' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS vrChats_actual,
      MAX(CASE WHEN metric_name = 'vrChats' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS vrChats_qgp,
      MAX(CASE WHEN metric_name = 'vrPostpaidActivations' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS vrPostpaidActivations_actual,
      MAX(CASE WHEN metric_name = 'vrPostpaidActivations' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS vrPostpaidActivations_qgp,
      MAX(CASE WHEN metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
      MAX(CASE WHEN metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,
      MAX(CASE WHEN metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
      MAX(CASE WHEN metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,
      MAX(CASE WHEN metric_name = 'digitalPctNoAssistanceActivations' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS digitalPctNoAssistanceActivations_actual,
      MAX(CASE WHEN metric_name = 'digitalPctNoAssistanceActivations' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS digitalPctNoAssistanceActivations_qgp,
      MAX(CASE WHEN metric_name = 'digitalPctAssistanceActivations' AND metric_type = 'QGP_ACTUAL' THEN metric_value END) AS digitalPctAssistanceActivations_actual,
      MAX(CASE WHEN metric_name = 'digitalPctAssistanceActivations' AND metric_type = 'QGP_TARGET' THEN metric_value END) AS digitalPctAssistanceActivations_qgp
    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly
    GROUP BY qgp_date
  ),

-- ===========================================================================
-- LAYER 4: GOLD -- pulled directly from gold_unified_wide's pre-pivoted
-- qgp* columns. QGP has no real channel_group dimension, so its columns are
-- broadcast identically across every channel_group row for a given week --
-- filtered to a single channel_group here to avoid counting each week
-- multiple times.
-- ===========================================================================
GoldWide AS (
    SELECT
      qgp_date AS wk,
      qgpActivationsBopisActual AS activationsBopis_actual,
      qgpActivationsBopisTarget AS activationsBopis_qgp,
      qgpActivationsNewAalNoAssistanceActual AS activationsNewAalNoAssistance_actual,
      qgpActivationsNewAalNoAssistanceTarget AS activationsNewAalNoAssistance_qgp,
      qgpStoreTrafficActual AS storeTraffic_actual,
      qgpStoreTrafficTarget AS storeTraffic_qgp,
      qgpVrCallsActual AS vrCalls_actual,
      qgpVrCallsTarget AS vrCalls_qgp,
      qgpVrChatsActual AS vrChats_actual,
      qgpVrChatsTarget AS vrChats_qgp,
      qgpVrPostpaidActivationsActual AS vrPostpaidActivations_actual,
      qgpVrPostpaidActivationsTarget AS vrPostpaidActivations_qgp,
      qgpDigitalPctPhoneNewActsNoAssistPlusAssistActual AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
      qgpDigitalPctPhoneNewActsNoAssistPlusAssistTarget AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,
      qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
      qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,
      qgpDigitalPctNoAssistanceActivationsActual AS digitalPctNoAssistanceActivations_actual,
      qgpDigitalPctNoAssistanceActivationsTarget AS digitalPctNoAssistanceActivations_qgp,
      qgpDigitalPctAssistanceActivationsActual AS digitalPctAssistanceActivations_actual,
      qgpDigitalPctAssistanceActivationsTarget AS digitalPctAssistanceActivations_qgp
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide
    WHERE channel_group = 'All Channels'  -- QGP columns are broadcast identically across every channel_group row; pick one to avoid duplicate weeks
  ),

-- ===========================================================================
-- FINAL: one row per week, descending, all 4 layers side by side per metric,
-- plus delta = Gold - Raw. A clean pipeline should show delta = 0 (or NULL
-- where the metric is genuinely null at that week, e.g. before the QGP
-- feed's own data start, or the actuals-reporting lag boundary) everywhere.
-- ===========================================================================
SELECT
  COALESCE(r.wk, b.wk, s.wk, g.wk) AS qgp_date,

  r.activationsBopis_actual  AS activationsBopis_actual_raw,
  b.activationsBopis_actual  AS activationsBopis_actual_bronze,
  s.activationsBopis_actual  AS activationsBopis_actual_silver,
  g.activationsBopis_actual  AS activationsBopis_actual_gold,
  ROUND(g.activationsBopis_actual - r.activationsBopis_actual, 6) AS activationsBopis_actual_delta_gold_minus_raw,
  r.activationsBopis_qgp  AS activationsBopis_target_raw,
  b.activationsBopis_qgp  AS activationsBopis_target_bronze,
  s.activationsBopis_qgp  AS activationsBopis_target_silver,
  g.activationsBopis_qgp  AS activationsBopis_target_gold,
  ROUND(g.activationsBopis_qgp - r.activationsBopis_qgp, 6) AS activationsBopis_target_delta_gold_minus_raw,
  r.activationsNewAalNoAssistance_actual  AS activationsNewAalNoAssistance_actual_raw,
  b.activationsNewAalNoAssistance_actual  AS activationsNewAalNoAssistance_actual_bronze,
  s.activationsNewAalNoAssistance_actual  AS activationsNewAalNoAssistance_actual_silver,
  g.activationsNewAalNoAssistance_actual  AS activationsNewAalNoAssistance_actual_gold,
  ROUND(g.activationsNewAalNoAssistance_actual - r.activationsNewAalNoAssistance_actual, 6) AS activationsNewAalNoAssistance_actual_delta_gold_minus_raw,
  r.activationsNewAalNoAssistance_qgp  AS activationsNewAalNoAssistance_target_raw,
  b.activationsNewAalNoAssistance_qgp  AS activationsNewAalNoAssistance_target_bronze,
  s.activationsNewAalNoAssistance_qgp  AS activationsNewAalNoAssistance_target_silver,
  g.activationsNewAalNoAssistance_qgp  AS activationsNewAalNoAssistance_target_gold,
  ROUND(g.activationsNewAalNoAssistance_qgp - r.activationsNewAalNoAssistance_qgp, 6) AS activationsNewAalNoAssistance_target_delta_gold_minus_raw,
  r.storeTraffic_actual  AS storeTraffic_actual_raw,
  b.storeTraffic_actual  AS storeTraffic_actual_bronze,
  s.storeTraffic_actual  AS storeTraffic_actual_silver,
  g.storeTraffic_actual  AS storeTraffic_actual_gold,
  ROUND(g.storeTraffic_actual - r.storeTraffic_actual, 6) AS storeTraffic_actual_delta_gold_minus_raw,
  r.storeTraffic_qgp  AS storeTraffic_target_raw,
  b.storeTraffic_qgp  AS storeTraffic_target_bronze,
  s.storeTraffic_qgp  AS storeTraffic_target_silver,
  g.storeTraffic_qgp  AS storeTraffic_target_gold,
  ROUND(g.storeTraffic_qgp - r.storeTraffic_qgp, 6) AS storeTraffic_target_delta_gold_minus_raw,
  r.vrCalls_actual  AS vrCalls_actual_raw,
  b.vrCalls_actual  AS vrCalls_actual_bronze,
  s.vrCalls_actual  AS vrCalls_actual_silver,
  g.vrCalls_actual  AS vrCalls_actual_gold,
  ROUND(g.vrCalls_actual - r.vrCalls_actual, 6) AS vrCalls_actual_delta_gold_minus_raw,
  r.vrCalls_qgp  AS vrCalls_target_raw,
  b.vrCalls_qgp  AS vrCalls_target_bronze,
  s.vrCalls_qgp  AS vrCalls_target_silver,
  g.vrCalls_qgp  AS vrCalls_target_gold,
  ROUND(g.vrCalls_qgp - r.vrCalls_qgp, 6) AS vrCalls_target_delta_gold_minus_raw,
  r.vrChats_actual  AS vrChats_actual_raw,
  b.vrChats_actual  AS vrChats_actual_bronze,
  s.vrChats_actual  AS vrChats_actual_silver,
  g.vrChats_actual  AS vrChats_actual_gold,
  ROUND(g.vrChats_actual - r.vrChats_actual, 6) AS vrChats_actual_delta_gold_minus_raw,
  r.vrChats_qgp  AS vrChats_target_raw,
  b.vrChats_qgp  AS vrChats_target_bronze,
  s.vrChats_qgp  AS vrChats_target_silver,
  g.vrChats_qgp  AS vrChats_target_gold,
  ROUND(g.vrChats_qgp - r.vrChats_qgp, 6) AS vrChats_target_delta_gold_minus_raw,
  r.vrPostpaidActivations_actual  AS vrPostpaidActivations_actual_raw,
  b.vrPostpaidActivations_actual  AS vrPostpaidActivations_actual_bronze,
  s.vrPostpaidActivations_actual  AS vrPostpaidActivations_actual_silver,
  g.vrPostpaidActivations_actual  AS vrPostpaidActivations_actual_gold,
  ROUND(g.vrPostpaidActivations_actual - r.vrPostpaidActivations_actual, 6) AS vrPostpaidActivations_actual_delta_gold_minus_raw,
  r.vrPostpaidActivations_qgp  AS vrPostpaidActivations_target_raw,
  b.vrPostpaidActivations_qgp  AS vrPostpaidActivations_target_bronze,
  s.vrPostpaidActivations_qgp  AS vrPostpaidActivations_target_silver,
  g.vrPostpaidActivations_qgp  AS vrPostpaidActivations_target_gold,
  ROUND(g.vrPostpaidActivations_qgp - r.vrPostpaidActivations_qgp, 6) AS vrPostpaidActivations_target_delta_gold_minus_raw,
  r.digitalPctPhoneNewActsNoAssistPlusAssist_actual  AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_raw,
  b.digitalPctPhoneNewActsNoAssistPlusAssist_actual  AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_bronze,
  s.digitalPctPhoneNewActsNoAssistPlusAssist_actual  AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_silver,
  g.digitalPctPhoneNewActsNoAssistPlusAssist_actual  AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_gold,
  ROUND(g.digitalPctPhoneNewActsNoAssistPlusAssist_actual - r.digitalPctPhoneNewActsNoAssistPlusAssist_actual, 6) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_delta_gold_minus_raw,
  r.digitalPctPhoneNewActsNoAssistPlusAssist_qgp  AS digitalPctPhoneNewActsNoAssistPlusAssist_target_raw,
  b.digitalPctPhoneNewActsNoAssistPlusAssist_qgp  AS digitalPctPhoneNewActsNoAssistPlusAssist_target_bronze,
  s.digitalPctPhoneNewActsNoAssistPlusAssist_qgp  AS digitalPctPhoneNewActsNoAssistPlusAssist_target_silver,
  g.digitalPctPhoneNewActsNoAssistPlusAssist_qgp  AS digitalPctPhoneNewActsNoAssistPlusAssist_target_gold,
  ROUND(g.digitalPctPhoneNewActsNoAssistPlusAssist_qgp - r.digitalPctPhoneNewActsNoAssistPlusAssist_qgp, 6) AS digitalPctPhoneNewActsNoAssistPlusAssist_target_delta_gold_minus_raw,
  r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_raw,
  b.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_bronze,
  s.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_silver,
  g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_gold,
  ROUND(g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual - r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual, 6) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_delta_gold_minus_raw,
  r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_raw,
  b.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_bronze,
  s.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_silver,
  g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp  AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_gold,
  ROUND(g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp - r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp, 6) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_delta_gold_minus_raw,
  r.digitalPctNoAssistanceActivations_actual  AS digitalPctNoAssistanceActivations_actual_raw,
  b.digitalPctNoAssistanceActivations_actual  AS digitalPctNoAssistanceActivations_actual_bronze,
  s.digitalPctNoAssistanceActivations_actual  AS digitalPctNoAssistanceActivations_actual_silver,
  g.digitalPctNoAssistanceActivations_actual  AS digitalPctNoAssistanceActivations_actual_gold,
  ROUND(g.digitalPctNoAssistanceActivations_actual - r.digitalPctNoAssistanceActivations_actual, 6) AS digitalPctNoAssistanceActivations_actual_delta_gold_minus_raw,
  r.digitalPctNoAssistanceActivations_qgp  AS digitalPctNoAssistanceActivations_target_raw,
  b.digitalPctNoAssistanceActivations_qgp  AS digitalPctNoAssistanceActivations_target_bronze,
  s.digitalPctNoAssistanceActivations_qgp  AS digitalPctNoAssistanceActivations_target_silver,
  g.digitalPctNoAssistanceActivations_qgp  AS digitalPctNoAssistanceActivations_target_gold,
  ROUND(g.digitalPctNoAssistanceActivations_qgp - r.digitalPctNoAssistanceActivations_qgp, 6) AS digitalPctNoAssistanceActivations_target_delta_gold_minus_raw,
  r.digitalPctAssistanceActivations_actual  AS digitalPctAssistanceActivations_actual_raw,
  b.digitalPctAssistanceActivations_actual  AS digitalPctAssistanceActivations_actual_bronze,
  s.digitalPctAssistanceActivations_actual  AS digitalPctAssistanceActivations_actual_silver,
  g.digitalPctAssistanceActivations_actual  AS digitalPctAssistanceActivations_actual_gold,
  ROUND(g.digitalPctAssistanceActivations_actual - r.digitalPctAssistanceActivations_actual, 6) AS digitalPctAssistanceActivations_actual_delta_gold_minus_raw,
  r.digitalPctAssistanceActivations_qgp  AS digitalPctAssistanceActivations_target_raw,
  b.digitalPctAssistanceActivations_qgp  AS digitalPctAssistanceActivations_target_bronze,
  s.digitalPctAssistanceActivations_qgp  AS digitalPctAssistanceActivations_target_silver,
  g.digitalPctAssistanceActivations_qgp  AS digitalPctAssistanceActivations_target_gold,
  ROUND(g.digitalPctAssistanceActivations_qgp - r.digitalPctAssistanceActivations_qgp, 6) AS digitalPctAssistanceActivations_target_delta_gold_minus_raw

FROM RawWide r
FULL OUTER JOIN BronzeWide b ON b.wk = r.wk
FULL OUTER JOIN SilverWide s ON s.wk = COALESCE(r.wk, b.wk)
FULL OUTER JOIN GoldWide   g ON g.wk = COALESCE(r.wk, b.wk, s.wk)
ORDER BY qgp_date DESC;