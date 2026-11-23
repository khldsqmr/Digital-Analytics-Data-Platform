/* =================================================================================================
FILE:  test_qgp_end_to_end_raw_bronze_silver_gold.sql
PURPOSE:
  Cross-layer validation query for all 10 PulseTMS QGP metrics. Independently recomputes each
  metric straight off the raw source, deduped the same way Bronze does, then compares that
  ground truth against what Bronze, Silver, and Gold each actually hold for the same week.

  Output:
    - One row per qgp_date/week.
    - Raw, Bronze, Silver, Gold values side by side.
    - Delta = Gold minus Raw for every metric actual/target side.

  Notes:
    - Raw is deduped by WeekEnding, MetricID, DateContext, MetricType, Page using latest
      InsertDateTime and PublishKey as tiebreaker.
    - Week keys are cast to DATE across all layers to avoid timestamp/date join drift.
    - Gold is aggregated to one row per qgp_date after filtering channel_group = 'All Channels'
      to prevent duplicate week rows if the view has additional dimensions.
================================================================================================= */

WITH

-- ===========================================================================
-- LAYER 0: Raw source, deduped exactly the way Bronze's own procedure does
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
-- LAYER 1: RAW
-- ===========================================================================
RawWide AS (
  SELECT
    CAST(raw.WeekEnding AS DATE) AS wk,

    -- --- activationsBopis --------------------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS'
           AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) IN (
                UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
                UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
              )
          THEN raw.Amount
        END) AS activationsBopis_actual,

    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital')
          THEN raw.Amount
        END) AS activationsBopis_qgp,

    -- --- activationsNewAalNoAssistance ------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS'
           AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) IN (
                UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),
                UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital')
              )
          THEN raw.Amount
        END) AS activationsNewAalNoAssistance_actual,

    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped')
          THEN raw.Amount
        END) AS activationsNewAalNoAssistance_qgp,

    -- --- storeTraffic ------------------------------------------------------
    SUM(CASE
          WHEN LOWER(TRIM(raw.MetricName)) = 'store traffic (excl store-in-store)'
           AND UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.Page)) = 'BRANDED RETAIL TOTAL'
           AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
          THEN raw.Amount
        END) AS storeTraffic_actual,

    SUM(CASE
          WHEN LOWER(TRIM(raw.MetricName)) = 'store traffic (excl store-in-store)'
           AND UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.Page)) = 'BRANDED RETAIL TOTAL'
          THEN raw.Amount
        END) AS storeTraffic_qgp,

    -- --- vrCalls -----------------------------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.Page)) = 'HERO - CORE POSTPAID KP2'
           AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
          THEN raw.Amount
        END) AS vrCalls_actual,

    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
           AND UPPER(TRIM(raw.DisplayMetricType)) = 'TARGET'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL'
          THEN raw.Amount
        END) AS vrCalls_qgp,

    -- --- vrChats -----------------------------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('VRChatsinclHSIAutomatedManual')
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.Page)) = 'HERO - CORE POSTPAID KP2'
           AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
          THEN raw.Amount
        END) AS vrChats_actual,

    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('VRChatsinclHSIAutomatedManual')
           AND UPPER(TRIM(raw.DisplayMetricType)) = 'TARGET'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL'
          THEN raw.Amount
        END) AS vrChats_qgp,

    -- --- vrPostpaidActivations --------------------------------------------
    SUM(CASE
          WHEN LOWER(TRIM(raw.MetricName)) LIKE '%postpaid activations%'
           AND UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped')
           AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL OUTCOMES 1'
           AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
          THEN raw.Amount
        END) AS vrPostpaidActivations_actual,

    SUM(CASE
          WHEN LOWER(TRIM(raw.MetricName)) LIKE '%postpaid activations%'
           AND UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped')
           AND UPPER(TRIM(raw.Page)) = 'VIRTUAL RETAIL OUTCOMES 1'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
          THEN raw.Amount
        END) AS vrPostpaidActivations_qgp,

    -- --- digitalPctPhoneNewActsNoAssistPlusAssist --------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.MetricType)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
           AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
          THEN raw.Amount
        END) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,

    SUM(CASE
          WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
          THEN raw.Amount
        END) AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

    -- --- digitalPctConsumerPostpaidActivationsTotalInclAssisted ------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(raw.Page)) = 'IT SUMMARY'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
           AND LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'
          THEN raw.Amount
        END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,

    SUM(CASE
          WHEN UPPER(TRIM(raw.MetricType)) = 'QGP'
           AND UPPER(TRIM(raw.Page)) = 'IT SUMMARY'
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
          THEN raw.Amount
        END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

    -- --- digitalPctNoAssistanceActivations --------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.MetricType)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
          THEN raw.Amount
        END) AS digitalPctNoAssistanceActivations_actual,

    CAST(NULL AS DOUBLE) AS digitalPctNoAssistanceActivations_qgp,

    -- --- digitalPctAssistanceActivations -----------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(raw.Page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(raw.MetricType)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(raw.DateContext)) = 'NORMAL'
           AND UPPER(TRIM(raw.MetricID)) = UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
          THEN raw.Amount
        END) AS digitalPctAssistanceActivations_actual,

    CAST(NULL AS DOUBLE) AS digitalPctAssistanceActivations_qgp

  FROM RawDeduped raw
  GROUP BY CAST(raw.WeekEnding AS DATE)
),

-- ===========================================================================
-- LAYER 2: BRONZE
-- ===========================================================================
BronzeWide AS (
  SELECT
    CAST(b.week_ending AS DATE) AS wk,

    -- --- activationsBopis --------------------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) IN (
                UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
                UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
              )
          THEN b.amount
        END) AS activationsBopis_actual,

    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital')
          THEN b.amount
        END) AS activationsBopis_qgp,

    -- --- activationsNewAalNoAssistance ------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) IN (
                UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),
                UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital')
              )
          THEN b.amount
        END) AS activationsNewAalNoAssistance_actual,

    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped')
          THEN b.amount
        END) AS activationsNewAalNoAssistance_qgp,

    -- --- storeTraffic ------------------------------------------------------
    SUM(CASE
          WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
           AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'BRANDED RETAIL TOTAL'
           AND b.is_future = FALSE
          THEN b.amount
        END) AS storeTraffic_actual,

    SUM(CASE
          WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
           AND UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'BRANDED RETAIL TOTAL'
          THEN b.amount
        END) AS storeTraffic_qgp,

    -- --- vrCalls -----------------------------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'HERO - CORE POSTPAID KP2'
           AND b.is_future = FALSE
          THEN b.amount
        END) AS vrCalls_actual,

    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
          THEN b.amount
        END) AS vrCalls_qgp,

    -- --- vrChats -----------------------------------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'HERO - CORE POSTPAID KP2'
           AND b.is_future = FALSE
          THEN b.amount
        END) AS vrChats_actual,

    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
          THEN b.amount
        END) AS vrChats_qgp,

    -- --- vrPostpaidActivations --------------------------------------------
    SUM(CASE
          WHEN LOWER(TRIM(b.metric_name_label)) LIKE '%postpaid activations%'
           AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped')
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
           AND b.is_future = FALSE
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
          THEN b.amount
        END) AS vrPostpaidActivations_actual,

    SUM(CASE
          WHEN LOWER(TRIM(b.metric_name_label)) LIKE '%postpaid activations%'
           AND UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped')
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
          THEN b.amount
        END) AS vrPostpaidActivations_qgp,

    -- --- digitalPctPhoneNewActsNoAssistPlusAssist --------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
           AND b.is_future = FALSE
          THEN b.amount
        END) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,

    SUM(CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
          THEN b.amount
        END) AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

    -- --- digitalPctConsumerPostpaidActivationsTotalInclAssisted ------------
    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.page)) = 'IT SUMMARY'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
           AND b.is_future = FALSE
          THEN b.amount
        END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,

    SUM(CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'IT SUMMARY'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
          THEN b.amount
        END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

    -- --- digitalPctNoAssistanceActivations --------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
          THEN b.amount
        END) AS digitalPctNoAssistanceActivations_actual,

    CAST(NULL AS DOUBLE) AS digitalPctNoAssistanceActivations_qgp,

    -- --- digitalPctAssistanceActivations -----------------------------------
    SUM(CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) = UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
          THEN b.amount
        END) AS digitalPctAssistanceActivations_actual,

    CAST(NULL AS DOUBLE) AS digitalPctAssistanceActivations_qgp

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_qgp_weekly b
  GROUP BY CAST(b.week_ending AS DATE)
),

-- ===========================================================================
-- LAYER 3: SILVER
-- ===========================================================================
SilverWide AS (
  SELECT
    CAST(qgp_date AS DATE) AS wk,

    MAX(CASE WHEN metric_name = 'activationsBopis'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS activationsBopis_actual,
    MAX(CASE WHEN metric_name = 'activationsBopis'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS activationsBopis_qgp,

    MAX(CASE WHEN metric_name = 'activationsNewAalNoAssistance'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS activationsNewAalNoAssistance_actual,
    MAX(CASE WHEN metric_name = 'activationsNewAalNoAssistance'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS activationsNewAalNoAssistance_qgp,

    MAX(CASE WHEN metric_name = 'storeTraffic'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS storeTraffic_actual,
    MAX(CASE WHEN metric_name = 'storeTraffic'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS storeTraffic_qgp,

    MAX(CASE WHEN metric_name = 'vrCalls'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS vrCalls_actual,
    MAX(CASE WHEN metric_name = 'vrCalls'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS vrCalls_qgp,

    MAX(CASE WHEN metric_name = 'vrChats'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS vrChats_actual,
    MAX(CASE WHEN metric_name = 'vrChats'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS vrChats_qgp,

    MAX(CASE WHEN metric_name = 'vrPostpaidActivations'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS vrPostpaidActivations_actual,
    MAX(CASE WHEN metric_name = 'vrPostpaidActivations'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS vrPostpaidActivations_qgp,

    MAX(CASE WHEN metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
    MAX(CASE WHEN metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

    MAX(CASE WHEN metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
    MAX(CASE WHEN metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

    MAX(CASE WHEN metric_name = 'digitalPctNoAssistanceActivations'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS digitalPctNoAssistanceActivations_actual,
    MAX(CASE WHEN metric_name = 'digitalPctNoAssistanceActivations'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS digitalPctNoAssistanceActivations_qgp,

    MAX(CASE WHEN metric_name = 'digitalPctAssistanceActivations'
              AND UPPER(TRIM(metric_type)) = 'QGP_ACTUAL'
             THEN metric_value END) AS digitalPctAssistanceActivations_actual,
    MAX(CASE WHEN metric_name = 'digitalPctAssistanceActivations'
              AND UPPER(TRIM(metric_type)) = 'QGP_TARGET'
             THEN metric_value END) AS digitalPctAssistanceActivations_qgp

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly
  GROUP BY CAST(qgp_date AS DATE)
),

-- ===========================================================================
-- LAYER 4: GOLD
-- Aggregate to one row per week to protect against duplicate rows from other
-- dimensions in gold_unified_wide.
-- ===========================================================================
GoldWide AS (
  SELECT
    CAST(qgp_date AS DATE) AS wk,

    MAX(qgpActivationsBopisActual) AS activationsBopis_actual,
    MAX(qgpActivationsBopisTarget) AS activationsBopis_qgp,

    MAX(qgpActivationsNewAalNoAssistanceActual) AS activationsNewAalNoAssistance_actual,
    MAX(qgpActivationsNewAalNoAssistanceTarget) AS activationsNewAalNoAssistance_qgp,

    MAX(qgpStoreTrafficActual) AS storeTraffic_actual,
    MAX(qgpStoreTrafficTarget) AS storeTraffic_qgp,

    MAX(qgpVrCallsActual) AS vrCalls_actual,
    MAX(qgpVrCallsTarget) AS vrCalls_qgp,

    MAX(qgpVrChatsActual) AS vrChats_actual,
    MAX(qgpVrChatsTarget) AS vrChats_qgp,

    MAX(qgpVrPostpaidActivationsActual) AS vrPostpaidActivations_actual,
    MAX(qgpVrPostpaidActivationsTarget) AS vrPostpaidActivations_qgp,

    MAX(qgpDigitalPctPhoneNewActsNoAssistPlusAssistActual) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
    MAX(qgpDigitalPctPhoneNewActsNoAssistPlusAssistTarget) AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

    MAX(qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
    MAX(qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

    MAX(qgpDigitalPctNoAssistanceActivationsActual) AS digitalPctNoAssistanceActivations_actual,
    MAX(qgpDigitalPctNoAssistanceActivationsTarget) AS digitalPctNoAssistanceActivations_qgp,

    MAX(qgpDigitalPctAssistanceActivationsActual) AS digitalPctAssistanceActivations_actual,
    MAX(qgpDigitalPctAssistanceActivationsTarget) AS digitalPctAssistanceActivations_qgp

  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide
  WHERE channel_group = 'All Channels'
  GROUP BY CAST(qgp_date AS DATE)
),

-- ===========================================================================
-- One complete week spine across all four layers
-- ===========================================================================
AllWeeks AS (
  SELECT wk FROM RawWide
  UNION
  SELECT wk FROM BronzeWide
  UNION
  SELECT wk FROM SilverWide
  UNION
  SELECT wk FROM GoldWide
)

-- ===========================================================================
-- FINAL
-- ===========================================================================
SELECT
  aw.wk AS qgp_date,

  r.activationsBopis_actual AS activationsBopis_actual_raw,
  b.activationsBopis_actual AS activationsBopis_actual_bronze,
  s.activationsBopis_actual AS activationsBopis_actual_silver,
  g.activationsBopis_actual AS activationsBopis_actual_gold,
  ROUND(g.activationsBopis_actual - r.activationsBopis_actual, 6) AS activationsBopis_actual_delta_gold_minus_raw,

  r.activationsBopis_qgp AS activationsBopis_target_raw,
  b.activationsBopis_qgp AS activationsBopis_target_bronze,
  s.activationsBopis_qgp AS activationsBopis_target_silver,
  g.activationsBopis_qgp AS activationsBopis_target_gold,
  ROUND(g.activationsBopis_qgp - r.activationsBopis_qgp, 6) AS activationsBopis_target_delta_gold_minus_raw,

  r.activationsNewAalNoAssistance_actual AS activationsNewAalNoAssistance_actual_raw,
  b.activationsNewAalNoAssistance_actual AS activationsNewAalNoAssistance_actual_bronze,
  s.activationsNewAalNoAssistance_actual AS activationsNewAalNoAssistance_actual_silver,
  g.activationsNewAalNoAssistance_actual AS activationsNewAalNoAssistance_actual_gold,
  ROUND(g.activationsNewAalNoAssistance_actual - r.activationsNewAalNoAssistance_actual, 6) AS activationsNewAalNoAssistance_actual_delta_gold_minus_raw,

  r.activationsNewAalNoAssistance_qgp AS activationsNewAalNoAssistance_target_raw,
  b.activationsNewAalNoAssistance_qgp AS activationsNewAalNoAssistance_target_bronze,
  s.activationsNewAalNoAssistance_qgp AS activationsNewAalNoAssistance_target_silver,
  g.activationsNewAalNoAssistance_qgp AS activationsNewAalNoAssistance_target_gold,
  ROUND(g.activationsNewAalNoAssistance_qgp - r.activationsNewAalNoAssistance_qgp, 6) AS activationsNewAalNoAssistance_target_delta_gold_minus_raw,

  r.storeTraffic_actual AS storeTraffic_actual_raw,
  b.storeTraffic_actual AS storeTraffic_actual_bronze,
  s.storeTraffic_actual AS storeTraffic_actual_silver,
  g.storeTraffic_actual AS storeTraffic_actual_gold,
  ROUND(g.storeTraffic_actual - r.storeTraffic_actual, 6) AS storeTraffic_actual_delta_gold_minus_raw,

  r.storeTraffic_qgp AS storeTraffic_target_raw,
  b.storeTraffic_qgp AS storeTraffic_target_bronze,
  s.storeTraffic_qgp AS storeTraffic_target_silver,
  g.storeTraffic_qgp AS storeTraffic_target_gold,
  ROUND(g.storeTraffic_qgp - r.storeTraffic_qgp, 6) AS storeTraffic_target_delta_gold_minus_raw,

  r.vrCalls_actual AS vrCalls_actual_raw,
  b.vrCalls_actual AS vrCalls_actual_bronze,
  s.vrCalls_actual AS vrCalls_actual_silver,
  g.vrCalls_actual AS vrCalls_actual_gold,
  ROUND(g.vrCalls_actual - r.vrCalls_actual, 6) AS vrCalls_actual_delta_gold_minus_raw,

  r.vrCalls_qgp AS vrCalls_target_raw,
  b.vrCalls_qgp AS vrCalls_target_bronze,
  s.vrCalls_qgp AS vrCalls_target_silver,
  g.vrCalls_qgp AS vrCalls_target_gold,
  ROUND(g.vrCalls_qgp - r.vrCalls_qgp, 6) AS vrCalls_target_delta_gold_minus_raw,

  r.vrChats_actual AS vrChats_actual_raw,
  b.vrChats_actual AS vrChats_actual_bronze,
  s.vrChats_actual AS vrChats_actual_silver,
  g.vrChats_actual AS vrChats_actual_gold,
  ROUND(g.vrChats_actual - r.vrChats_actual, 6) AS vrChats_actual_delta_gold_minus_raw,

  r.vrChats_qgp AS vrChats_target_raw,
  b.vrChats_qgp AS vrChats_target_bronze,
  s.vrChats_qgp AS vrChats_target_silver,
  g.vrChats_qgp AS vrChats_target_gold,
  ROUND(g.vrChats_qgp - r.vrChats_qgp, 6) AS vrChats_target_delta_gold_minus_raw,

  r.vrPostpaidActivations_actual AS vrPostpaidActivations_actual_raw,
  b.vrPostpaidActivations_actual AS vrPostpaidActivations_actual_bronze,
  s.vrPostpaidActivations_actual AS vrPostpaidActivations_actual_silver,
  g.vrPostpaidActivations_actual AS vrPostpaidActivations_actual_gold,
  ROUND(g.vrPostpaidActivations_actual - r.vrPostpaidActivations_actual, 6) AS vrPostpaidActivations_actual_delta_gold_minus_raw,

  r.vrPostpaidActivations_qgp AS vrPostpaidActivations_target_raw,
  b.vrPostpaidActivations_qgp AS vrPostpaidActivations_target_bronze,
  s.vrPostpaidActivations_qgp AS vrPostpaidActivations_target_silver,
  g.vrPostpaidActivations_qgp AS vrPostpaidActivations_target_gold,
  ROUND(g.vrPostpaidActivations_qgp - r.vrPostpaidActivations_qgp, 6) AS vrPostpaidActivations_target_delta_gold_minus_raw,

  r.digitalPctPhoneNewActsNoAssistPlusAssist_actual AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_raw,
  b.digitalPctPhoneNewActsNoAssistPlusAssist_actual AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_bronze,
  s.digitalPctPhoneNewActsNoAssistPlusAssist_actual AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_silver,
  g.digitalPctPhoneNewActsNoAssistPlusAssist_actual AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_gold,
  ROUND(g.digitalPctPhoneNewActsNoAssistPlusAssist_actual - r.digitalPctPhoneNewActsNoAssistPlusAssist_actual, 6) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual_delta_gold_minus_raw,

  r.digitalPctPhoneNewActsNoAssistPlusAssist_qgp AS digitalPctPhoneNewActsNoAssistPlusAssist_target_raw,
  b.digitalPctPhoneNewActsNoAssistPlusAssist_qgp AS digitalPctPhoneNewActsNoAssistPlusAssist_target_bronze,
  s.digitalPctPhoneNewActsNoAssistPlusAssist_qgp AS digitalPctPhoneNewActsNoAssistPlusAssist_target_silver,
  g.digitalPctPhoneNewActsNoAssistPlusAssist_qgp AS digitalPctPhoneNewActsNoAssistPlusAssist_target_gold,
  ROUND(g.digitalPctPhoneNewActsNoAssistPlusAssist_qgp - r.digitalPctPhoneNewActsNoAssistPlusAssist_qgp, 6) AS digitalPctPhoneNewActsNoAssistPlusAssist_target_delta_gold_minus_raw,

  r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_raw,
  b.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_bronze,
  s.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_silver,
  g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_gold,
  ROUND(g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual - r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual, 6) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual_delta_gold_minus_raw,

  r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_raw,
  b.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_bronze,
  s.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_silver,
  g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_gold,
  ROUND(g.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp - r.digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp, 6) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_target_delta_gold_minus_raw,

  r.digitalPctNoAssistanceActivations_actual AS digitalPctNoAssistanceActivations_actual_raw,
  b.digitalPctNoAssistanceActivations_actual AS digitalPctNoAssistanceActivations_actual_bronze,
  s.digitalPctNoAssistanceActivations_actual AS digitalPctNoAssistanceActivations_actual_silver,
  g.digitalPctNoAssistanceActivations_actual AS digitalPctNoAssistanceActivations_actual_gold,
  ROUND(g.digitalPctNoAssistanceActivations_actual - r.digitalPctNoAssistanceActivations_actual, 6) AS digitalPctNoAssistanceActivations_actual_delta_gold_minus_raw,

  r.digitalPctNoAssistanceActivations_qgp AS digitalPctNoAssistanceActivations_target_raw,
  b.digitalPctNoAssistanceActivations_qgp AS digitalPctNoAssistanceActivations_target_bronze,
  s.digitalPctNoAssistanceActivations_qgp AS digitalPctNoAssistanceActivations_target_silver,
  g.digitalPctNoAssistanceActivations_qgp AS digitalPctNoAssistanceActivations_target_gold,
  ROUND(g.digitalPctNoAssistanceActivations_qgp - r.digitalPctNoAssistanceActivations_qgp, 6) AS digitalPctNoAssistanceActivations_target_delta_gold_minus_raw,

  r.digitalPctAssistanceActivations_actual AS digitalPctAssistanceActivations_actual_raw,
  b.digitalPctAssistanceActivations_actual AS digitalPctAssistanceActivations_actual_bronze,
  s.digitalPctAssistanceActivations_actual AS digitalPctAssistanceActivations_actual_silver,
  g.digitalPctAssistanceActivations_actual AS digitalPctAssistanceActivations_actual_gold,
  ROUND(g.digitalPctAssistanceActivations_actual - r.digitalPctAssistanceActivations_actual, 6) AS digitalPctAssistanceActivations_actual_delta_gold_minus_raw,

  r.digitalPctAssistanceActivations_qgp AS digitalPctAssistanceActivations_target_raw,
  b.digitalPctAssistanceActivations_qgp AS digitalPctAssistanceActivations_target_bronze,
  s.digitalPctAssistanceActivations_qgp AS digitalPctAssistanceActivations_target_silver,
  g.digitalPctAssistanceActivations_qgp AS digitalPctAssistanceActivations_target_gold,
  ROUND(g.digitalPctAssistanceActivations_qgp - r.digitalPctAssistanceActivations_qgp, 6) AS digitalPctAssistanceActivations_target_delta_gold_minus_raw

FROM AllWeeks aw
LEFT JOIN RawWide    r ON r.wk = aw.wk
LEFT JOIN BronzeWide b ON b.wk = aw.wk
LEFT JOIN SilverWide s ON s.wk = aw.wk
LEFT JOIN GoldWide   g ON g.wk = aw.wk
ORDER BY aw.wk DESC;
