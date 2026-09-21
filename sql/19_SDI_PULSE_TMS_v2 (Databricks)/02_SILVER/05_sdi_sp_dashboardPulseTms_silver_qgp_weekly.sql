/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_qgp_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_qgp_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_qgp_weekly.

  Reproduces the 12 named QGP metrics from the existing Tableau/business definitions off
  sdi_tbl_dashboardPulseTms_bronze_qgp_weekly, reshaped into the same long
  metric_name/metric_type/metric_value format as the rest of PulseTMS Silver, with
  WoW/YoY computed the same way as adobeFunnel/mfcSpend/platformSpend Silver.

METRICS (metric_name -- Actual filter -- QGP/target filter -- status):

  activationsBopis
    Actual: MetricType=Actuals, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID IN (
              ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital,
              ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital
            )
    QGP:    MetricType=QGP, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID=ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital
    Status: LIVE. Existing combined Phone BOPIS + Non-BOPIS metric remains unchanged.

  activationsBopisOnly
    Actual: MetricType=Actuals, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID=ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital
    QGP:    MetricType=QGP, Page=Digital Transformation NEW, DateContext=Normal,
            same BOPIS-only MetricID
    Status: Added as the BOPIS-only component. QGP is read directly from the component
            MetricID when available; otherwise the target naturally remains NULL.

  activationsNonBopisOnly
    Actual: MetricType=Actuals, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID=ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital
    QGP:    MetricType=QGP, Page=Digital Transformation NEW, DateContext=Normal,
            same Non-BOPIS-only MetricID
    Status: Added as the Non-BOPIS-only component. QGP is read directly from the component
            MetricID when available; otherwise the target naturally remains NULL.

  activationsNewAalNoAssistance
    Actual: MetricType=Actuals, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID IN (
              ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital,
              ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital
            )
    QGP:    MetricType=QGP, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID=TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped
    Status: LIVE, both sides confirmed working against real data. Originally scoped under a
            different, nonexistent Phone MetricID -- see CHANGE LOG #8.

  storeTraffic
    Actual: MetricName='store traffic (excl store-in-store)', MetricType=Actuals/Outlook,
            DateContext=Normal, Page=Branded Retail Total, IsFuture=Is Past
    QGP:    same name match, MetricType=QGP, DateContext=Normal, Page=Branded Retail Total
    Status: LIVE. Page filter added to avoid duplicate-page double counting.

  vrCalls
    Actual: MetricType=Actuals/Outlook, MetricID=VRInboundCallsinclHSIAutomatedManual,
            Page=Hero - Core Postpaid KP2, DateContext=Normal, IsFuture=Is Past
    QGP:    MetricType=QGP, same ID, DisplayMetricType=Target,
            DateContext=Normal, Page=Virtual Retail
    Status: LIVE.

  vrChats
    Actual/QGP shape same as vrCalls.
    MetricID=VRChatsinclHSIAutomatedManual
    Status: LIVE.

  vrPostpaidActivations
    Actual: MetricName contains 'postpaid activations', MetricType=Actuals/Outlook,
            MetricID=VRPostpaidActivationsinclVirtualBusinessTM1Mapped,
            Page=Virtual Retail Outcomes 1, IsFuture=Is Past, DateContext=Normal
    QGP:    same substring/ID/Page, MetricType=QGP, DateContext=Normal
    Status: LIVE.

  digitalPctPhoneNewActsNoAssistPlusAssist
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal,
            MetricID=ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect,
            IsFuture=Is Past
    QGP:    same Page/MetricID/DateContext, MetricType=QGP
    Status: LIVE.

  digitalPctConsumerPostpaidActivationsTotalInclAssisted
    Actual: MetricType=Actuals/Outlook, Page=IT Summary, DateContext=Normal,
            MetricID=DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA,
            IsFuture=Is Past
    QGP:    same MetricID, MetricType=QGP, Page=IT Summary, DateContext=Normal
    Status: LIVE.

  digitalPctNoAssistanceActivations
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal,
            MetricID=DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect
    QGP:    HARDCODED NULL. Confirmed zero metric_type='QGP' rows for this MetricID.
    Status: Actual LIVE; QGP confirmed absent.

  digitalPctAssistanceActivations
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal,
            MetricID=NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect
    QGP:    HARDCODED NULL. Confirmed zero metric_type='QGP' rows for this MetricID.
    Status: Actual LIVE; QGP confirmed absent.


---------------------------------------------------------------------------------------------------
PHONE BOPIS / NON-BOPIS RELATIONSHIP
---------------------------------------------------------------------------------------------------

Existing combined metric:

  activationsBopis

Actual definition:

  activationsBopis
    =
  activationsBopisOnly
    +
  activationsNonBopisOnly

The existing activationsBopis QGP target remains sourced from the existing combined target ID:

  ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital

The two new component QGP targets are independently looked up using their own component
MetricIDs with metric_type='QGP'.

No target allocation or mathematical split is performed in Silver.


---------------------------------------------------------------------------------------------------
DEDUP GRAIN AUDIT
---------------------------------------------------------------------------------------------------

See Bronze header for full write-up.

Summary:
  page and date_context are both required in Bronze's dedup partition and both are present.

  publish_key and is_future were checked directly against live data and found not to vary in
  a way that matters.

  display_metric_type was not independently verified but shows no signs of being an independent
  axis.


---------------------------------------------------------------------------------------------------
NORMALIZATION APPLIED
---------------------------------------------------------------------------------------------------

  - All Page/MetricType/DateContext/MetricID comparisons are case-insensitive:
      UPPER(TRIM(...))

  - is_future comparisons use Bronze's normalized boolean is_future column.


---------------------------------------------------------------------------------------------------
CHANGE LOG
---------------------------------------------------------------------------------------------------

  1. vrPostpaidActivations QGP:
     added DateContext=Normal.

  2. digitalPctPhoneNewActsNoAssistPlusAssist, digitalPctNoAssistanceActivations,
     digitalPctAssistanceActivations:
     QGP side originally built by mirroring each metric's Actual predicates.

  3. activationsBopis:
     Page corrected from 'digital' to 'Digital Transformation NEW'.

  4. vrCalls/vrChats QGP:
     DisplayMetricType corrected from 'QGP' to 'Target'.

  5. digitalPctNoAssistanceActivations_qgp and digitalPctAssistanceActivations_qgp:
     changed to hardcoded NULL after Bronze diagnostics confirmed zero QGP rows.

  6. activationsNewAalNoAssistance and
     digitalPctConsumerPostpaidActivationsTotalInclAssisted:
     temporarily downgraded while source MetricIDs were investigated.

  7. storeTraffic, vrCalls_actual, vrChats_actual:
     explicit Page filters added to prevent duplicate-page double counting.

  8. activationsNewAalNoAssistance:
     re-scoped to the confirmed BTS-family MetricID trio.

  9. digitalPctConsumerPostpaidActivationsTotalInclAssisted:
     re-scoped to the confirmed IT Summary MetricID.

  10. Added Phone activation components while preserving activationsBopis unchanged:

      activationsBopisOnly
        Actual:
          ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital
        QGP:
          same component ID under metric_type='QGP'

      activationsNonBopisOnly
        Actual:
          ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital
        QGP:
          same component ID under metric_type='QGP'

      If no component-level QGP rows exist in Bronze, those QGP_TARGET values naturally
      remain NULL. No artificial target split is performed.


---------------------------------------------------------------------------------------------------
ASSUMPTION CARRIED FROM BRONZE
---------------------------------------------------------------------------------------------------

week_ending is assumed to land on the same dates as:

  sdi_vw_dashboardPulseTms_dim_qgp_calendar.qgp_date

including BOUNDARY_STUB/BOUNDARY_FIRST quarter-boundary dates.

No proration is therefore applied here.

If week_ending does not align with qgp_date for boundary weeks, the join can leave
orphaned/NULL rows around quarter-end and will require adjustment.


---------------------------------------------------------------------------------------------------
WoW/YoY LOGIC
---------------------------------------------------------------------------------------------------

NORMAL:
  numerator   = current value
  denominator = prior QGP value
  if prior was BOUNDARY_FIRST:
    denominator = BOUNDARY_FIRST + its stub

BOUNDARY_STUB:
  numerator   = NULL
  denominator = NULL

BOUNDARY_FIRST:
  numerator   = current + preceding stub
  denominator = last NORMAL week before the stub

LY:
  prior-year same ISO week weekly total x current days_in_period / 7

================================================================================================= */


CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_silver_qgp_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly
  USING DELTA
  CLUSTER BY (qgp_date, metric_name, metric_type)
  COMMENT '
    PulseTMS Silver - QGP scorecard metrics in long format with WoW/YoY.

    12 named metrics with QGP_ACTUAL / QGP_TARGET representation.

    Phone activation metrics:
      activationsBopis
        = existing combined BOPIS + Non-BOPIS metric

      activationsBopisOnly
        = BOPIS-only component

      activationsNonBopisOnly
        = Non-BOPIS-only component

    Component QGP targets are read directly from their corresponding
    MetricIDs when metric_type = QGP rows exist.

    No artificial BOPIS / Non-BOPIS target allocation is performed.

    Confirmed hardcoded NULL QGP targets:
      digitalPctNoAssistanceActivations
      digitalPctAssistanceActivations

    metric_type:
      QGP_ACTUAL
      QGP_TARGET

    Refreshed weekly via:
      sdi_sp_dashboardPulseTms_silver_qgp_weekly
  '
  AS

  WITH

  /* ===============================================================================================
     STEP 1: AGGREGATE BRONZE QGP ROWS TO ONE COLUMN PER METRIC x VARIANT
     =============================================================================================== */

  MetricsWide AS (

    SELECT
      b.week_ending,

      /* -------------------------------------------------------------------------------------------
         activationsBopis
         Existing combined Phone BOPIS + Non-BOPIS metric.
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) IN (
             UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
             UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
           )
            THEN b.amount
        END
      ) AS activationsBopis_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital')
            THEN b.amount
        END
      ) AS activationsBopis_qgp,


      /* -------------------------------------------------------------------------------------------
         activationsBopisOnly
         Phone BOPIS-only component.
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital')
            THEN b.amount
        END
      ) AS activationsBopisOnly_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital')
            THEN b.amount
        END
      ) AS activationsBopisOnly_qgp,


      /* -------------------------------------------------------------------------------------------
         activationsNonBopisOnly
         Phone Non-BOPIS-only component.
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
            THEN b.amount
        END
      ) AS activationsNonBopisOnly_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
            THEN b.amount
        END
      ) AS activationsNonBopisOnly_qgp,


      /* -------------------------------------------------------------------------------------------
         activationsNewAalNoAssistance
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) IN (
             UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),
             UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital')
           )
            THEN b.amount
        END
      ) AS activationsNewAalNoAssistance_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped')
            THEN b.amount
        END
      ) AS activationsNewAalNoAssistance_qgp,


      /* -------------------------------------------------------------------------------------------
         storeTraffic
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
           AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'BRANDED RETAIL TOTAL'
           AND b.is_future = FALSE
            THEN b.amount
        END
      ) AS storeTraffic_actual,

      SUM(
        CASE
          WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
           AND UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'BRANDED RETAIL TOTAL'
            THEN b.amount
        END
      ) AS storeTraffic_qgp,


      /* -------------------------------------------------------------------------------------------
         vrCalls
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'HERO - CORE POSTPAID KP2'
           AND b.is_future = FALSE
            THEN b.amount
        END
      ) AS vrCalls_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
            THEN b.amount
        END
      ) AS vrCalls_qgp,


      /* -------------------------------------------------------------------------------------------
         vrChats
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'HERO - CORE POSTPAID KP2'
           AND b.is_future = FALSE
            THEN b.amount
        END
      ) AS vrChats_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
           AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
            THEN b.amount
        END
      ) AS vrChats_qgp,


      /* -------------------------------------------------------------------------------------------
         vrPostpaidActivations
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'postpaid activations')
           AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped')
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
           AND b.is_future = FALSE
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
            THEN b.amount
        END
      ) AS vrPostpaidActivations_actual,

      SUM(
        CASE
          WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'postpaid activations')
           AND UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped')
           AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
            THEN b.amount
        END
      ) AS vrPostpaidActivations_qgp,


      /* -------------------------------------------------------------------------------------------
         digitalPctPhoneNewActsNoAssistPlusAssist
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
           AND b.is_future = FALSE
            THEN b.amount
        END
      ) AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
            THEN b.amount
        END
      ) AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,


      /* -------------------------------------------------------------------------------------------
         digitalPctConsumerPostpaidActivationsTotalInclAssisted
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
           AND UPPER(TRIM(b.page)) = 'IT SUMMARY'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
           AND b.is_future = FALSE
            THEN b.amount
        END
      ) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
           AND UPPER(TRIM(b.page)) = 'IT SUMMARY'
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA')
            THEN b.amount
        END
      ) AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,


      /* -------------------------------------------------------------------------------------------
         digitalPctNoAssistanceActivations
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
            THEN b.amount
        END
      ) AS digitalPctNoAssistanceActivations_actual,

      CAST(NULL AS DOUBLE) AS digitalPctNoAssistanceActivations_qgp,


      /* -------------------------------------------------------------------------------------------
         digitalPctAssistanceActivations
         ------------------------------------------------------------------------------------------- */

      SUM(
        CASE
          WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
           AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
           AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           AND UPPER(TRIM(b.metric_id)) =
               UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
            THEN b.amount
        END
      ) AS digitalPctAssistanceActivations_actual,

      CAST(NULL AS DOUBLE) AS digitalPctAssistanceActivations_qgp

    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_qgp_weekly b

    GROUP BY
      b.week_ending
  ),


  /* ===============================================================================================
     STEP 2: JOIN TO QGP CALENDAR

     Direct join; no proration.
     =============================================================================================== */

  WithCalendar AS (

    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter AS qgp_quarter,
      cal.days_in_period,
      cal.is_complete_period,
      cal.wow_prior_qgp_date,
      cal.boundary_stub_date,
      cal.iso_week_number,
      cal.iso_year,
      m.*

    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal

    LEFT JOIN MetricsWide m
      ON m.week_ending = cal.qgp_date

    WHERE
      cal.qgp_date < trunc(current_date(), 'QUARTER')
      OR (
        cal.qgp_date >= trunc(current_date(), 'QUARTER')
        AND cal.qgp_date <= date_sub(
          add_months(
            trunc(current_date(), 'QUARTER'),
            3
          ),
          1
        )
      )
  ),


  /* ===============================================================================================
     STEP 3: UNPIVOT 12 METRICS x 2 VARIANTS
     =============================================================================================== */

  Unpivoted AS (

    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsBopis' AS metric_name, 'QGP_ACTUAL' AS metric_type, activationsBopis_actual AS metric_value
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsBopis', 'QGP_TARGET', activationsBopis_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsBopisOnly', 'QGP_ACTUAL', activationsBopisOnly_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsBopisOnly', 'QGP_TARGET', activationsBopisOnly_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsNonBopisOnly', 'QGP_ACTUAL', activationsNonBopisOnly_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsNonBopisOnly', 'QGP_TARGET', activationsNonBopisOnly_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsNewAalNoAssistance', 'QGP_ACTUAL', activationsNewAalNoAssistance_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'activationsNewAalNoAssistance', 'QGP_TARGET', activationsNewAalNoAssistance_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'storeTraffic', 'QGP_ACTUAL', storeTraffic_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'storeTraffic', 'QGP_TARGET', storeTraffic_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'vrCalls', 'QGP_ACTUAL', vrCalls_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'vrCalls', 'QGP_TARGET', vrCalls_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'vrChats', 'QGP_ACTUAL', vrChats_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'vrChats', 'QGP_TARGET', vrChats_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'vrPostpaidActivations', 'QGP_ACTUAL', vrPostpaidActivations_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'vrPostpaidActivations', 'QGP_TARGET', vrPostpaidActivations_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctPhoneNewActsNoAssistPlusAssist', 'QGP_ACTUAL', digitalPctPhoneNewActsNoAssistPlusAssist_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctPhoneNewActsNoAssistPlusAssist', 'QGP_TARGET', digitalPctPhoneNewActsNoAssistPlusAssist_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_ACTUAL', digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_TARGET', digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctNoAssistanceActivations', 'QGP_ACTUAL', digitalPctNoAssistanceActivations_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctNoAssistanceActivations', 'QGP_TARGET', digitalPctNoAssistanceActivations_qgp
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctAssistanceActivations', 'QGP_ACTUAL', digitalPctAssistanceActivations_actual
    FROM WithCalendar

    UNION ALL
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year,
           'digitalPctAssistanceActivations', 'QGP_TARGET', digitalPctAssistanceActivations_qgp
    FROM WithCalendar
  ),


  /* ===============================================================================================
     STEP 4: CURRENT-PERIOD LOOKUP
     =============================================================================================== */

  MetricLookup AS (

    SELECT
      qgp_date,
      metric_name,
      metric_type,
      metric_value

    FROM Unpivoted
  ),


  /* ===============================================================================================
     STEP 5: PRIOR-YEAR NATURAL WEEK LOOKUP
     =============================================================================================== */

  LYWeeklyLookup AS (

    SELECT
      iso_year,
      iso_week_number,
      metric_name,
      metric_type,
      SUM(metric_value) AS ly_weekly_metric_value

    FROM Unpivoted

    WHERE metric_value IS NOT NULL

    GROUP BY
      iso_year,
      iso_week_number,
      metric_name,
      metric_type
  ),


  /* ===============================================================================================
     STEP 6: WOW / YOY
     =============================================================================================== */

  WithWowYoy AS (

    SELECT
      u.qgp_date,
      u.week_type,
      u.qgp_quarter,
      u.days_in_period,
      u.is_complete_period,
      u.metric_name,
      u.metric_type,
      u.metric_value,

      ROUND(
        ly_week.ly_weekly_metric_value * try_divide(u.days_in_period, 7),
        2
      ) AS metric_value_ly,

      CASE u.week_type
        WHEN 'BOUNDARY_STUB'
          THEN NULL

        WHEN 'BOUNDARY_FIRST'
          THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)

        ELSE u.metric_value
      END AS wow_numerator,

      CASE
        WHEN u.metric_value IS NULL
          THEN NULL

        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN NULL

        WHEN wow_prior_stub_lookup.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0)
             + COALESCE(wow_prior_stub_lookup.metric_value, 0)

        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END AS wow_denominator,

      CASE u.week_type
        WHEN 'BOUNDARY_STUB'
          THEN NULL

        WHEN 'BOUNDARY_FIRST'
          THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)

        ELSE u.metric_value
      END AS yoy_numerator,

      CASE
        WHEN u.metric_value IS NULL
          THEN NULL

        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN NULL

        ELSE ly_week.ly_weekly_metric_value
      END AS yoy_denominator

    FROM Unpivoted u

    LEFT JOIN MetricLookup wow_prior_lookup
      ON  wow_prior_lookup.qgp_date = u.wow_prior_qgp_date
      AND wow_prior_lookup.metric_name = u.metric_name
      AND wow_prior_lookup.metric_type = u.metric_type

    LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal
      ON prior_cal.qgp_date = u.wow_prior_qgp_date

    LEFT JOIN MetricLookup wow_prior_stub_lookup
      ON  wow_prior_stub_lookup.qgp_date = prior_cal.boundary_stub_date
      AND wow_prior_stub_lookup.metric_name = u.metric_name
      AND wow_prior_stub_lookup.metric_type = u.metric_type

    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date = u.boundary_stub_date
      AND stub_lookup.metric_name = u.metric_name
      AND stub_lookup.metric_type = u.metric_type

    LEFT JOIN LYWeeklyLookup ly_week
      ON  ly_week.iso_year = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.metric_name = u.metric_name
      AND ly_week.metric_type = u.metric_type
  )


  /* ===============================================================================================
     FINAL SILVER OUTPUT

     GRAIN:
       qgp_date x metric_name x metric_type

     metric_type:
       QGP_ACTUAL
       QGP_TARGET

     New metrics:
       activationsBopisOnly
       activationsNonBopisOnly

     Existing activationsBopis remains unchanged.
     =============================================================================================== */

  SELECT
    qgp_date,
    week_type,
    qgp_quarter,
    days_in_period,
    is_complete_period,
    metric_name,
    metric_type,
    metric_value,
    metric_value_ly,
    wow_numerator,
    wow_denominator,

    CASE
      WHEN wow_denominator IS NULL
        OR wow_denominator = 0
        THEN NULL
      ELSE wow_numerator / wow_denominator - 1
    END AS wow_pct,

    yoy_numerator,
    yoy_denominator,

    CASE
      WHEN yoy_denominator IS NULL
        OR yoy_denominator = 0
        THEN NULL
      ELSE yoy_numerator / yoy_denominator - 1
    END AS yoy_pct,

    MAX(
      CASE
        WHEN metric_value IS NOT NULL
          THEN qgp_date
      END
    ) OVER (
      PARTITION BY
        metric_name,
        metric_type
    ) AS max_date

  FROM WithWowYoy
  ;

END;