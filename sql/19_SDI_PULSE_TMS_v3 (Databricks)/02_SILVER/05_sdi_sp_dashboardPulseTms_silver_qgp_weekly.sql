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

  11. Boundary-week WoW / YoY corrected (see WoW/YoY LOGIC):
      - digitalPct* ratios: full week is now the day-weighted average of the stub and
        BOUNDARY_FIRST pieces (was their sum, e.g. 0.467 + 0.493 = 0.960).
      - Missing piece: full week is now NULL (was the single piece treated as a full week,
        because the missing piece was COALESCEd to 0).
      - wow_denominator: decides "prior week was a boundary week" from the calendar
        (via the prior row's own full week) and is NULL, not 0, when the prior week is missing.
      - LY: built from prior-year full weeks (same ISO week Saturday) instead of summing all
        QGP periods in that ISO week; metric_value_ly no longer scales ratios by days / 7 and
        keeps 3 decimals for ratios (2 would show 0.4819 as 0.48, a full point off).
      - metric_value and every output column are unchanged.


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
WoW/YoY LOGIC (full Sunday-Saturday week, unit-aware)
---------------------------------------------------------------------------------------------------

Every WoW / YoY value is built from a FULL Sunday-Saturday week value (FullWeek CTE):

  NORMAL          full week = metric_value

  BOUNDARY_FIRST  full week = BOUNDARY_STUB piece (old quarter) + BOUNDARY_FIRST piece (new quarter)
                    counts (activations, store traffic, VR) : stub + first
                    ratios (the four digitalPct* metrics)   : day-weighted average
                        (first x first_days + stub x stub_days) / 7
                        first_days = days_in_period of the BOUNDARY_FIRST row
                        stub_days  = 7 - first_days
                    if EITHER piece is missing: full week = NULL (QGP rule: missing stays missing;
                    no estimate is made)

  BOUNDARY_STUB   no full week of its own (NULL); its value is used by the BOUNDARY_FIRST row

  wow_numerator   = full week of this row
  wow_denominator = full week of wow_prior_qgp_date (NULL, never 0, when it does not exist)
  yoy_numerator   = full week of this row
  yoy_denominator = full week of the prior-year Saturday in the same ISO week
  wow_pct / yoy_pct = numerator / denominator - 1 (ratios: relative change of the percentage)

  metric_value_ly
    counts : prior-year full week x days_in_period / 7 (share of the week this period covers)
    ratios : prior-year full week as is (a percentage does not scale with days), rounded to 3
             decimals (0.482 = 48.2%) instead of 2 (0.48), which would lose most of a point

metric_value itself is NOT changed by any of this: every QGP period keeps exactly the value
published by the feed, so weekly values, QTD sums and validation are unaffected.

Example (digitalPctConsumerPostpaidActivationsTotalInclAssisted, week ending 2026-07-04):
  stub   2026-06-30  3 days  0.467037
  first  2026-07-04  4 days  0.493004
  full week = (0.493004 x 4 + 0.467037 x 3) / 7 = 0.481875   (previously 0.960041 = the two added)

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
     One row per QGP period (used to find the BOUNDARY_STUB piece of a BOUNDARY_FIRST week).
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
     STEP 5: FULL SUNDAY-SATURDAY WEEK VALUE
     NORMAL         = metric_value
     BOUNDARY_FIRST = stub + first (counts) / day-weighted average (ratios); NULL if a piece is missing
     BOUNDARY_STUB  = NULL (its value is consumed by the BOUNDARY_FIRST row)
     =============================================================================================== */

  FullWeek AS (

    SELECT
      u.qgp_date,
      u.week_type,
      u.iso_year,
      u.iso_week_number,
      u.metric_name,
      u.metric_type,

      CASE
        WHEN u.week_type = 'BOUNDARY_STUB'
          THEN CAST(NULL AS DOUBLE)

        WHEN u.week_type = 'BOUNDARY_FIRST' THEN
          CASE
            WHEN u.metric_value IS NULL
              OR stub_lookup.metric_value IS NULL
              THEN CAST(NULL AS DOUBLE)

            WHEN u.metric_name IN (
             'digitalPctPhoneNewActsNoAssistPlusAssist',
             'digitalPctConsumerPostpaidActivationsTotalInclAssisted',
             'digitalPctNoAssistanceActivations',
             'digitalPctAssistanceActivations'
           )
              THEN ( u.metric_value * u.days_in_period
                   + stub_lookup.metric_value * (7 - u.days_in_period) ) / 7.0

            ELSE u.metric_value + stub_lookup.metric_value
          END

        ELSE u.metric_value
      END AS full_week_value

    FROM Unpivoted u

    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date    = u.boundary_stub_date
      AND stub_lookup.metric_name = u.metric_name
      AND stub_lookup.metric_type = u.metric_type
  ),


  /* ===============================================================================================
     STEP 6: PRIOR-YEAR FULL WEEK LOOKUP
     Every ISO week (Monday-Sunday) contains exactly one Saturday, so the full week of the
     non-stub row in an ISO week is that ISO week's value.
     =============================================================================================== */

  LYWeeklyLookup AS (

    SELECT
      iso_year,
      iso_week_number,
      metric_name,
      metric_type,
      MAX(full_week_value) AS ly_weekly_metric_value

    FROM FullWeek

    WHERE week_type <> 'BOUNDARY_STUB'

    GROUP BY
      iso_year,
      iso_week_number,
      metric_name,
      metric_type
  ),


  /* ===============================================================================================
     STEP 7: WOW / YOY FROM FULL WEEKS
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

      CASE
        WHEN u.metric_name IN (
             'digitalPctPhoneNewActsNoAssistPlusAssist',
             'digitalPctConsumerPostpaidActivationsTotalInclAssisted',
             'digitalPctNoAssistanceActivations',
             'digitalPctAssistanceActivations'
           )
          THEN ROUND(ly_week.ly_weekly_metric_value, 3)
        ELSE ROUND(ly_week.ly_weekly_metric_value * try_divide(u.days_in_period, 7), 2)
      END AS metric_value_ly,

      fw.full_week_value AS wow_numerator,

      CASE
        WHEN fw.full_week_value IS NULL
          THEN CAST(NULL AS DOUBLE)
        ELSE prior_fw.full_week_value
      END AS wow_denominator,

      fw.full_week_value AS yoy_numerator,

      CASE
        WHEN fw.full_week_value IS NULL
          THEN CAST(NULL AS DOUBLE)
        ELSE ly_week.ly_weekly_metric_value
      END AS yoy_denominator

    FROM Unpivoted u

    LEFT JOIN FullWeek fw
      ON  fw.qgp_date    = u.qgp_date
      AND fw.metric_name = u.metric_name
      AND fw.metric_type = u.metric_type

    LEFT JOIN FullWeek prior_fw
      ON  prior_fw.qgp_date    = u.wow_prior_qgp_date
      AND prior_fw.metric_name = u.metric_name
      AND prior_fw.metric_type = u.metric_type

    LEFT JOIN LYWeeklyLookup ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.metric_name     = u.metric_name
      AND ly_week.metric_type     = u.metric_type
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