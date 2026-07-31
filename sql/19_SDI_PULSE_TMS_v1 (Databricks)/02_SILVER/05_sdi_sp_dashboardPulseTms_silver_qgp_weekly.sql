/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_silver_qgp_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_silver_qgp_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_silver_qgp_weekly.
  Reproduces the 10 named metrics from your existing Tableau calculated fields (each as an
  Actual/QGP-target pair) off sdi_tbl_dashboardPulseTms_bronze_qgp_weekly, reshaped into the
  same long metric_name/metric_type/metric_value format as the rest of PulseTMS Silver, with
  WoW/YoY computed the same way as adobeFunnel/mfcSpend/platformSpend Silver.

METRICS (metric_name -- Actual filter -- QGP/target filter):
  activationsBopis
    Actual: MetricType=Actuals, Page=digital, DateContext=Normal,
            MetricID IN (...BOPISUnassisted..., ...NonBOPISUnassisted...)  -- sums 2 IDs
    QGP:    MetricType=QGP, Page=digital, DateContext=Normal,
            MetricID=...Activations...  -- a 3rd, different ID from the Actual side
  activationsNewAalNoAssistance
    Actual: MetricName contains 'activations (new + aal' and 'no assistance',
            MetricType IN (Actuals, Actuals/Outlook), MetricID=TotalDigitalPhoneActivations...,
            Page=digital, IsFuture=Is Past, DateContext=Normal
    QGP:    same substring/ID/Page, MetricType=QGP, DateContext=Normal (no IsFuture)
  storeTraffic
    Actual: MetricName='store traffic (excl store-in-store)' (exact), MetricType=Actuals/Outlook,
            DateContext=Normal, IsFuture=Is Past  -- identified by name only, no MetricID
    QGP:    same name match, MetricType=QGP, DateContext=Normal
  vrCalls
    Actual: MetricType=Actuals/Outlook, MetricID=VRInboundCalls..., DateContext=Normal, IsFuture=Is Past
    QGP:    MetricType=QGP, same ID, DisplayMetricType=QGP, DateContext=Normal, Page=virtual retail
  vrChats
    Same shape as vrCalls, MetricID=VRChats...
  vrPostpaidActivations
    Actual: MetricName contains 'postpaid activations', MetricType=Actuals/Outlook,
            MetricID=VRPostpaidActivations..., Page=virtual retail outcomes 1, IsFuture=Is Past,
            DateContext=Normal
    QGP:    same substring/ID/Page, MetricType=QGP, DateContext=Normal
            *** DateContext=Normal added here per your confirmation -- see CHANGE LOG ***
  digitalPctPhoneNewActsNoAssistPlusAssist
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal, MetricID=ConPostpaidDigitalPct..., IsFuture=Is Past
    QGP:    *** built from the pattern, not directly pasted -- see CHANGE LOG ***
            same Page/MetricID/DateContext, MetricType=QGP
  digitalPctConsumerPostpaidActivationsTotalInclAssisted
    Actual: MetricType=Actuals/Outlook, MetricID=DigitalPctofConsumerPostpaidActivationsTotalInclAssistedNEW,
            DateContext=Normal, IsFuture=Is Past, Page=Digital
    QGP:    MetricType=QGP, same ID, Page=Digital, DisplayMetricType=QGP, DateContext=Normal
  digitalPctNoAssistanceActivations
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal, MetricID=DigitalPctofConsumerPostpaidActivationsPhoneNEW...
            (no IsFuture filter in your original calc, preserved as-is)
    QGP:    *** built from the pattern -- see CHANGE LOG ***
            same Page/MetricID/DateContext, MetricType=QGP
  digitalPctAssistanceActivations
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal, MetricID=NewPhoneBANAssistedActivationsDigitalPCT...
            (no IsFuture filter in your original calc, preserved as-is)
    QGP:    *** built from the pattern -- see CHANGE LOG ***
            same Page/MetricID/DateContext, MetricType=QGP

NORMALIZATION APPLIED (flagged, not silent):
  - All Page/MetricType/DateContext/MetricID comparisons done case-insensitively
    (UPPER(TRIM(...))), even where your original Tableau calc used exact-case matching --
    casing looked inconsistent across your own calcs rather than deliberately meaningful.
  - is_future comparisons use Bronze's already-normalized boolean is_future column
    (TRUE = 'Is Future'/'IsFuture', FALSE = 'Is Past'), instead of re-parsing the raw string.

CHANGE LOG (this translation, vs. your pasted Tableau calcs):
  - vrPostpaidActivations QGP: added back DateContext=Normal (was commented out in your calc).
    Every other QGP calc filters to Normal; IsFuture is the only piece that's Actuals-specific,
    per your confirmation. Left the IsFuture filter OFF the QGP side, matching every other pair.
  - digitalPctPhoneNewActsNoAssistPlusAssist, digitalPctNoAssistanceActivations,
    digitalPctAssistanceActivations: QGP side built by mirroring each metric's own Actual
    Page/MetricID/DateContext filters, MetricType swapped to QGP, IsFuture dropped -- no QGP
    block was pasted for these three, so this is inferred from the pattern the other seven
    metrics share, not a direct 1:1 port. Worth a quick sense-check against Tableau once this
    is running.

ASSUMPTION CARRIED FROM BRONZE (not yet confirmed):
  week_ending is assumed to land on the same dates as
  sdi_vw_dashboardPulseTms_dim_qgp_calendar.qgp_date, including BOUNDARY_STUB/BOUNDARY_FIRST
  quarter-boundary dates, so no proration is applied here (unlike adobeFunnel/mfcSpend/
  platformSpend Silver, which prorate because their raw sources are strictly Saturday-anchored).
  If week_ending turns out not to align with qgp_date for boundary weeks, this join will leave
  orphaned/NULL rows around quarter-end and will need adjusting.

WoW/YoY LOGIC (same pattern as every other PulseTMS Silver):
  NORMAL         : numerator = current value; denominator = prior QGP value
                   (if prior was BOUNDARY_FIRST: denominator = BF + its stub)
  BOUNDARY_STUB  : numerator = NULL, denominator = NULL
  BOUNDARY_FIRST : numerator = current + preceding stub
                   denominator = last NORMAL week before the stub
  LY             : prior-year same ISO week weekly total x current days_in_period / 7
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
  COMMENT 'PulseTMS Silver — QGP scorecard metrics (Activations BOPIS, Activations New+AAL No Assistance, Store Traffic, VR Calls, VR Chats, VR Postpaid Activations, 3 Digital % metrics) in long format with WoW/YoY. metric_type = QGP_ACTUAL | QGP_TARGET. Clustered by qgp_date, metric_name, metric_type. Refreshed weekly via sdi_sp_dashboardPulseTms_silver_qgp_weekly.'
  AS
  WITH

  -- ===========================================================================
  -- STEP 1: Aggregate Bronze QGP rows into one named column per metric x variant,
  --         at week_ending grain. Each predicate mirrors your Tableau calc exactly
  --         (see header for the full list and the flagged normalizations).
  -- ===========================================================================
  MetricsWide AS (
    SELECT
      b.week_ending,

      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS'
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) IN (
                  UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
                  UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital')
                )
           THEN b.amount END)                                              AS activationsBopis_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital')
           THEN b.amount END)                                              AS activationsBopis_qgp,

      SUM(CASE WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'activations (new + aal')
                AND CONTAINS(LOWER(TRIM(b.metric_name_label)), 'no assistance')
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.metric_id)) = UPPER('TotalDigitalPhoneActivationsTMOAndSprintGlanceTM1Mapped') /* ⚠ UNCONFIRMED casing, see note */
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW' /* inferred from activationsBopis fix, not independently confirmed */
                AND b.is_future = FALSE
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS activationsNewAalNoAssistance_actual,
      SUM(CASE WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'activations (new + aal')
                AND CONTAINS(LOWER(TRIM(b.metric_name_label)), 'no assistance')
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('TotalDigitalPhoneActivationsTMOAndSprintGlanceTM1Mapped') /* ⚠ UNCONFIRMED casing, see note */
                AND UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW' /* inferred from activationsBopis fix, not independently confirmed */
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS activationsNewAalNoAssistance_qgp,

      SUM(CASE WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
                AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS storeTraffic_actual,
      SUM(CASE WHEN LOWER(TRIM(b.metric_name_label)) = 'store traffic (excl store-in-store)'
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS storeTraffic_qgp,

      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS vrCalls_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRInboundCallsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
           THEN b.amount END)                                              AS vrCalls_qgp,

      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS vrChats_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRChatsinclHSIAutomatedManual')
                AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL'
           THEN b.amount END)                                              AS vrChats_qgp,

      SUM(CASE WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'postpaid activations')
                AND UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') /* ⚠ UNCONFIRMED casing, see note */
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
                AND b.is_future = FALSE
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS vrPostpaidActivations_actual,
      -- DateContext=Normal added back here vs. your pasted calc (see CHANGE LOG above)
      SUM(CASE WHEN CONTAINS(LOWER(TRIM(b.metric_name_label)), 'postpaid activations')
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') /* ⚠ UNCONFIRMED casing, see note */
                AND UPPER(TRIM(b.page)) = 'VIRTUAL RETAIL OUTCOMES 1'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS vrPostpaidActivations_qgp,

      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
                AND b.is_future = FALSE
           THEN b.amount END)                                              AS digitalPctPhoneNewActsNoAssistPlusAssist_actual,
      -- QGP built from the pattern, not directly pasted (see CHANGE LOG above)
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect')
           THEN b.amount END)                                              AS digitalPctPhoneNewActsNoAssistPlusAssist_qgp,

      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'ACTUALS/OUTLOOK'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsTotalInclAssistedNEW')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND b.is_future = FALSE
                AND UPPER(TRIM(b.page)) = 'DIGITAL'
           THEN b.amount END)                                              AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual,
      SUM(CASE WHEN UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsTotalInclAssistedNEW')
                AND UPPER(TRIM(b.page)) = 'DIGITAL'
                AND UPPER(TRIM(b.display_metric_type)) = 'TARGET'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
           THEN b.amount END)                                              AS digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp,

      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
           THEN b.amount END)                                              AS digitalPctNoAssistanceActivations_actual,
      -- QGP built from the pattern, not directly pasted (see CHANGE LOG above)
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect')
           THEN b.amount END)                                              AS digitalPctNoAssistanceActivations_qgp,

      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) IN ('ACTUALS', 'ACTUALS/OUTLOOK')
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
           THEN b.amount END)                                              AS digitalPctAssistanceActivations_actual,
      -- QGP built from the pattern, not directly pasted (see CHANGE LOG above)
      SUM(CASE WHEN UPPER(TRIM(b.page)) = 'DIGITAL TRANSFORMATION NEW'
                AND UPPER(TRIM(b.metric_type)) = 'QGP'
                AND UPPER(TRIM(b.date_context)) = 'NORMAL'
                AND UPPER(TRIM(b.metric_id)) = UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
           THEN b.amount END)                                              AS digitalPctAssistanceActivations_qgp

    FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_qgp_weekly b
    GROUP BY b.week_ending
  ),

  -- ===========================================================================
  -- STEP 2: Join to the calendar dim. Direct join, no proration -- see the
  --         ASSUMPTION note in the header about week_ending vs qgp_date alignment.
  -- ===========================================================================
  WithCalendar AS (
    SELECT
      cal.qgp_date,
      cal.week_type,
      cal.quarter                AS qgp_quarter,
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
        AND cal.qgp_date <= date_sub(add_months(trunc(current_date(), 'QUARTER'), 3), 1)
      )
  ),

  -- ===========================================================================
  -- STEP 3: Unpivot 10 metrics x 2 variants (actual, qgp target) to long format
  -- ===========================================================================
  Unpivoted AS (
    SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'activationsBopis' AS metric_name, 'QGP_ACTUAL' AS metric_type, activationsBopis_actual AS metric_value FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'activationsBopis', 'QGP_TARGET', activationsBopis_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'activationsNewAalNoAssistance', 'QGP_ACTUAL', activationsNewAalNoAssistance_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'activationsNewAalNoAssistance', 'QGP_TARGET', activationsNewAalNoAssistance_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'storeTraffic', 'QGP_ACTUAL', storeTraffic_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'storeTraffic', 'QGP_TARGET', storeTraffic_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'vrCalls', 'QGP_ACTUAL', vrCalls_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'vrCalls', 'QGP_TARGET', vrCalls_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'vrChats', 'QGP_ACTUAL', vrChats_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'vrChats', 'QGP_TARGET', vrChats_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'vrPostpaidActivations', 'QGP_ACTUAL', vrPostpaidActivations_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'vrPostpaidActivations', 'QGP_TARGET', vrPostpaidActivations_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctPhoneNewActsNoAssistPlusAssist', 'QGP_ACTUAL', digitalPctPhoneNewActsNoAssistPlusAssist_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctPhoneNewActsNoAssistPlusAssist', 'QGP_TARGET', digitalPctPhoneNewActsNoAssistPlusAssist_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_ACTUAL', digitalPctConsumerPostpaidActivationsTotalInclAssisted_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_TARGET', digitalPctConsumerPostpaidActivationsTotalInclAssisted_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctNoAssistanceActivations', 'QGP_ACTUAL', digitalPctNoAssistanceActivations_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctNoAssistanceActivations', 'QGP_TARGET', digitalPctNoAssistanceActivations_qgp FROM WithCalendar

    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctAssistanceActivations', 'QGP_ACTUAL', digitalPctAssistanceActivations_actual FROM WithCalendar
    UNION ALL SELECT qgp_date, week_type, qgp_quarter, days_in_period, is_complete_period, wow_prior_qgp_date, boundary_stub_date, iso_week_number, iso_year, 'digitalPctAssistanceActivations', 'QGP_TARGET', digitalPctAssistanceActivations_qgp FROM WithCalendar
  ),

  -- ===========================================================================
  -- STEP 4: Lookups for WoW/YoY self-joins (same pattern as every other Silver)
  -- ===========================================================================
  MetricLookup AS (
    SELECT qgp_date, metric_name, metric_type, metric_value
    FROM Unpivoted
  ),

  LYWeeklyLookup AS (
    SELECT
      iso_year,
      iso_week_number,
      metric_name,
      metric_type,
      SUM(metric_value) AS ly_weekly_metric_value
    FROM Unpivoted
    WHERE metric_value IS NOT NULL
    GROUP BY iso_year, iso_week_number, metric_name, metric_type
  ),

  WithWowYoy AS (
    SELECT
      u.qgp_date, u.week_type, u.qgp_quarter, u.days_in_period, u.is_complete_period,
      u.metric_name, u.metric_type, u.metric_value,

      ROUND(
        ly_week.ly_weekly_metric_value * try_divide(u.days_in_period, 7),
        2
      )                                                                   AS metric_value_ly,

      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS wow_numerator,

      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        WHEN wow_prior_stub_lookup.metric_value IS NOT NULL
          THEN COALESCE(wow_prior_lookup.metric_value, 0) + COALESCE(wow_prior_stub_lookup.metric_value, 0)
        ELSE COALESCE(wow_prior_lookup.metric_value, 0)
      END                                                                 AS wow_denominator,

      CASE u.week_type
        WHEN 'BOUNDARY_STUB'  THEN NULL
        WHEN 'BOUNDARY_FIRST' THEN u.metric_value + COALESCE(stub_lookup.metric_value, 0)
        ELSE                       u.metric_value
      END                                                                 AS yoy_numerator,

      CASE
        WHEN u.metric_value IS NULL        THEN NULL
        WHEN u.week_type = 'BOUNDARY_STUB' THEN NULL
        ELSE ly_week.ly_weekly_metric_value
      END                                                                 AS yoy_denominator

    FROM Unpivoted u
    LEFT JOIN MetricLookup wow_prior_lookup
      ON  wow_prior_lookup.qgp_date    = u.wow_prior_qgp_date
      AND wow_prior_lookup.metric_name = u.metric_name
      AND wow_prior_lookup.metric_type = u.metric_type
    LEFT JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar prior_cal
      ON  prior_cal.qgp_date = u.wow_prior_qgp_date
    LEFT JOIN MetricLookup wow_prior_stub_lookup
      ON  wow_prior_stub_lookup.qgp_date    = prior_cal.boundary_stub_date
      AND wow_prior_stub_lookup.metric_name = u.metric_name
      AND wow_prior_stub_lookup.metric_type = u.metric_type
    LEFT JOIN MetricLookup stub_lookup
      ON  stub_lookup.qgp_date    = u.boundary_stub_date
      AND stub_lookup.metric_name = u.metric_name
      AND stub_lookup.metric_type = u.metric_type
    LEFT JOIN LYWeeklyLookup ly_week
      ON  ly_week.iso_year        = u.iso_year - 1
      AND ly_week.iso_week_number = u.iso_week_number
      AND ly_week.metric_name     = u.metric_name
      AND ly_week.metric_type     = u.metric_type
  )

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
    CASE WHEN wow_denominator IS NULL OR wow_denominator = 0 THEN NULL
         ELSE wow_numerator / wow_denominator - 1
    END                                                                   AS wow_pct,
    yoy_numerator,
    yoy_denominator,
    CASE WHEN yoy_denominator IS NULL OR yoy_denominator = 0 THEN NULL
         ELSE yoy_numerator / yoy_denominator - 1
    END                                                                   AS yoy_pct,
    MAX(CASE WHEN metric_value IS NOT NULL THEN qgp_date END)
      OVER (PARTITION BY metric_name, metric_type)                        AS max_date
  FROM WithWowYoy;

END;