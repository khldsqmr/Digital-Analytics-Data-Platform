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

METRICS (metric_name -- Actual filter -- QGP/target filter -- status):
  activationsBopis
    Actual: MetricType=Actuals, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID IN (ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital,
                          ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital)
    QGP:    MetricType=QGP, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID=ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital (a 3rd, different ID)
    Status: LIVE, both sides confirmed working against real data.
  activationsNewAalNoAssistance
    Actual: MetricType=Actuals, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID IN (ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital,
                          ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital)
    QGP:    MetricType=QGP, Page=Digital Transformation NEW, DateContext=Normal,
            MetricID=TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped (a 3rd, different ID)
    Status: LIVE, both sides confirmed working against real data. Originally scoped under a
            different, nonexistent Phone MetricID -- see RESOLVED note in Bronze header for the
            full reasoning behind the swap to this BTS-family MetricID trio. Shape (Page/
            DateContext/MetricType/actuals-lag behavior) independently verified against a raw
            dump, identical to every other Actuals-type metric in this file.
  storeTraffic
    Actual: MetricName='store traffic (excl store-in-store)' (exact raw casing:
            'Store Traffic (excl Store-In-Store)'), MetricType=Actuals/Outlook, DateContext=Normal,
            Page=Branded Retail Total, IsFuture=Is Past
    QGP:    same name match, MetricType=QGP, DateContext=Normal, Page=Branded Retail Total
    Status: LIVE. Page filter ADDED here (see CHANGE LOG) -- this metric publishes under two
            Page tags with identical values ('Branded Retail Total' / 'Branded Retail Total
            Outcomes'); confirmed via raw dump the two are byte-identical duplicates, same
            PublishKey and InsertDateTime.
  vrCalls
    Actual: MetricType=Actuals/Outlook, MetricID=VRInboundCallsinclHSIAutomatedManual,
            Page=Hero - Core Postpaid KP2, DateContext=Normal, IsFuture=Is Past
    QGP:    MetricType=QGP, same ID, DisplayMetricType=Target, DateContext=Normal, Page=Virtual Retail
    Status: LIVE. Actual-side Page filter ADDED here (see CHANGE LOG).
  vrChats
    Same shape as vrCalls, MetricID=VRChatsinclHSIAutomatedManual. Same fix applied.
  vrPostpaidActivations
    Actual: MetricName contains 'postpaid activations', MetricType=Actuals/Outlook,
            MetricID=VRPostpaidActivationsinclVirtualBusinessTM1Mapped (⚠ unconfirmed casing,
            see Bronze header), Page=Virtual Retail Outcomes 1, IsFuture=Is Past, DateContext=Normal
    QGP:    same substring/ID/Page, MetricType=QGP, DateContext=Normal
            (DateContext=Normal added here vs. your original calc -- see CHANGE LOG)
    Status: LIVE, both sides confirmed working against real data.
  digitalPctPhoneNewActsNoAssistPlusAssist
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal, MetricID=ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect,
            IsFuture=Is Past
    QGP:    same Page/MetricID/DateContext, MetricType=QGP (built from the pattern, not
            directly pasted from Tableau -- see CHANGE LOG)
    Status: LIVE, both sides confirmed working against real data. This metric's MetricType is
            Actuals/Outlook, so unlike the plain-Actuals metrics above, its actual side keeps
            reporting into future weeks rather than stopping at the current lag point.
  digitalPctConsumerPostpaidActivationsTotalInclAssisted
    Actual: MetricType=Actuals/Outlook, Page=IT Summary, DateContext=Normal,
            MetricID=DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA,
            IsFuture=Is Past
    QGP:    same MetricID, MetricType=QGP, Page=IT Summary, DateContext=Normal
    Status: LIVE, both sides confirmed working against real data. Originally scoped under a
            different, nonexistent MetricID on a nonexistent Page -- see RESOLVED note in
            Bronze header for the full reasoning behind the swap. The apparent one-week value
            swing that originally held this back turned out to be a clean, consistent step at
            the actuals-to-outlook boundary (not a data quality issue) once the full weekly
            history was visible -- see Bronze header.
  digitalPctNoAssistanceActivations
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal, MetricID=DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect
            (no IsFuture filter in your original calc, preserved as-is)
    QGP:    HARDCODED NULL. Confirmed via Bronze diagnostic: this MetricID has zero
            metric_type='QGP' rows in the source at all -- genuinely no Target/plan value,
            not a filter bug.
    Status: Actual side LIVE and confirmed working; QGP side hardcoded NULL (confirmed absent).
  digitalPctAssistanceActivations
    Actual: Page=Digital Transformation NEW, MetricType IN (Actuals, Actuals/Outlook),
            DateContext=Normal, MetricID=NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect
            (no IsFuture filter in your original calc, preserved as-is)
    QGP:    HARDCODED NULL. Same situation as digitalPctNoAssistanceActivations above --
            confirmed zero QGP rows for this MetricID.
    Status: Actual side LIVE and confirmed working; QGP side hardcoded NULL (confirmed absent).

--------------------------------------------------------------------------------------------------
DEDUP GRAIN AUDIT -- see Bronze header for the full write-up. Summary: page and date_context are
both required in Bronze's dedup partition and both are present; publish_key and is_future were
checked directly against live data and found not to vary in a way that matters; display_metric_type
was not independently verified but shows no signs of being an independent axis.

NORMALIZATION APPLIED (flagged, not silent):
  - All Page/MetricType/DateContext/MetricID comparisons done case-insensitively
    (UPPER(TRIM(...))), even where your original Tableau calc used exact-case matching --
    casing looked inconsistent across your own calcs rather than deliberately meaningful.
  - is_future comparisons use Bronze's already-normalized boolean is_future column
    (TRUE = 'Is Future'/'IsFuture', FALSE = 'Is Past'), instead of re-parsing the raw string.

CHANGE LOG (this translation, vs. your pasted Tableau calcs, in the order they were found):
  1. vrPostpaidActivations QGP: added back DateContext=Normal (was commented out in your calc).
     Every other QGP calc filters to Normal; IsFuture is the only piece that's Actuals-specific,
     per your confirmation. Left the IsFuture filter OFF the QGP side, matching every other pair.
  2. digitalPctPhoneNewActsNoAssistPlusAssist, digitalPctNoAssistanceActivations,
     digitalPctAssistanceActivations: QGP side built by mirroring each metric's own Actual
     Page/MetricID/DateContext filters, MetricType swapped to QGP, IsFuture dropped -- no QGP
     block was pasted for these three originally.
  3. activationsBopis: Page corrected from 'digital' (your calc's literal) to the real stored
     value 'Digital Transformation NEW', confirmed via live data -- your calc's Page literal
     didn't match anything in the actual table.
  4. vrCalls/vrChats QGP: DisplayMetricType corrected from 'QGP' (your calc's literal) to the
     real stored value 'Target' -- every QGP-type row has DisplayMetricType='Target', never
     'QGP'; your calc's literal didn't match anything.
  5. digitalPctNoAssistanceActivations_qgp, digitalPctAssistanceActivations_qgp: downgraded
     from a guessed predicate (built from the pattern, per #2 above) to a hardcoded NULL once
     a Bronze diagnostic confirmed these two MetricIDs genuinely have zero QGP rows -- the
     guessed predicate would have always returned NULL anyway, this just says so honestly
     instead of carrying dead SQL.
  6. activationsNewAalNoAssistance, digitalPctConsumerPostpaidActivationsTotalInclAssisted:
     downgraded from live predicates (both sides) to hardcoded NULL once a fuzzy substring
     search confirmed neither MetricID exists anywhere in the source under any spelling --
     these aren't casing bugs, the metrics themselves are absent from the current feed.
  7. storeTraffic, vrCalls_actual, vrChats_actual: added an explicit Page filter to each,
     which none of these had originally. Needed once Page was added to Bronze's dedup
     partition (see Bronze header) -- without an explicit single-page filter here, a metric
     that publishes under two identical-valued Page tags would get double-counted once both
     tags survive the dedup independently. storeTraffic -> Page='Branded Retail Total';
     vrCalls_actual/vrChats_actual -> Page='Hero - Core Postpaid KP2' (the only page observed
     under MetricType=Actuals/Outlook for these two IDs).
  8. activationsNewAalNoAssistance: un-hardcoded and wired live again, on top of change #6.
     The original MetricID (TotalDigitalPhoneActivationsTMOAndSprintGlanceTM1Mapped) was
     confirmed absent, but investigating the dashboard's own Page layout (Section4=Phone vs.
     Section6=BTS on the 'Digital Transformation NEW' page) showed the "Activations (New +
     AALs)" label the original calc was filtering for only ever exists for BTS -- Phone's
     equivalent section has no such variant. The calc's MetricID was almost certainly a
     copy-paste mistake, not a retired metric. Now built the same way as activationsBopis:
     actual = SUM of ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital +
     ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital (MetricType=Actuals);
     QGP = TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped (MetricType=QGP, a
     separate, pre-aggregated ID, confirmed via exact arithmetic match across 5+ weeks that
     it equals the sum of the two Actuals-side IDs -- same relationship already trusted for
     the Phone family's own aggregate ID).
  9. digitalPctConsumerPostpaidActivationsTotalInclAssisted: un-hardcoded and wired live, on
     top of change #6. The original MetricID (DigitalPctofConsumerPostpaidActivationsTotalInclAssistedNEW)
     and Page ('Digital') were both confirmed nonexistent. Pulling the dashboard's IT Summary
     page directly found DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA
     as the "Total" row of a 3-metric family (Total / No Assistance component / Assistance
     component), with a MetricName ("Digital % of Postpaid Activations (No Assistance +
     Assistance)") that's a near-verbatim match to the metric's intended concept. Now built
     as: actual and QGP both read the same MetricID (MetricType=Actuals/Outlook vs. QGP),
     Page pinned to 'IT Summary' (confirmed byte-identical to 'IT Summary Outcomes', same
     duplicate-page pattern as Store Traffic). An apparent one-week value swing that
     originally held this metric back turned out, once the full weekly history was pulled, to
     be a clean and consistent step at the actuals-to-outlook boundary -- not a data quality
     issue.

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
  COMMENT 'PulseTMS Silver — QGP scorecard metrics (Activations BOPIS, Activations New+AAL No Assistance, Store Traffic, VR Calls, VR Chats, VR Postpaid Activations, 3 Digital % metrics) in long format with WoW/YoY. Two QGP-side columns (Digital % No Assistance / Assistance, Phone-specific) are hardcoded NULL — confirmed no Target/plan value exists for those two, see header. metric_type = QGP_ACTUAL | QGP_TARGET. Clustered by qgp_date, metric_name, metric_type. Refreshed weekly via sdi_sp_dashboardPulseTms_silver_qgp_weekly.'
  AS
  WITH

  -- ===========================================================================
  -- STEP 1: Aggregate Bronze QGP rows into one named column per metric x variant,
  --         at week_ending grain. Each predicate mirrors your Tableau calc, with
  --         corrections/hardcoded values flagged inline -- see header CHANGE LOG
  --         for the full reasoning behind each one.
  -- ===========================================================================
  MetricsWide AS (
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