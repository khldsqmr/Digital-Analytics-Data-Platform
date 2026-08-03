/* =================================================================================================
FILE:         sdi_sp_dashboardPulseTms_bronze_qgp_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_qgp_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_bronze_qgp_weekly.
  Lands the enterprise QGP scorecard feed (prdrzrlakehouse.qgp_restricted.qgpweeklyview) into
  PulseTMS Bronze, deduped to the latest insert per grain key.

SOURCE:
  prdrzrlakehouse.qgp_restricted.qgpweeklyview -- a different catalog (prdrzrlakehouse, not
  prdrzranalytics) from everything else in this pipeline. This is an enterprise-wide KPI
  scorecard feed, not PulseTMS-specific, so the WHERE clause below scopes it down.

GRAIN:
  One row per week_ending x metric_id x date_context x metric_type x page.
  ('page' added to the grain/partition below -- see DEDUP GRAIN AUDIT.)

--------------------------------------------------------------------------------------------------
DEDUP GRAIN AUDIT (every dimension below was checked against live data, not assumed):

  page            -- MUST be in the partition. Several metrics publish the identical true value
                     twice under two different Page tags (e.g. Store Traffic under 'Branded Retail
                     Total' and 'Branded Retail Total Outcomes'; several Digital % metrics under
                     'Digital Transformation NEW' and 'Digital Transformation NEW Outcomes 2').
                     Confirmed via direct query: every week, every metric checked, both pages carry
                     byte-identical Amount values -- so this isn't a value discrepancy, but without
                     page in the partition, a week's row could get dropped by the dedup tie-break
                     landing on the "other" page relative to whatever page Silver filters on
                     downstream. Adding page here fixed exactly that: a prior run without it showed
                     scattered, nondeterministic nulls on weeks that had real data; re-running with
                     page included resolved them completely.
  date_context    -- Confirmed necessary and already correct. DateContext='Normal' is the true
                     weekly grain; QTD and QTR rows are cumulative ROLLUPS of the Normal weekly
                     values, not independent numbers -- proven directly: summing every 'Normal'
                     week in a quarter reproduces that quarter's 'QTR' value exactly, and a partial
                     sum reproduces 'QTD'. Without date_context in the partition, a rollup row
                     could win the InsertDateTime tie-break over the true weekly Normal row.
  metric_type     -- Necessary (Actuals / Actuals/Outlook / QGP are genuinely different series,
                     not duplicates).
  publish_key     -- Checked and found NOT to vary: the entire source table currently has exactly
                     one PublishKey value across all ~400K rows and 46 weeks of InsertDateTime
                     history. It is not functioning as a load-batch identifier the way MFC's
                     file_load_datetime does -- there is currently nothing to "pick the latest
                     batch" from. Kept as a secondary ORDER BY tiebreaker below for defensiveness
                     in case that ever changes, but it is not doing meaningful work today.
  is_future       -- Checked directly for the 10 QGP metrics below (any cell with >1 distinct
                     IsFuture value): zero rows returned. No conflict found; not added to the
                     partition.
  display_metric_type -- Not independently verified with a query, but every sample seen across
                     this whole investigation shows it locked 1:1 with metric_type (metric_type=
                     'QGP' always pairs with display_metric_type='Target'), i.e. a derived label
                     rather than an independent axis. Treated as low-risk, not added to the
                     partition.

VERSIONING:
  Rows for the same (week_ending, metric_id, date_context, metric_type, page) combination get
  re-inserted over time as values are corrected/republished. Deduped to the latest insert_datetime
  per grain key (same pattern as adobeFunnel Bronze's file_load_datetime dedup), with publish_key
  as a secondary tiebreaker (see note above).

SCOPE FILTER -- MetricID casing confirmed vs. unconfirmed:
  This source is enterprise-wide, so it's scoped below to just the MetricIDs behind your existing
  Activations / Store Traffic / VR Calls / VR Chats / VR Postpaid Activations / Digital % Tableau
  calcs. Casing below is copied VERBATIM from live table output wherever confirmed; two IDs remain
  best-effort reconstructions because the metric itself has never appeared in the source (see
  inline flags on each literal below):

    CONFIRMED (seen verbatim in raw table output):
      ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital
      ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital
      ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital
      VRInboundCallsinclHSIAutomatedManual
      VRChatsinclHSIAutomatedManual
      ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect
      DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect
      NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect
      StoreTrafficPostpaidAutomated (matched by MetricName below, not MetricID -- see note)
      ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital       (activationsNewAalNoAssistance, see RESOLVED note below)
      ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital    (activationsNewAalNoAssistance, see RESOLVED note below)
      TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped             (activationsNewAalNoAssistance, see RESOLVED note below)
      DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA (digitalPctConsumerPostpaidActivationsTotalInclAssisted, see RESOLVED note below)

    RESOLVED (originally scoped under a different, nonexistent MetricID -- see below):
      activationsNewAalNoAssistance was originally scoped to
      TotalDigitalPhoneActivationsTMOAndSprintGlanceTM1Mapped, which is confirmed absent from
      the source (a fuzzy substring search across the whole table found nothing under any
      spelling). Investigating further: the dashboard's "Digital Transformation NEW" page has
      two separate sections -- Section4 (Phone) and Section6 (BTS) -- and only Section6 has a
      metric labeled "Activations (New + AALs)" at all; Section4's Phone metrics are labeled
      plain "Activations" (no AAL variant exists for Phone). The original calc's own substring
      filter -- CONTAINS "activations (new + aal" -- could therefore only ever have matched a BTS
      metric, yet it was paired with a Phone-shaped MetricID -- almost certainly a copy-paste
      error in the original calc rather than a genuinely retired Phone metric. Confirmed via
      exact arithmetic match across 5+ weeks: TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped
      (MetricType=Actuals/Outlook, has its own QGP/Target side) = ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital
      + ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital, every week checked --
      the identical structural relationship already trusted for the Phone aggregate
      (ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital = Phone BOPIS + Phone Non-BOPIS).
      Now wired into Silver the same way as activationsBopis -- see Silver's header.

      digitalPctConsumerPostpaidActivationsTotalInclAssisted was originally scoped to
      DigitalPctofConsumerPostpaidActivationsTotalInclAssistedNEW on Page='Digital', both
      confirmed nonexistent (Page='Digital' returns zero rows; a %digital% sweep of every Page
      value in the table shows only the Digital Transformation NEW family and Hero - Digital
      exist -- no bare "Digital" page under any spelling). Pulling the dashboard's IT Summary
      page directly found DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA
      sitting at MetricOrder 6190 with MetricName "Digital % of Postpaid Activations (No
      Assistance + Assistance)" -- a near-verbatim match to the metric's intended concept -- as
      the Total row of a 3-metric family (6190 Total, 6210 No Assistance component, 6230
      Assistance component), the same "Total = component + component" shape already trusted
      elsewhere in this file. Confirmed MetricType=QGP/DisplayMetricType=Target exists, and
      IT Summary / IT Summary Outcomes carry byte-identical values (same duplicate-page pattern
      as Store Traffic) -- pinned to Page='IT Summary'. An apparent one-week value swing
      (~0.49 -> ~0.31) turned out to be a clean, consistent step at the actuals-to-outlook
      boundary (IsFuture flips from 'Is Past' to 'Is Future' at the same point), not a data
      quality issue -- every post-boundary week holds steady in the new ~0.31-0.34 band. Now
      wired into Silver.

    UNCONFIRMED CASING (the metric genuinely exists and matches real data reliably -- the WHERE
    clause below is case-insensitive via UPPER(), so this doesn't affect correctness -- but the
    exact mixed-case spelling has never been independently verified against a raw row, only
    reconstructed from the naming convention seen elsewhere in this feed):
      VRPostpaidActivationsinclVirtualBusinessTM1Mapped

  Store Traffic has no MetricID in the original calc (identified by MetricName only: exact raw
  casing 'Store Traffic (excl Store-In-Store)', though the comparison below is done
  case-insensitively). Its actual MetricID, StoreTrafficPostpaidAutomated, was confirmed
  separately in a raw dump but the name-match is kept as the primary filter to preserve the
  original calc's logic exactly.

DATA QUALITY NORMALIZATION APPLIED:
  - IsFuture arrives as 'Is Future', 'IsFuture', and 'Is Past' (inconsistent spacing/casing)
    -> normalized to a clean boolean is_future column.
  - MetricName arrives with leading whitespace (e.g. '     Business') -> trimmed.
    Renamed to metric_name_label since it's a segment/category label (e.g. 'Business'), not
    the metric identifier itself -- MetricID is what plays that role elsewhere in this
    pipeline's metric_name convention, so this avoids a naming collision once this stacks
    into gold_unified_long.

REPORTING LAG (observed, not a bug):
  MetricType='Actuals' (no Outlook component) rows for these metrics currently stop around
  2026-08-01 (a week earlier, 2026-07-25, for VR Calls/VR Chats) and are legitimately absent
  -- not missing/dropped -- for any week after that, since real reporting hasn't caught up yet.
  MetricType='Actuals/Outlook' rows carry a forward-looking blended estimate and continue further
  into the future, though how far depends on that metric's own publish horizon (observed to lag
  slightly behind "now" as well -- don't expect Outlook rows to be populated all the way through
  the current week if queried very close to real time).
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_bronze_qgp_weekly()
LANGUAGE SQL
AS
BEGIN

  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_qgp_weekly
  USING DELTA
  CLUSTER BY (week_ending, metric_id, date_context, metric_type)
  COMMENT 'PulseTMS Bronze — enterprise QGP scorecard feed, deduped to latest insert per week_ending x metric_id x date_context x metric_type x page. Scoped to the 10 PulseTMS QGP metrics (Activations BOPIS, Activations New+AAL No Assistance, Store Traffic, VR Calls, VR Chats, VR Postpaid Activations, 3 Digital % metrics). Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_qgp_weekly.'
  AS
  WITH Mapped AS (
    SELECT
      TRY_CAST(raw.PublishKey AS DATE)              AS publish_key,
      raw.QuarterNum                                AS quarter_num,
      raw.YearNum                                   AS year_num,
      TRY_CAST(raw.WeekEnding AS DATE)              AS week_ending,
      TRIM(raw.MetricID)                            AS metric_id,
      TRIM(raw.DateContext)                         AS date_context,       -- Normal | Monthly Normal | PQWA | ROQ | QTD | QTR | WoW
      TRIM(raw.Page)                                AS page,
      TRIM(raw.Section)                             AS section,
      TRIM(raw.Header)                              AS header,
      TRIM(raw.MetricName)                          AS metric_name_label, -- segment/category label, e.g. 'Business' -- NOT the metric identifier
      TRIM(raw.MetricType)                          AS metric_type,       -- Actuals | Actuals/Outlook | QGP
      TRIM(raw.DisplayMetricType)                   AS display_metric_type,
      raw.MetricFormat                              AS metric_format,
      raw.DaysInArrears                             AS days_in_arrears,
      raw.CumulativeDates                           AS cumulative_dates,
      -- Normalize inconsistent IsFuture spacing/casing ('Is Future' / 'IsFuture' / 'Is Past') to a clean boolean
      CASE
        WHEN LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'isfuture' THEN TRUE
        WHEN LOWER(REPLACE(TRIM(raw.IsFuture), ' ', '')) = 'ispast'   THEN FALSE
        ELSE NULL
      END                                            AS is_future,
      raw.MetricOrder                               AS metric_order,
      raw.VarianceDirection                         AS variance_direction,
      raw.LevelofPrecision                          AS level_of_precision,
      TRY_CAST(raw.Amount AS DOUBLE)                AS amount,
      raw.DisplayAmount                             AS display_amount,
      TRY_CAST(raw.VariancePercentage AS DOUBLE)    AS variance_percentage,
      raw.VarianceColor                             AS variance_color,
      raw.MetricOwner                               AS metric_owner,
      raw.DataDictionaryURL                         AS data_dictionary_url,
      raw.DrillDownURL1                             AS drill_down_url_1,
      raw.DrillDownURL2                             AS drill_down_url_2,
      TRY_CAST(raw.InsertDateTime AS TIMESTAMP)     AS insert_datetime
    FROM prdrzrlakehouse.qgp_restricted.qgpweeklyview raw
    -- Scoped to the MetricIDs behind your existing Tableau calcs. See the SCOPE FILTER section in
    -- the header above for which literals are confirmed verbatim vs. best-effort reconstructions.
    WHERE UPPER(TRIM(raw.MetricID)) IN (
      UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),      -- confirmed
      UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital'),   -- confirmed
      UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital'),                     -- confirmed
      UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),              -- confirmed, activationsNewAalNoAssistance, see RESOLVED note in header
      UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital'),           -- confirmed, activationsNewAalNoAssistance, see RESOLVED note in header
      UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped'),                    -- confirmed, activationsNewAalNoAssistance, see RESOLVED note in header
      UPPER('VRInboundCallsinclHSIAutomatedManual'),                                       -- confirmed
      UPPER('VRChatsinclHSIAutomatedManual'),                                              -- confirmed
      UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped'),                          -- ⚠ UNCONFIRMED casing, metric exists and matches fine (case-insensitive), see header
      UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect'),        -- confirmed
      UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA'), -- confirmed, digitalPctConsumerPostpaidActivationsTotalInclAssisted, see RESOLVED note in header
      UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect'),        -- confirmed
      UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect') -- confirmed
    )
    OR LOWER(TRIM(raw.MetricName)) = 'store traffic (excl store-in-store)'  -- confirmed exact raw casing: 'Store Traffic (excl Store-In-Store)'
  ),
  Deduped AS (
    SELECT *
    FROM Mapped
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY week_ending, metric_id, date_context, metric_type, page
      ORDER BY insert_datetime DESC, publish_key DESC
    ) = 1
  )
  SELECT * FROM Deduped;

END;