/* =================================================================================================
FILE:         05_sdi_sp_dashboardPulseTms_bronze_qgp_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_dashboardPulseTms_bronze_qgp_weekly

PURPOSE:
  Creates/refreshes physical table sdi_tbl_dashboardPulseTms_bronze_qgp_weekly.
  Lands the enterprise QGP scorecard feed (prdrzrlakehouse.qgp_restricted.qgpweeklyview) into
  PulseTMS Bronze, deduped to the latest insert per grain key.

SOURCE:
  prdrzrlakehouse.qgp_restricted.qgpweeklyview -- a different catalog (prdrzrlakehouse, not
  prdrzranalytics) from everything else in this pipeline. This is an enterprise-wide KPI
  scorecard feed, not PulseTMS-specific -- see the WHERE clause note below.

GRAIN:
  One row per week_ending x metric_id x date_context x metric_type.

VERSIONING:
  The same (week_ending, metric_id, date_context, metric_type) combination gets re-inserted
  over time as values are corrected/republished. Deduped here to the latest insert_datetime
  per grain key, same pattern as adobeFunnel Bronze's file_load_datetime dedup.

SCOPE FILTER (now resolved from your existing Tableau calcs):
  This source is enterprise-wide (the earlier sample MetricID, 5GBroadbandDisconnectRateBusinessManual,
  isn't one of PulseTMS's own). Scoped below to the 11 MetricIDs plus the one name-only metric
  (Store Traffic) behind your Activations/Store Traffic/VR Calls/VR Chats/VR Postpaid Activations/
  Digital % Tableau calcs. If a new metric gets added to those dashboards later, its MetricID needs
  adding to this list too.

DATA QUALITY NORMALIZATION APPLIED:
  - IsFuture arrives as 'Is Future', 'IsFuture', and 'Is Past' (inconsistent spacing/casing)
    -> normalized to a clean boolean is_future column.
  - MetricName arrives with leading whitespace (e.g. '     Business') -> trimmed.
    Renamed to metric_name_label since it's a segment/category label (e.g. 'Business'), not
    the metric identifier itself -- MetricID is what plays that role elsewhere in this
    pipeline's metric_name convention, so this avoids a naming collision once this stacks
    into gold_unified_long.

STILL OPEN (blocks Silver, not this file):
  - Whether Silver should trust this source's own VariancePercentage / WoW-context rows, or
    independently compute wow/yoy the same way adobeFunnel/mfcSpend/platformSpend Silver do
    (self-joins against the calendar dim), for a consistent wow_pct/yoy_pct meaning across
    every data_source in gold_unified_long.
  - Whether week_ending values here actually land on the same dates as
    sdi_vw_dashboardPulseTms_dim_qgp_calendar.qgp_date (including the BOUNDARY_STUB/
    BOUNDARY_FIRST quarter-boundary dates) -- if so, joining directly on that column is clean;
    if not, Silver needs its own alignment logic.
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
  COMMENT 'PulseTMS Bronze — enterprise QGP scorecard feed, deduped to latest insert per week_ending x metric_id x date_context x metric_type. Not yet filtered to PulseTMS-specific metrics. Refreshed weekly via sdi_sp_dashboardPulseTms_bronze_qgp_weekly.'
  AS
  WITH Mapped AS (
    SELECT
      TRY_CAST(raw.PublishKey AS DATE)            AS publish_key,
      raw.QuarterNum                               AS quarter_num,
      raw.YearNum                                  AS year_num,
      TRY_CAST(raw.WeekEnding AS DATE)             AS week_ending,
      TRIM(raw.MetricID)                           AS metric_id,
      TRIM(raw.DateContext)                        AS date_context,       -- Normal | Monthly Normal | PQWA | ROQ | QTD | WoW
      TRIM(raw.Page)                                AS page,
      TRIM(raw.Section)                             AS section,
      TRIM(raw.Header)                              AS header,
      TRIM(raw.MetricName)                          AS metric_name_label, -- segment/category label, e.g. 'Business' -- NOT the metric identifier
      TRIM(raw.MetricType)                          AS metric_type,       -- QGP | Actuals/Outlook
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
      raw.VarianceDirection                          AS variance_direction,
      raw.LevelofPrecision                           AS level_of_precision,
      TRY_CAST(raw.Amount AS DOUBLE)                AS amount,
      raw.DisplayAmount                              AS display_amount,
      TRY_CAST(raw.VariancePercentage AS DOUBLE)    AS variance_percentage,
      raw.VarianceColor                              AS variance_color,
      raw.MetricOwner                                AS metric_owner,
      raw.DataDictionaryURL                          AS data_dictionary_url,
      raw.DrillDownURL1                              AS drill_down_url_1,
      raw.DrillDownURL2                              AS drill_down_url_2,
      TRY_CAST(raw.InsertDateTime AS TIMESTAMP)      AS insert_datetime
    FROM prdrzrlakehouse.qgp_restricted.qgpweeklyview raw
    -- Scoped to the MetricIDs behind your existing Tableau calcs (Activations BOPIS, Digital
    -- Activations, Store Traffic, VR Calls/Chats, VR Postpaid Activations, Digital % metrics).
    -- Filtering on metric_id rather than Page: a couple of the Actuals-side calcs (VR Calls, VR
    -- Chats) carry no Page filter at all, so Page tagging isn't reliable enough to scope on alone.
    -- Store Traffic has no MetricID in the original calcs (identified by metric_name_label only),
    -- so it's matched separately below and isn't part of this list.
    WHERE UPPER(TRIM(raw.MetricID)) IN (
      UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),
      UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital'),
      UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital'),
      UPPER('TotalDigitalPhoneActivationsTMOAndSprintGlanceTM1Mapped') /* ⚠ UNCONFIRMED casing, see note */,
      UPPER('VRInboundCallsinclHSIAutomatedManual'),
      UPPER('VRChatsinclHSIAutomatedManual'),
      UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped') /* ⚠ UNCONFIRMED casing, see note */,
      UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect'),
      UPPER('DigitalPctofConsumerPostpaidActivationsTotalInclAssistedNEW'),
      UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect'),
      UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
    )
    OR LOWER(TRIM(raw.MetricName)) = 'store traffic (excl store-in-store)'
  ),
  Deduped AS (
    SELECT *
    FROM Mapped
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY week_ending, metric_id, date_context, metric_type
      ORDER BY insert_datetime DESC, publish_key DESC
    ) = 1
  )
  SELECT * FROM Deduped;

END;