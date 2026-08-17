/* =================================================================================================
FILE:         sdi_sp_qgpArchive_bronze_retained_weekly.sql
LAYER:        Stored Procedure
PROCEDURE:    sdi_sp_qgpArchive_bronze_retained_weekly

PURPOSE:
  Standalone, PulseTMS-agnostic archive of the FULL enterprise QGP scorecard feed
  (prdrzrlakehouse.qgp_restricted.qgpweeklyview). Exists to solve exactly one problem: that
  source table is a rolling window -- it appears to retain only the trailing 2-3 quarters and
  drops older data outright. Every other procedure in this pipeline (PulseTMS's own Bronze
  included) uses CREATE OR REPLACE TABLE, which fully rebuilds from whatever the source
  currently contains -- meaning any procedure reading directly from that feed would silently
  and permanently lose historical weeks the moment the source ages them out, with nothing in
  the pipeline ever flagging that it happened.

  This procedure is the fix, isolated to one place: it MERGEs into a persistent table instead
  of replacing it, so once a row has been captured here, it stays -- even after the raw source
  no longer has it. PulseTMS's Bronze (sdi_sp_dashboardPulseTms_bronze_qgp_weekly) then reads
  FROM this table instead of the raw feed directly, so its own logic, predicates, and every fix
  made to it in this whole investigation stay completely unchanged -- only its FROM clause
  changes, from the raw feed to this table. See that file's own header for confirmation of
  exactly which one line changed.

SCOPE:
  Full raw feed, NOT scoped to PulseTMS's 10 metrics. The whole point of this table is to be a
  durable safety net for the feed generally -- narrowing it to just what PulseTMS currently
  uses would recreate the same "we didn't know we'd need this metric until later" problem this
  whole thread kept running into (activationsNewAalNoAssistance, digitalPctConsumerPostpaid...
  TotalInclAssisted) at the archive level instead of just the consumption level.

GRAIN / MERGE KEY:
  WeekEnding, MetricID, DateContext, MetricType, Page -- the same 5-column grain PulseTMS's own
  Bronze already uses for its own dedup (see that file's DEDUP GRAIN AUDIT for why each of
  these is necessary). Using the identical grain here means PulseTMS's Bronze sees exactly the
  same shape of data reading from this table as it did reading directly from raw -- this table
  changes WHERE the data persists, not WHAT the data looks like.

SCHEMA:
  Deliberately mirrors the raw source's own column names and casing exactly (PublishKey,
  MetricID, WeekEnding, IsFuture, etc. -- not PulseTMS Bronze's normalized snake_case) so that
  PulseTMS Bronze's Mapped CTE (raw.MetricID, raw.Page, its own IsFuture normalization, its own
  TRY_CASTs) keeps working unmodified against this table. Numeric/date/timestamp columns are
  lightly typed here for storage efficiency; this is safe for any downstream consumer that
  re-casts them again (a TRY_CAST of an already-typed column is a harmless no-op) -- text
  columns are left untyped/untrimmed exactly as raw so no consumer's own normalization logic
  sees anything different than it would reading the live feed.

MERGE BEHAVIOR -- two distinct scenarios, handled two different ways:
  SCENARIO A: a grain key that existed in a previous run is no longer present in the raw
  source at all (the whole week/metric/page combination has aged out of the rolling window).
  Handled by WHEN NOT MATCHED BY SOURCE -> do nothing. Nothing in a MERGE INTO ... WHEN
  MATCHED / WHEN NOT MATCHED (BY TARGET) statement deletes unmatched target rows unless a
  WHEN NOT MATCHED BY SOURCE THEN DELETE clause is explicitly added -- which this intentionally
  does NOT have. The row simply stays exactly as it was, forever.

  SCENARIO B: a grain key IS still present in the raw source (still matched), but its Amount
  has gone from a real value to null (a data quality hiccup upstream, a value reset ahead of a
  correction, or any other reason a previously-populated week might momentarily show null
  again). This is NOT the same failure mode as Scenario A, and a naive WHEN MATCHED THEN
  UPDATE SET * would get it wrong -- a blind full-column overwrite would happily clobber a
  previously-good Amount with the new null, silently destroying retained data through the
  layer that exists specifically to prevent data loss. Handled instead by a column-by-column
  UPDATE where Amount, DisplayAmount, and VariancePercentage (the only columns actually derived
  from/dependent on Amount) use COALESCE(source.value, target.value) -- only overwritten when
  the incoming value is non-null, otherwise the previously-archived value is kept. Every other
  column still updates normally on every match, since there's no equivalent risk of legitimate
  metadata (Page, MetricOwner, etc.) needing the same protection.

  WHEN NOT MATCHED (BY TARGET; a genuinely new grain key, never seen before) -> INSERT.

SCHEDULING DEPENDENCY:
  Must run BEFORE sdi_sp_dashboardPulseTms_bronze_qgp_weekly in the orchestration script.
  PulseTMS's Bronze now depends on this table being current -- if this procedure hasn't run
  yet this cycle, PulseTMS's Bronze will simply see last cycle's archive state (stale but not
  wrong), not an error, but the ordering should still be enforced.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_qgpArchive_bronze_retained_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN

  -- Moved inside the procedure body (was a separate top-level statement before this file's
  -- deployment threw PARSE_SYNTAX_ERROR "extra input 'CREATE'" -- Databricks only accepts one
  -- top-level statement per submission, same reason the orchestration script needs one CALL
  -- per cell/task). IF NOT EXISTS makes this a harmless no-op on every run after the first.
  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_qgpArchive_bronze_retained_weekly (
    PublishKey            DATE,
    QuarterNum            INT,
    YearNum               INT,
    WeekEnding            DATE,
    MetricID              STRING,
    DateContext           STRING,
    Page                  STRING,
    Section               STRING,
    Header                STRING,
    MetricName            STRING,
    MetricType            STRING,
    DisplayMetricType     STRING,
    MetricFormat          STRING,
    DaysInArrears         INT,
    CumulativeDates       STRING,
    IsFuture              STRING,
    MetricOrder           INT,
    VarianceDirection     INT,
    LevelofPrecision      INT,
    Amount                DOUBLE,
    DisplayAmount         STRING,
    VariancePercentage    DOUBLE,
    VarianceColor         INT,
    MetricOwner           STRING,
    DataDictionaryURL     STRING,
    DrillDownURL1         STRING,
    DrillDownURL2         STRING,
    InsertDateTime        TIMESTAMP
  )
  USING DELTA
  CLUSTER BY (WeekEnding, MetricID, DateContext, MetricType)
  COMMENT 'Standalone, never-shrinking archive of the full enterprise QGP scorecard feed (qgpweeklyview), MERGE-upserted so historical weeks survive even after the rolling-retention raw source ages them out. Not scoped to PulseTMS -- full feed. PulseTMS Bronze reads from this table instead of the raw feed directly. Refreshed weekly via sdi_sp_qgpArchive_bronze_retained_weekly, must run before sdi_sp_dashboardPulseTms_bronze_qgp_weekly.';

  MERGE INTO prdrzranalytics.lab42.sdi_tbl_qgpArchive_bronze_retained_weekly AS target
  USING (
    SELECT
      TRY_CAST(raw.PublishKey AS DATE)           AS PublishKey,
      TRY_CAST(raw.QuarterNum AS INT)             AS QuarterNum,
      TRY_CAST(raw.YearNum AS INT)                AS YearNum,
      TRY_CAST(raw.WeekEnding AS DATE)            AS WeekEnding,
      raw.MetricID                                AS MetricID,
      raw.DateContext                             AS DateContext,
      raw.Page                                    AS Page,
      raw.Section                                 AS Section,
      raw.Header                                  AS Header,
      raw.MetricName                              AS MetricName,
      raw.MetricType                              AS MetricType,
      raw.DisplayMetricType                       AS DisplayMetricType,
      raw.MetricFormat                            AS MetricFormat,
      TRY_CAST(raw.DaysInArrears AS INT)          AS DaysInArrears,
      raw.CumulativeDates                         AS CumulativeDates,
      raw.IsFuture                                AS IsFuture,
      TRY_CAST(raw.MetricOrder AS INT)            AS MetricOrder,
      TRY_CAST(raw.VarianceDirection AS INT)      AS VarianceDirection,
      TRY_CAST(raw.LevelofPrecision AS INT)       AS LevelofPrecision,
      TRY_CAST(raw.Amount AS DOUBLE)              AS Amount,
      raw.DisplayAmount                           AS DisplayAmount,
      TRY_CAST(raw.VariancePercentage AS DOUBLE)  AS VariancePercentage,
      TRY_CAST(raw.VarianceColor AS INT)          AS VarianceColor,
      raw.MetricOwner                             AS MetricOwner,
      raw.DataDictionaryURL                       AS DataDictionaryURL,
      raw.DrillDownURL1                           AS DrillDownURL1,
      raw.DrillDownURL2                           AS DrillDownURL2,
      TRY_CAST(raw.InsertDateTime AS TIMESTAMP)   AS InsertDateTime
    FROM prdrzrlakehouse.qgp_restricted.qgpweeklyview raw
    -- Same latest-insert-per-grain-key dedup PulseTMS's own Bronze already applies -- see
    -- that file's DEDUP GRAIN AUDIT. Deliberately NOT deduped down to just PulseTMS's 10
    -- metrics; this stays the full feed.
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY raw.WeekEnding, raw.MetricID, raw.DateContext, raw.MetricType, raw.Page
      ORDER BY raw.InsertDateTime DESC, raw.PublishKey DESC
    ) = 1
  ) AS source
  ON  target.WeekEnding   = source.WeekEnding
  AND target.MetricID     = source.MetricID
  AND target.DateContext  = source.DateContext
  AND target.MetricType   = source.MetricType
  AND target.Page         = source.Page
  WHEN MATCHED THEN UPDATE SET
    target.PublishKey         = source.PublishKey,
    target.QuarterNum         = source.QuarterNum,
    target.YearNum            = source.YearNum,
    target.Section            = source.Section,
    target.Header             = source.Header,
    target.MetricName         = source.MetricName,
    target.DisplayMetricType  = source.DisplayMetricType,
    target.MetricFormat       = source.MetricFormat,
    target.DaysInArrears      = source.DaysInArrears,
    target.CumulativeDates    = source.CumulativeDates,
    target.IsFuture           = source.IsFuture,
    target.MetricOrder        = source.MetricOrder,
    target.VarianceDirection  = source.VarianceDirection,
    target.LevelofPrecision   = source.LevelofPrecision,
    -- Amount and its two derived columns are protected: only overwritten when the incoming
    -- value is non-null. A matched row whose source Amount has gone null (data quality hiccup,
    -- a reset ahead of a correction, anything upstream) keeps its last known good value instead
    -- of being clobbered -- this is the specific gap a plain UPDATE SET * would have left open.
    target.Amount              = COALESCE(source.Amount, target.Amount),
    target.DisplayAmount       = COALESCE(source.DisplayAmount, target.DisplayAmount),
    target.VariancePercentage  = COALESCE(source.VariancePercentage, target.VariancePercentage),
    target.VarianceColor       = source.VarianceColor,
    target.MetricOwner         = source.MetricOwner,
    target.DataDictionaryURL   = source.DataDictionaryURL,
    target.DrillDownURL1       = source.DrillDownURL1,
    target.DrillDownURL2       = source.DrillDownURL2,
    target.InsertDateTime      = source.InsertDateTime
  WHEN NOT MATCHED THEN INSERT *;
  -- No WHEN NOT MATCHED BY SOURCE clause -- this is intentional. A grain key present in a
  -- previous run's archive but absent from the current raw pull is left exactly as it was,
  -- not deleted. That's the entire mechanism that makes this table never shrink.

END;
