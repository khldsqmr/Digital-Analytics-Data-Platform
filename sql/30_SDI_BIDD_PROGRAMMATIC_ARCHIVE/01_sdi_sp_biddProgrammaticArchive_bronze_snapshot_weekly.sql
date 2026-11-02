/* =================================================================================================
FILE:           01_sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly.sql
PLATFORM:       Databricks
PROJECT:        biddProgrammaticArchive (Biddable Programmatic spend archive)
LAYER:          Bronze
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly
TABLE:          sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly

WHY THIS PROJECT EXISTS:
  The Programmatic spend source

    prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

  holds the CURRENT YEAR only. When it rolls to a new year the prior year's rows disappear.
  PulseTMS rebuilds its Biddable Bronze from scratch every run, so reading that source directly
  would silently drop all prior-year Programmatic spend (Biddable "Programmatic - All",
  "All Channels" and LOB = ALL history would shrink, WoW across the year change would break,
  and Programmatic YoY could never be computed).

  Same idea and same 3-layer shape as the QGP archive:
    sdi_tbl_qgpArchive_bronze_retained_weekly    ->  sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly
    sdi_tbl_qgpArchive_silver_conformed_weekly   ->  sdi_tbl_biddProgrammaticArchive_silver_conformed_weekly
    sdi_tbl_qgpArchive_gold_curated_weekly       ->  sdi_tbl_biddProgrammaticArchive_gold_curated_weekly
  The project is PulseTMS-agnostic: every LOB, DSP, channel, buy type and account is kept.

THIS LAYER (Bronze, retained):
  A never-shrinking copy of the raw source. Every source column unchanged, plus:
    archive_date           CAST(source date AS DATE). Clustering and replace key.
    archive_run_id         BIDDPROGARCH_yyyyMMdd_HHmmss_SSS of the run that loaded the row.
    archive_loaded_ts      Timestamp of that run.
    archive_source_object  Fully qualified source object the row was copied from.

HOW EACH RUN WORKS (date-scoped replace, safe order):
  The source has no reliable row-level key (browser / placement grain), so instead of the QGP
  archive's key-based MERGE, each run replaces whole dates:

  1. GUARD   Source returns no dated rows -> stop with an error. Archive untouched.
  2. CREATE  Create the table on the first run only (source columns + archive columns).
  3. INSERT  Copy every dated source row, stamped with this run's archive_run_id.
  4. DELETE  Remove OLDER copies of exactly the dates this run just loaded.

  Effect:
    - Dates still in the source are refreshed every run (corrections / late spend picked up,
      same behavior PulseTMS has today reading the source directly).
    - Dates no longer in the source (prior years after the rollover) are never touched.

  Why INSERT before DELETE:
    If step 3 fails, nothing was deleted. If step 4 fails, a date briefly has two copies; Silver
    keeps only the latest run per date, so nothing downstream double counts. The next
    successful run removes the old copy.

SCHEMA CHANGES IN THE SOURCE:
  The insert is positional (source columns, then archive columns). If Improvado adds, removes
  or reorders a source column, step 3 fails loudly BEFORE anything is deleted, so no data is
  lost. Fix: align the archive's columns with the source (add the column in the matching
  position), then re-run.

RUN ORDER:
  Bronze -> Silver -> Gold, via sdi_sp_biddProgrammaticArchive_orchestration_allLayers_weekly,
  which PulseTMS orchestration calls BEFORE sdi_sp_dashboardPulseTms_bronze_biddableSpend_weekly.

FIRST DEPLOYMENT:
  Must have run at least once before the source rolls over to the next year: history can only
  be kept from the first run on.

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Biddable Programmatic archive, Bronze: copies prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr (current-year-only source) into sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly, refreshing dates still in the source and never deleting dates the source has dropped.'

AS
BEGIN

  DECLARE v_ts            TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_run_id        STRING;
  DECLARE v_source_rows   BIGINT;
  DECLARE v_source_min    DATE;
  DECLARE v_source_max    DATE;


  SET v_run_id =
    CONCAT('BIDDPROGARCH_', DATE_FORMAT(v_ts, 'yyyyMMdd_HHmmss_SSS'));


  /* ===============================================================================================
     STEP 1 - GUARD: never touch the archive if the source is empty
     =============================================================================================== */

  SET v_source_rows =
  (
    SELECT COUNT(*)
    FROM   prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
    WHERE  CAST(raw.`date` AS DATE) IS NOT NULL
  );

  SET v_source_min =
  (
    SELECT MIN(CAST(raw.`date` AS DATE))
    FROM   prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
  );

  SET v_source_max =
  (
    SELECT MAX(CAST(raw.`date` AS DATE))
    FROM   prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
  );

  IF v_source_rows IS NULL OR v_source_rows = 0 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT =
        'Biddable Programmatic archive: source pbi_programmatic_browsers_currentyr returned no dated rows. Archive left unchanged.';
  END IF;


  /* ===============================================================================================
     STEP 2 - CREATE THE TABLE ON THE FIRST RUN ONLY
     =============================================================================================== */

  CREATE TABLE IF NOT EXISTS
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly

  USING DELTA

  CLUSTER BY (archive_date)

  COMMENT 'Biddable Programmatic archive, Bronze (retained). Never-shrinking copy of prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr, which only holds the current year. All source columns unchanged, plus archive_date (source date as DATE), archive_run_id, archive_loaded_ts, archive_source_object. Dates still in the source are refreshed every run; dates the source has dropped are kept. Loaded by sdi_sp_biddProgrammaticArchive_bronze_snapshot_weekly.'

  AS
  SELECT
    CAST(raw.`date` AS DATE)                AS archive_date,
    raw.*,
    CAST(NULL AS STRING)                    AS archive_run_id,
    CAST(NULL AS TIMESTAMP)                 AS archive_loaded_ts,
    CAST(NULL AS STRING)                    AS archive_source_object
  FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
  WHERE FALSE;


  /* ===============================================================================================
     STEP 3 - INSERT THIS RUN'S COPY OF EVERY DATED SOURCE ROW
     =============================================================================================== */

  INSERT INTO
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly

  SELECT
    CAST(raw.`date` AS DATE),
    raw.*,
    v_run_id,
    v_ts,
    'prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr'
  FROM prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw
  WHERE CAST(raw.`date` AS DATE) IS NOT NULL;


  /* ===============================================================================================
     STEP 4 - REMOVE OLDER COPIES OF THE DATES THIS RUN LOADED
     Dates outside this run (e.g. prior years after the rollover) are never touched.
     =============================================================================================== */

  DELETE FROM
    prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly

  WHERE archive_date BETWEEN v_source_min AND v_source_max
    AND NOT (archive_run_id <=> v_run_id)
    AND archive_date IN (
      SELECT DISTINCT a.archive_date
      FROM   prdrzranalytics.lab42.sdi_tbl_biddProgrammaticArchive_bronze_retained_weekly a
      WHERE  a.archive_run_id = v_run_id
    );

END;