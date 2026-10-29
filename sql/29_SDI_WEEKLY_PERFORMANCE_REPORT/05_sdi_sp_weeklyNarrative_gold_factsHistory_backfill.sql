/* =================================================================================================
FILE:           05_sdi_sp_weeklyNarrative_gold_factsHistory_backfill.sql
PLATFORM:       Databricks
LAYER:          Gold (one-off / on-demand backfill)
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_weeklyNarrative_gold_factsHistory_backfill

PURPOSE:
  Fills sdi_tbl_weeklyNarrative_gold_factsHistory_perRun for a range of past weeks by calling
  sdi_sp_weeklyNarrative_gold_factsHistory_perRun once per Saturday, oldest week first.

  Needed once before go-live so that:
    - the What moved view has prior weeks to compare against (13-week highs / lows), and
    - Track B (past reports) can be lined up week by week against the numbers.

PARAMETERS:
  p_start_week_end  First Saturday to snapshot (required). Pick a week where PulseTMS Gold
                    already has data; the snapshot procedure errors on a week with no data.
  p_end_week_end    Last Saturday to snapshot. NULL = latest completed Saturday.
  p_skip_existing   TRUE (default) = skip weeks that already have a snapshot, so the backfill
                    can be re-run safely and resumes where it stopped after a failure.
                    FALSE = take a fresh snapshot of every week (new snapshot_id per week).

LINEAGE:
  Every snapshot written by one backfill call shares the same job_run_id
  (WKNARR_BACKFILL_yyyyMMdd_HHmmss_SSS) with run_type = MANUAL, so a whole backfill can be
  identified or removed as one batch.

NOTE ON BACKFILLED VALUES:
  Each backfilled week only uses PulseTMS Gold dates up to its own week_end, but the values
  are TODAY'S Gold values (including any later PulseTMS restatements), not what Gold showed at
  the time. Fine for history and averages; keep it in mind when backtesting the drafter.

FAILURE / RESUME:
  If one week fails (e.g. no PulseTMS data that week), the procedure stops. Fix or move
  p_start_week_end and call it again with p_skip_existing = TRUE; finished weeks are skipped.

EXAMPLE:
  -- two years up to the latest completed week
  CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_backfill(DATE'2024-11-02', NULL, TRUE);

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_backfill
(
  IN p_start_week_end DATE,
  IN p_end_week_end   DATE    DEFAULT NULL,
  IN p_skip_existing  BOOLEAN DEFAULT TRUE
)

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Weekly narrative report: backfills sdi_tbl_weeklyNarrative_gold_factsHistory_perRun one Saturday at a time by calling sdi_sp_weeklyNarrative_gold_factsHistory_perRun. Resume-safe when p_skip_existing = TRUE.'

AS
BEGIN

  DECLARE v_batch_id      STRING;
  DECLARE v_week          DATE;
  DECLARE v_end           DATE;
  DECLARE v_table_exists  BIGINT;
  DECLARE v_existing      BIGINT;


  SET v_batch_id =
    CONCAT('WKNARR_BACKFILL_', DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd_HHmmss_SSS'));


  /* ===============================================================================================
     VALIDATE PARAMETERS
     =============================================================================================== */

  IF sdi_sp_weeklyNarrative_gold_factsHistory_backfill.p_start_week_end IS NULL
     OR DAYOFWEEK(sdi_sp_weeklyNarrative_gold_factsHistory_backfill.p_start_week_end) <> 7 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Backfill: p_start_week_end is required and must be a Saturday.';
  END IF;


  SET v_end =
    COALESCE(
      sdi_sp_weeklyNarrative_gold_factsHistory_backfill.p_end_week_end,
      (
        SELECT cal.qgp_date
        FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
        WHERE  cal.is_complete_period = TRUE
          AND  cal.qgp_date <= CURRENT_DATE()
          AND  DAYOFWEEK(cal.qgp_date) = 7
        ORDER BY cal.qgp_date DESC
        LIMIT 1
      )
    );


  IF v_end IS NULL OR DAYOFWEEK(v_end) <> 7 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Backfill: p_end_week_end must be a Saturday (or NULL for the latest completed Saturday).';
  END IF;


  IF v_end < sdi_sp_weeklyNarrative_gold_factsHistory_backfill.p_start_week_end THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'Backfill: p_end_week_end is earlier than p_start_week_end.';
  END IF;


  /* ===============================================================================================
     LOOP OVER SATURDAYS, OLDEST FIRST
     =============================================================================================== */

  SET v_week = sdi_sp_weeklyNarrative_gold_factsHistory_backfill.p_start_week_end;

  WHILE v_week <= v_end DO

    /* The history table is created by the first snapshot call, so check it exists before
       looking for existing snapshots. information_schema stores names in lower case. */
    SET v_table_exists =
    (
      SELECT COUNT(*)
      FROM   prdrzranalytics.information_schema.tables
      WHERE  table_schema = 'lab42'
        AND  table_name   = 'sdi_tbl_weeklynarrative_gold_factshistory_perrun'
    );

    SET v_existing = 0;

    IF sdi_sp_weeklyNarrative_gold_factsHistory_backfill.p_skip_existing AND v_table_exists > 0 THEN
      SET v_existing =
      (
        SELECT COUNT(*)
        FROM   prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_perRun
        WHERE  week_end = v_week
      );
    END IF;

    IF v_existing = 0 THEN
      CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_perRun(v_week, 'MANUAL', v_batch_id);
    END IF;

    SET v_week = date_add(v_week, 7);

  END WHILE;

END;