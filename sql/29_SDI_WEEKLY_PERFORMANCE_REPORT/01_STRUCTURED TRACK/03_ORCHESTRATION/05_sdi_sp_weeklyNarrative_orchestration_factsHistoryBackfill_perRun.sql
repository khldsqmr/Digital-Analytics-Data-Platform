/* FILE 05 — Track A / Orchestration.
   Run 02 once for a current week first so that factsHistory exists.
   Append-only backfill, oldest Saturday first; skip already snapshotted weeks by default.
   Historical restatements caveat: these snapshots reflect CURRENT source Gold values.
*/
CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_weeklyNarrative_orchestration_factsHistoryBackfill_perRun
(
  IN p_start_week_end DATE,
  IN p_end_week_end DATE DEFAULT NULL,
  IN p_skip_existing BOOLEAN DEFAULT TRUE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Append weekly facts snapshots over a Saturday range; resume-safe with p_skip_existing TRUE.'
AS
BEGIN
  DECLARE v_week DATE;
  DECLARE v_end DATE;
  DECLARE v_count BIGINT;
  DECLARE v_batch STRING;

  SET v_batch = CONCAT('WN_BACKFILL_', DATE_FORMAT(CURRENT_TIMESTAMP(), 'yyyyMMdd_HHmmss_SSS'), '_', REPLACE(UUID(),'-',''));
  IF p_start_week_end IS NULL OR DAYOFWEEK(p_start_week_end) <> 7 THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'Start must be a Saturday.';
  END IF;
  SET v_end = COALESCE(p_end_week_end, (
    SELECT MAX(qgp_date)
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar
    WHERE is_complete_period = TRUE AND DAYOFWEEK(qgp_date)=7 AND qgp_date < CURRENT_DATE()
  ));
  IF v_end IS NULL OR DAYOFWEEK(v_end)<>7 OR v_end>=CURRENT_DATE() OR v_end<p_start_week_end THEN
    SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT = 'End must be a completed Saturday >= start.';
  END IF;

  SET v_week = p_start_week_end;
  WHILE v_week <= v_end DO
    SET v_count = (
      SELECT COUNT(*) FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long
      WHERE week_end = v_week
    );
    IF NOT COALESCE(p_skip_existing,TRUE) OR v_count = 0 THEN
      CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(v_week,'MANUAL',v_batch);
    END IF;
    SET v_week = DATE_ADD(v_week,7);
  END WHILE;
END;
