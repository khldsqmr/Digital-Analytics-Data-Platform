-- ============================================================================
-- FILE  : 02_sdi_sp_mip_validation_postStage_perRun.sql
-- LAYER : VALIDATION
-- PURPOSE:
--   POST-stage quality validation.
--
-- POLICY:
--   - Availability/freshness/comparator gaps => INFO/WARNING, non-blocking.
--   - Broken declared grain/reconciliation invariants => FAILED, STOP.
--   - Gold App objects may be legitimately empty when comparison history is not
--     available; that is INFO rather than FAILED.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_validation_postStage_perRun(
    IN p_stageName       STRING,
    IN p_runId           STRING DEFAULT NULL,
    IN p_asOfDate        DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_weeksToCheck    INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'MIP POST-stage validation. Severe grain/reconciliation failures SIGNAL; normal availability/freshness/comparator gaps remain non-blocking.'
AS
BEGIN
    DECLARE v_stageName STRING DEFAULT upper(trim(p_stageName));
    DECLARE v_runId STRING DEFAULT coalesce(
        nullif(trim(p_runId), ''),
        concat(
            'MAN_',
            date_format(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'), 'yyyyMMdd_HHmmss'),
            '_',
            upper(substr(sha2(concat(cast(current_timestamp() AS STRING), cast(rand() AS STRING)), 256), 1, 8))
        )
    );
    DECLARE v_checkedAt TIMESTAMP DEFAULT current_timestamp();
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_windowEnd DATE;
    DECLARE v_windowStart DATE;
    DECLARE v_weekTo DATE;
    DECLARE v_weekFrom DATE;
    DECLARE v_stopCount BIGINT DEFAULT 0;

    IF v_stageName NOT IN (
        'BRONZE',
        'SILVER_HIT',
        'SILVER_SESSION',
        'SILVER_WEEK',
        'GOLD_OVERVIEW',
        'GOLD_BREAKOUT',
        'GOLD_CROSSTAB',
        'GOLD_EXPLORE',
        'GOLD_APP'
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Invalid p_stageName for MIP POST validation.';
    END IF;

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    IF p_weeksToCheck IS NULL OR p_weeksToCheck < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_weeksToCheck must be >= 1.';
    END IF;

    SET v_windowEnd = v_asOfDate;
    SET v_windowStart = date_add(v_asOfDate, -(p_eventWindowDays - 1));
    SET v_weekTo = date_add(v_asOfDate, 1 - dayofweek(v_asOfDate));
    SET v_weekFrom = date_add(v_weekTo, -7 * (p_weeksToCheck - 1));

    DELETE FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WHERE runId = v_runId
      AND validationPhase = 'POST'
      AND stageName = v_stageName;

    -- ------------------------------------------------------------------------
    -- Every stage gets an object-population check.
    -- Empty data is WARNING, not FAILED.
    -- ------------------------------------------------------------------------
    INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WITH population AS (
        SELECT 'BRONZE' AS stageName,'BRONZE' AS layerName,'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily' AS objectName,'eventDate' AS scopeType,v_windowStart AS scopeStart,v_windowEnd AS scopeEnd,count(*) AS rowCount,NULL AS sourceTs,max(_ingestedAt) AS targetTs
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName='BRONZE'

        UNION ALL

        SELECT 'SILVER_HIT','SILVER','prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily','eventDate',v_windowStart,v_windowEnd,count(*),NULL,max(silverProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName='SILVER_HIT'

        UNION ALL

        SELECT 'SILVER_SESSION','SILVER','prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily','sessionStartDatePst',v_windowStart,v_windowEnd,count(*),NULL,max(silverProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName='SILVER_SESSION'

        UNION ALL

        SELECT 'SILVER_SESSION','SILVER','prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily','sessionStartDatePst',v_windowStart,v_windowEnd,count(*),NULL,max(silverProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
        WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
        HAVING v_stageName='SILVER_SESSION'

        UNION ALL

        SELECT 'SILVER_WEEK','SILVER','prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly','weekStartDate',v_weekFrom,v_weekTo,count(*),NULL,max(silverProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName='SILVER_WEEK'

        UNION ALL

        SELECT 'SILVER_WEEK','SILVER','prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly','weekStartDate',v_weekFrom,v_weekTo,count(*),NULL,max(silverProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName='SILVER_WEEK'

        UNION ALL

        SELECT 'GOLD_OVERVIEW','GOLD','prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long','weekStartDate',v_weekFrom,v_weekTo,count(*),NULL,max(goldProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName='GOLD_OVERVIEW'

        UNION ALL

        SELECT 'GOLD_BREAKOUT','GOLD','prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long','weekStartDate',v_weekFrom,v_weekTo,count(*),NULL,max(goldProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName='GOLD_BREAKOUT'

        UNION ALL

        SELECT 'GOLD_CROSSTAB','GOLD','prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long','weekStartDate',v_weekFrom,v_weekTo,count(*),NULL,max(goldProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
        WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName='GOLD_CROSSTAB'

        UNION ALL

        SELECT 'GOLD_EXPLORE','GOLD','prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide','weekStartDate',v_weekFrom,v_weekTo,count(*),NULL,max(goldProcessedAt)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        HAVING v_stageName='GOLD_EXPLORE'
    )
    SELECT
        concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST',stageName,objectName,'objectPopulation'),256),1,24))),
        v_runId,v_checkedAt,v_asOfDate,'POST',stageName,layerName,objectName,scopeType,scopeStart,scopeEnd,
        CASE WHEN scopeType='weekStartDate' THEN scopeEnd ELSE NULL END,
        NULL,NULL,NULL,NULL,NULL,NULL,
        'objectPopulation','VOLUME','DATA_AVAILABILITY',
        1D,cast(rowCount AS DOUBLE),cast(rowCount-1 AS DOUBLE),NULL,
        CASE WHEN rowCount>0 THEN 'HEALTHY' ELSE 'WARNING' END,
        CASE WHEN rowCount>0 THEN 'INFO' ELSE 'MEDIUM' END,
        'PROCEED',FALSE,sourceTs,targetTs,
        CASE WHEN rowCount>0 THEN 'Stage output contains rows in the requested scope.' ELSE 'Stage output is empty in the requested scope.' END,
        CASE WHEN rowCount=0 THEN 'Upstream data may be late or the requested scope may not be loaded.' END,
        CASE WHEN rowCount=0 THEN 'Check source availability and the child procedure output before the next scheduled run.' END,
        'MIP Data Engineering',NULL,NULL,NULL,NULL
    FROM population;

    -- ------------------------------------------------------------------------
    -- BRONZE: duplicate link keys are warning-only because Silver deduplicates.
    -- ------------------------------------------------------------------------
    IF v_stageName='BRONZE' THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH d AS (
            SELECT coalesce(sum(cnt-1),0) AS duplicateRows
            FROM (
                SELECT row_identity_hash,event_date,source_table,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
                WHERE event_date BETWEEN v_windowStart AND v_windowEnd
                GROUP BY row_identity_hash,event_date,source_table
                HAVING count(*)>1
            )
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','BRONZE','duplicateHitSessionLinkKeys'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','BRONZE','BRONZE',
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily',
            'eventDate',v_windowStart,v_windowEnd,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
            'duplicateHitSessionLinkKeys','UNIQUENESS','DUPLICATE_KEY',
            0D,cast(duplicateRows AS DOUBLE),cast(duplicateRows AS DOUBLE),NULL,
            CASE WHEN duplicateRows=0 THEN 'HEALTHY' ELSE 'WARNING' END,
            CASE WHEN duplicateRows=0 THEN 'INFO' ELSE 'MEDIUM' END,
            'PROCEED',FALSE,NULL,NULL,
            CASE WHEN duplicateRows=0 THEN 'Bronze hit-session link keys are unique.' ELSE 'Duplicate Bronze hit-session link keys detected.' END,
            CASE WHEN duplicateRows>0 THEN 'Multiple hit-to-session assignments exist upstream.' END,
            CASE WHEN duplicateRows>0 THEN 'Review SEF assignments; Silver detailsPerHit currently deduplicates these keys defensively.' END,
            'EDL Engineering',NULL,NULL,NULL,NULL
        FROM d;
    END IF;

    -- ------------------------------------------------------------------------
    -- SILVER HIT: exact Bronze-hit preservation is a hard invariant.
    -- Sessionization coverage is warning-only.
    -- ------------------------------------------------------------------------
    IF v_stageName='SILVER_HIT' THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH c AS (
            SELECT
                (SELECT count(*) FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily WHERE event_date BETWEEN v_windowStart AND v_windowEnd) AS bronzeRows,
                (SELECT count(*) FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily WHERE eventDate BETWEEN v_windowStart AND v_windowEnd) AS silverRows
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','SILVER_HIT','detailsVsBronzeRowCount'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','SILVER_HIT','SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily',
            'eventDate',v_windowStart,v_windowEnd,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
            'detailsVsBronzeRowCount','RECONCILIATION','RECONCILIATION',
            cast(bronzeRows AS DOUBLE),cast(silverRows AS DOUBLE),cast(silverRows-bronzeRows AS DOUBLE),
            CASE WHEN bronzeRows=0 THEN NULL ELSE (silverRows-bronzeRows)/cast(bronzeRows AS DOUBLE) END,
            CASE WHEN silverRows=bronzeRows THEN 'HEALTHY' ELSE 'FAILED' END,
            CASE WHEN silverRows=bronzeRows THEN 'INFO' ELSE 'CRITICAL' END,
            CASE WHEN silverRows=bronzeRows THEN 'PROCEED' ELSE 'STOP' END,
            CASE WHEN silverRows=bronzeRows THEN FALSE ELSE TRUE END,
            NULL,NULL,
            CASE WHEN silverRows=bronzeRows THEN 'Silver detailsPerHit preserves the Bronze hit row count.' ELSE 'Silver detailsPerHit does not reconcile to Bronze hits.' END,
            CASE WHEN silverRows<>bronzeRows THEN 'A join/deduplication change altered the declared one-row-per-hit grain.' END,
            CASE WHEN silverRows<>bronzeRows THEN 'Reconcile Bronze hits to detailsPerHit before continuing downstream.' END,
            'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM c;

        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH s AS (
            SELECT
                count(*) AS totalRows,
                sum(CASE WHEN isSessionized=1 THEN 1 ELSE 0 END) AS sessionizedRows
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
            WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','SILVER_HIT','sessionizationCoverage'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','SILVER_HIT','SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily',
            'eventDate',v_windowStart,v_windowEnd,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
            'sessionizationCoverage','COVERAGE','DATA_QUALITY',
            0.97D,
            CASE WHEN totalRows=0 THEN 0D ELSE sessionizedRows/cast(totalRows AS DOUBLE) END,
            NULL,NULL,
            CASE WHEN totalRows>0 AND sessionizedRows/cast(totalRows AS DOUBLE)>=0.97D THEN 'HEALTHY' ELSE 'WARNING' END,
            CASE WHEN totalRows>0 AND sessionizedRows/cast(totalRows AS DOUBLE)>=0.97D THEN 'INFO' ELSE 'MEDIUM' END,
            'PROCEED',FALSE,NULL,NULL,
            CASE WHEN totalRows>0 AND sessionizedRows/cast(totalRows AS DOUBLE)>=0.97D THEN 'Sessionization coverage is within the working threshold.' ELSE 'Sessionization coverage is below the working threshold.' END,
            CASE WHEN totalRows=0 OR sessionizedRows/cast(totalRows AS DOUBLE)<0.97D THEN 'Session-event coverage may be incomplete or late.' END,
            CASE WHEN totalRows=0 OR sessionizedRows/cast(totalRows AS DOUBLE)<0.97D THEN 'Review sessionization availability; this warning does not block the pipeline.' END,
            'EDL Engineering',NULL,NULL,NULL,NULL
        FROM s;
    END IF;

    -- ------------------------------------------------------------------------
    -- SILVER SESSION: declared grains are hard invariants.
    -- ------------------------------------------------------------------------
    IF v_stageName='SILVER_SESSION' THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH checks AS (
            SELECT
                'prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily' AS objectName,
                'duplicateSessionKeys' AS checkName,
                coalesce(sum(cnt-1),0) AS duplicateRows
            FROM (
                SELECT sessionId,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerSession_daily
                WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
                GROUP BY sessionId
                HAVING count(*)>1
            )

            UNION ALL

            SELECT
                'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily',
                'duplicateSessionPageCategoryKeys',
                coalesce(sum(cnt-1),0)
            FROM (
                SELECT sessionId,pageCategory,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerSessionPageCategory_daily
                WHERE sessionStartDatePst BETWEEN v_windowStart AND v_windowEnd
                GROUP BY sessionId,pageCategory
                HAVING count(*)>1
            )
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','SILVER_SESSION',objectName,checkName),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','SILVER_SESSION','SILVER',objectName,
            'sessionStartDatePst',v_windowStart,v_windowEnd,NULL,NULL,NULL,NULL,NULL,NULL,NULL,
            checkName,'UNIQUENESS','DUPLICATE_KEY',
            0D,cast(duplicateRows AS DOUBLE),cast(duplicateRows AS DOUBLE),NULL,
            CASE WHEN duplicateRows=0 THEN 'HEALTHY' ELSE 'FAILED' END,
            CASE WHEN duplicateRows=0 THEN 'INFO' ELSE 'CRITICAL' END,
            CASE WHEN duplicateRows=0 THEN 'PROCEED' ELSE 'STOP' END,
            CASE WHEN duplicateRows=0 THEN FALSE ELSE TRUE END,
            NULL,NULL,
            CASE WHEN duplicateRows=0 THEN 'Declared Silver session grain is unique.' ELSE 'Duplicate rows violate the declared Silver session grain.' END,
            CASE WHEN duplicateRows>0 THEN 'The Silver transformation emitted more than one row for its declared key.' END,
            CASE WHEN duplicateRows>0 THEN 'Correct the Silver aggregation/join grain before visitor-week processing.' END,
            'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM checks;
    END IF;

    -- ------------------------------------------------------------------------
    -- SILVER WEEK: key pairing and order split reconciliation are hard.
    -- ------------------------------------------------------------------------
    IF v_stageName='SILVER_WEEK' THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH k AS (
            SELECT
                (
                    SELECT count(*) FROM (
                        SELECT weekStartDate,visitorId
                        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
                        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                        EXCEPT
                        SELECT weekStartDate,visitorId
                        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
                        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                    )
                )
                +
                (
                    SELECT count(*) FROM (
                        SELECT weekStartDate,visitorId
                        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
                        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                        EXCEPT
                        SELECT weekStartDate,visitorId
                        FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_attributesPerVisitorWeek_weekly
                        WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                    )
                ) AS mismatchedKeys
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','SILVER_WEEK','visitorWeekPairMismatch'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','SILVER_WEEK','SILVER',
            'attributesPerVisitorWeek + actionsPerVisitorWeek',
            'weekStartDate',v_weekFrom,v_weekTo,v_weekTo,NULL,NULL,NULL,NULL,NULL,NULL,
            'visitorWeekPairMismatch','RECONCILIATION','RECONCILIATION',
            0D,cast(mismatchedKeys AS DOUBLE),cast(mismatchedKeys AS DOUBLE),NULL,
            CASE WHEN mismatchedKeys=0 THEN 'HEALTHY' ELSE 'FAILED' END,
            CASE WHEN mismatchedKeys=0 THEN 'INFO' ELSE 'CRITICAL' END,
            CASE WHEN mismatchedKeys=0 THEN 'PROCEED' ELSE 'STOP' END,
            CASE WHEN mismatchedKeys=0 THEN FALSE ELSE TRUE END,
            NULL,NULL,
            CASE WHEN mismatchedKeys=0 THEN 'Visitor-week attribute/action keys reconcile 1:1.' ELSE 'Visitor-week attribute/action keys do not reconcile.' END,
            CASE WHEN mismatchedKeys>0 THEN 'One weekly Silver object contains visitor-week keys missing from the other.' END,
            CASE WHEN mismatchedKeys>0 THEN 'Correct weekly Silver key coverage before analytical Gold.' END,
            'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM k;

        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH s AS (
            SELECT coalesce(sum(CASE
                WHEN orders<>ordersAcquisition+ordersBase
                  OR orders<>ordersUnassisted+ordersAssisted
                THEN 1 ELSE 0 END),0) AS violations
            FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','SILVER_WEEK','visitorWeekOrderSplitViolations'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','SILVER_WEEK','SILVER',
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_actionsPerVisitorWeek_weekly',
            'weekStartDate',v_weekFrom,v_weekTo,v_weekTo,NULL,NULL,NULL,NULL,NULL,NULL,
            'visitorWeekOrderSplitViolations','RECONCILIATION','RECONCILIATION',
            0D,cast(violations AS DOUBLE),cast(violations AS DOUBLE),NULL,
            CASE WHEN violations=0 THEN 'HEALTHY' ELSE 'FAILED' END,
            CASE WHEN violations=0 THEN 'INFO' ELSE 'CRITICAL' END,
            CASE WHEN violations=0 THEN 'PROCEED' ELSE 'STOP' END,
            CASE WHEN violations=0 THEN FALSE ELSE TRUE END,
            NULL,NULL,
            CASE WHEN violations=0 THEN 'Visitor-week order splits reconcile.' ELSE 'Visitor-week order split violations detected.' END,
            CASE WHEN violations>0 THEN 'Acquisition/base or assisted/unassisted classifications do not sum to orders.' END,
            CASE WHEN violations>0 THEN 'Correct the weekly action derivations before analytical Gold.' END,
            'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM s;
    END IF;

    -- ------------------------------------------------------------------------
    -- Analytical Gold: duplicate declared keys are hard failures.
    -- ------------------------------------------------------------------------
    IF v_stageName IN ('GOLD_OVERVIEW','GOLD_BREAKOUT','GOLD_CROSSTAB','GOLD_EXPLORE') THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH d AS (
            SELECT
                'GOLD_OVERVIEW' AS stageName,
                'prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long' AS objectName,
                coalesce(sum(cnt-1),0) AS duplicateRows
            FROM (
                SELECT targetWeekStartDate,filterLob,filterPlatform,metricName,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
                WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                GROUP BY targetWeekStartDate,filterLob,filterPlatform,metricName
                HAVING count(*)>1
            )
            HAVING v_stageName='GOLD_OVERVIEW'

            UNION ALL

            SELECT
                'GOLD_BREAKOUT',
                'prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long',
                coalesce(sum(cnt-1),0)
            FROM (
                SELECT targetWeekStartDate,filterLob,filterPlatform,breakoutType,breakoutValue,metricName,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
                WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                GROUP BY targetWeekStartDate,filterLob,filterPlatform,breakoutType,breakoutValue,metricName
                HAVING count(*)>1
            )
            HAVING v_stageName='GOLD_BREAKOUT'

            UNION ALL

            SELECT
                'GOLD_CROSSTAB',
                'prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long',
                coalesce(sum(cnt-1),0)
            FROM (
                SELECT targetWeekStartDate,filterLob,filterPlatform,pairKey,rowBreakoutValue,columnBreakoutValue,metricName,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
                WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
                GROUP BY targetWeekStartDate,filterLob,filterPlatform,pairKey,rowBreakoutValue,columnBreakoutValue,metricName
                HAVING count(*)>1
            )
            HAVING v_stageName='GOLD_CROSSTAB'

            UNION ALL

            SELECT
                'GOLD_EXPLORE',
                'prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide',
                coalesce(sum(cnt-1),0)
            FROM (
                SELECT weekStartDate,sessionId,pageCategory,count(*) AS cnt
                FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
                WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
                GROUP BY weekStartDate,sessionId,pageCategory
                HAVING count(*)>1
            )
            HAVING v_stageName='GOLD_EXPLORE'
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST',stageName,objectName,'duplicateGoldKeys'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST',stageName,'GOLD',objectName,
            'weekStartDate',v_weekFrom,v_weekTo,v_weekTo,NULL,NULL,NULL,NULL,NULL,NULL,
            'duplicateGoldKeys','UNIQUENESS','DUPLICATE_KEY',
            0D,cast(duplicateRows AS DOUBLE),cast(duplicateRows AS DOUBLE),NULL,
            CASE WHEN duplicateRows=0 THEN 'HEALTHY' ELSE 'FAILED' END,
            CASE WHEN duplicateRows=0 THEN 'INFO' ELSE 'CRITICAL' END,
            CASE WHEN duplicateRows=0 THEN 'PROCEED' ELSE 'STOP' END,
            CASE WHEN duplicateRows=0 THEN FALSE ELSE TRUE END,
            NULL,NULL,
            CASE WHEN duplicateRows=0 THEN 'Analytical Gold declared grain is unique.' ELSE 'Duplicate analytical Gold keys detected.' END,
            CASE WHEN duplicateRows>0 THEN 'The Gold procedure emitted more than one row for its declared grain.' END,
            CASE WHEN duplicateRows>0 THEN 'Correct the Gold grain before building downstream objects.' END,
            'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM d;
    END IF;

    -- ------------------------------------------------------------------------
    -- GOLD APP: all 11 objects use a uniform object-level monitoring contract.
    -- Availability/freshness/comparator gaps are never hard failures here.
    -- ------------------------------------------------------------------------
    IF v_stageName='GOLD_APP' THEN
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH overviewAvailability AS (
            SELECT
                targetWeekStartDate,
                sum(CASE WHEN thisWeekDataAvailable THEN 1 ELSE 0 END) AS currentRows,
                sum(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END) AS priorRows,
                sum(CASE WHEN fourWeekTrendWeekCount>0 THEN 1 ELSE 0 END) AS fourWeekRows,
                max(coalesce(fourWeekTrendWeekCount,0)) AS maxFourWeekCount,
                sum(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END) AS lastYearRows,
                max(goldProcessedAt) AS sourceTs
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_overviewMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            GROUP BY targetWeekStartDate
        ),
        breakoutAvailability AS (
            SELECT
                targetWeekStartDate,
                sum(CASE WHEN thisWeekDataAvailable THEN 1 ELSE 0 END) AS currentRows,
                sum(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END) AS priorRows,
                sum(CASE WHEN fourWeekTrendWeekCount>0 THEN 1 ELSE 0 END) AS fourWeekRows,
                max(coalesce(fourWeekTrendWeekCount,0)) AS maxFourWeekCount,
                sum(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END) AS lastYearRows,
                max(goldProcessedAt) AS sourceTs
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            GROUP BY targetWeekStartDate
        ),
        crosstabAvailability AS (
            SELECT
                targetWeekStartDate,
                sum(CASE WHEN thisWeekDataAvailable THEN 1 ELSE 0 END) AS currentRows,
                sum(CASE WHEN priorWeekDataAvailable THEN 1 ELSE 0 END) AS priorRows,
                sum(CASE WHEN fourWeekTrendWeekCount>0 THEN 1 ELSE 0 END) AS fourWeekRows,
                max(coalesce(fourWeekTrendWeekCount,0)) AS maxFourWeekCount,
                sum(CASE WHEN sameWeekLyDataAvailable THEN 1 ELSE 0 END) AS lastYearRows,
                max(goldProcessedAt) AS sourceTs
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_crosstabMetricIngredientsByWeek_long
            WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            GROUP BY targetWeekStartDate
        ),
        exploreAvailability AS (
            SELECT
                weekStartDate AS targetWeekStartDate,
                count(*) AS currentRows,
                0 AS priorRows,
                0 AS fourWeekRows,
                0 AS maxFourWeekCount,
                0 AS lastYearRows,
                max(goldProcessedAt) AS sourceTs
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_exploreSessionPageCategoryByWeek_wide
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
            GROUP BY weekStartDate
        ),
        sourceAvailability AS (
            SELECT targetWeekStartDate,'OVERVIEW' AS sourceType,currentRows,priorRows,fourWeekRows,maxFourWeekCount,lastYearRows,sourceTs FROM overviewAvailability
            UNION ALL
            SELECT targetWeekStartDate,'BREAKOUT',currentRows,priorRows,fourWeekRows,maxFourWeekCount,lastYearRows,sourceTs FROM breakoutAvailability
            UNION ALL
            SELECT targetWeekStartDate,'CROSSTAB',currentRows,priorRows,fourWeekRows,maxFourWeekCount,lastYearRows,sourceTs FROM crosstabAvailability
            UNION ALL
            SELECT targetWeekStartDate,'EXPLORE',currentRows,priorRows,fourWeekRows,maxFourWeekCount,lastYearRows,sourceTs FROM exploreAvailability
        ),
        config AS (
            SELECT * FROM VALUES
                ('sdi_tbl_mip_gold_appOverviewCards_wide',            'OVERVIEW','CURRENT','WIDE'),
                ('sdi_tbl_mip_gold_appOverviewTrend_long',            'OVERVIEW','CURRENT','ALWAYS_THREE'),
                ('sdi_tbl_mip_gold_appOverviewToplineMovers_long',    'BREAKOUT','COMPARATOR','AVAILABLE_ONLY'),
                ('sdi_tbl_mip_gold_appOverviewConversionFunnel_long', 'OVERVIEW','CURRENT','ALWAYS_THREE'),
                ('sdi_tbl_mip_gold_appBreakoutsComparisonTable_long', 'BREAKOUT','CURRENT','ALWAYS_THREE'),
                ('sdi_tbl_mip_gold_appBreakoutsWaterfall_long',       'BREAKOUT','COMPARATOR','AVAILABLE_ONLY'),
                ('sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long',   'BREAKOUT','COMPARATOR','AVAILABLE_ONLY'),
                ('sdi_tbl_mip_gold_appCrosstabsMatrix_long',          'CROSSTAB','CURRENT','ALWAYS_THREE'),
                ('sdi_tbl_mip_gold_appCrosstabsRankedPairs_long',     'CROSSTAB','COMPARATOR','AVAILABLE_ONLY'),
                ('sdi_tbl_mip_gold_appExploreBase_wide',              'EXPLORE','CURRENT','NONE'),
                ('sdi_tbl_mip_gold_appExploreRankedPairs_long',       'CROSSTAB','COMPARATOR','AVAILABLE_ONLY')
            AS t(objectName,sourceType,populationMode,comparatorMode)
        ),
        weeks AS (
            SELECT weekStartDate AS targetWeekStartDate
            FROM prdrzranalytics.lab42.sdi_vw_mip_control_fiscalCalendar_static
            WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        appStats AS (
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewCards_wide' AS objectName,count(*) AS rowCount,max(goldProcessedAt) AS goldTs,max(appProcessedAt) AS appTs
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewTrend_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewToplineMovers_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewConversionFunnel_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsComparisonTable_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsWaterfall_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appCrosstabsMatrix_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appCrosstabsRankedPairs_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate
            UNION ALL
            SELECT weekStartDate,'sdi_tbl_mip_gold_appExploreBase_wide',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreBase_wide WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY weekStartDate
            UNION ALL
            SELECT weekStartDate,'sdi_tbl_mip_gold_appExploreRankedPairs_long',count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY weekStartDate
        ),
        comparatorRows AS (
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewTrend_long' AS objectName,comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewToplineMovers_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewConversionFunnel_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsComparisonTable_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsWaterfall_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appCrosstabsMatrix_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appCrosstabsRankedPairs_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo
            UNION ALL SELECT weekStartDate,'sdi_tbl_mip_gold_appExploreRankedPairs_long',comparisonType FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo
        ),
        comparatorStats AS (
            SELECT
                targetWeekStartDate,objectName,
                max(CASE WHEN comparisonType='priorWeek' THEN 1 ELSE 0 END) AS hasPrior,
                max(CASE WHEN comparisonType='fourWeek' THEN 1 ELSE 0 END) AS hasFourWeek,
                max(CASE WHEN comparisonType='lastYear' THEN 1 ELSE 0 END) AS hasLastYear,
                sum(CASE WHEN comparisonType IS NULL OR comparisonType NOT IN ('priorWeek','fourWeek','lastYear') THEN 1 ELSE 0 END) AS unexpectedRows
            FROM comparatorRows
            GROUP BY targetWeekStartDate,objectName
        ),
        joined AS (
            SELECT
                w.targetWeekStartDate,c.objectName,c.sourceType,c.populationMode,c.comparatorMode,
                coalesce(s.currentRows,0) AS currentRows,coalesce(s.priorRows,0) AS priorRows,
                coalesce(s.fourWeekRows,0) AS fourWeekRows,coalesce(s.maxFourWeekCount,0) AS maxFourWeekCount,
                coalesce(s.lastYearRows,0) AS lastYearRows,s.sourceTs,
                coalesce(a.rowCount,0) AS rowCount,a.goldTs,a.appTs,
                coalesce(cs.hasPrior,0) AS hasPrior,coalesce(cs.hasFourWeek,0) AS hasFourWeek,
                coalesce(cs.hasLastYear,0) AS hasLastYear,coalesce(cs.unexpectedRows,0) AS unexpectedRows
            FROM weeks w
            CROSS JOIN config c
            LEFT JOIN sourceAvailability s ON s.targetWeekStartDate=w.targetWeekStartDate AND s.sourceType=c.sourceType
            LEFT JOIN appStats a ON a.targetWeekStartDate=w.targetWeekStartDate AND a.objectName=c.objectName
            LEFT JOIN comparatorStats cs ON cs.targetWeekStartDate=w.targetWeekStartDate AND cs.objectName=c.objectName
        ),
        derived AS (
            SELECT
                *,
                CASE
                    WHEN currentRows=0 THEN FALSE
                    WHEN populationMode='CURRENT' THEN TRUE
                    WHEN priorRows>0 OR fourWeekRows>0 OR lastYearRows>0 THEN TRUE
                    ELSE FALSE
                END AS expectedRows,
                CASE
                    WHEN comparatorMode='ALWAYS_THREE' THEN 3
                    WHEN comparatorMode='AVAILABLE_ONLY' THEN
                        (CASE WHEN priorRows>0 THEN 1 ELSE 0 END)
                        +(CASE WHEN fourWeekRows>0 THEN 1 ELSE 0 END)
                        +(CASE WHEN lastYearRows>0 THEN 1 ELSE 0 END)
                    ELSE 0
                END AS expectedComparatorCount,
                hasPrior+hasFourWeek+hasLastYear AS actualComparatorCount
            FROM joined
        ),
        checks AS (
            SELECT
                targetWeekStartDate,objectName,sourceTs,appTs,
                'appPopulation' AS checkName,'VOLUME' AS checkType,'POPULATION' AS issueType,
                CASE WHEN expectedRows THEN 1D ELSE 0D END AS expectedValue,
                cast(rowCount AS DOUBLE) AS actualValue,
                CASE
                    WHEN currentRows=0 THEN 'WARNING'
                    WHEN NOT expectedRows AND rowCount=0 THEN 'INFO'
                    WHEN expectedRows AND rowCount=0 THEN 'WARNING'
                    WHEN NOT expectedRows AND rowCount>0 THEN 'WARNING'
                    ELSE 'HEALTHY'
                END AS checkStatus,
                CASE
                    WHEN currentRows=0 THEN 'MEDIUM'
                    WHEN expectedRows AND rowCount=0 THEN 'HIGH'
                    WHEN NOT expectedRows AND rowCount=0 THEN 'INFO'
                    WHEN NOT expectedRows AND rowCount>0 THEN 'LOW'
                    ELSE 'INFO'
                END AS severity,
                CASE
                    WHEN currentRows=0 THEN 'App source has no current-week data.'
                    WHEN NOT expectedRows AND rowCount=0 THEN 'App table is correctly empty because required comparator history is unavailable.'
                    WHEN expectedRows AND rowCount=0 THEN 'App table is unexpectedly empty even though source coverage is available.'
                    WHEN NOT expectedRows AND rowCount>0 THEN 'App table has rows even though comparator readiness does not expect them.'
                    ELSE 'App population is consistent with analytical source readiness.'
                END AS description,
                CASE
                    WHEN currentRows=0 THEN 'Current analytical Gold is not loaded.'
                    WHEN NOT expectedRows AND rowCount=0 THEN 'No prior-week, four-week or last-year comparison is available.'
                    WHEN expectedRows AND rowCount=0 THEN 'The App transformation produced no rows for available source data.'
                    WHEN NOT expectedRows AND rowCount>0 THEN 'Stale rows may remain from an earlier load.'
                END AS likelyCause,
                CASE
                    WHEN currentRows=0 THEN 'Load/rebuild analytical Gold first.'
                    WHEN expectedRows AND rowCount=0 THEN 'Inspect and rerun the corresponding Gold App procedure.'
                    WHEN NOT expectedRows AND rowCount>0 THEN 'Review stale partitions and comparator readiness.'
                END AS nextSteps

            UNION ALL

            SELECT
                targetWeekStartDate,objectName,sourceTs,appTs,
                'appFreshness','FRESHNESS','FRESHNESS',
                0D,
                CASE WHEN rowCount=0 OR appTs IS NULL OR sourceTs IS NULL THEN NULL
                     ELSE cast(unix_timestamp(appTs)-unix_timestamp(sourceTs) AS DOUBLE) END,
                CASE
                    WHEN rowCount=0 THEN 'INFO'
                    WHEN appTs IS NULL THEN 'WARNING'
                    WHEN sourceTs IS NOT NULL AND appTs<sourceTs THEN 'WARNING'
                    ELSE 'HEALTHY'
                END,
                CASE
                    WHEN rowCount=0 THEN 'INFO'
                    WHEN appTs IS NULL THEN 'HIGH'
                    WHEN sourceTs IS NOT NULL AND appTs<sourceTs THEN 'MEDIUM'
                    ELSE 'INFO'
                END,
                CASE
                    WHEN rowCount=0 THEN 'Freshness is not applicable because the App object is empty for this week.'
                    WHEN appTs IS NULL THEN 'App rows exist but appProcessedAt is NULL.'
                    WHEN sourceTs IS NOT NULL AND appTs<sourceTs THEN 'App data is older than analytical Gold.'
                    ELSE 'App data is current with analytical Gold.'
                END,
                CASE
                    WHEN appTs IS NULL AND rowCount>0 THEN 'appProcessedAt was not populated.'
                    WHEN sourceTs IS NOT NULL AND appTs<sourceTs THEN 'Analytical Gold was rebuilt after the App object.'
                END,
                CASE
                    WHEN appTs IS NULL AND rowCount>0 THEN 'Inspect appProcessedAt assignment.'
                    WHEN sourceTs IS NOT NULL AND appTs<sourceTs THEN 'Rerun the corresponding App procedure.'
                END

            UNION ALL

            SELECT
                targetWeekStartDate,objectName,sourceTs,appTs,
                'appComparatorCoverage','COVERAGE','COMPARATOR_AVAILABILITY',
                cast(expectedComparatorCount AS DOUBLE),
                cast(actualComparatorCount AS DOUBLE),
                CASE
                    WHEN comparatorMode IN ('NONE','WIDE') THEN 'INFO'
                    WHEN comparatorMode='AVAILABLE_ONLY' AND expectedComparatorCount=0 AND actualComparatorCount=0 THEN 'INFO'
                    WHEN expectedComparatorCount<>actualComparatorCount OR unexpectedRows>0 THEN 'WARNING'
                    ELSE 'HEALTHY'
                END,
                CASE
                    WHEN comparatorMode IN ('NONE','WIDE') THEN 'INFO'
                    WHEN comparatorMode='AVAILABLE_ONLY' AND expectedComparatorCount=0 AND actualComparatorCount=0 THEN 'INFO'
                    WHEN expectedComparatorCount<>actualComparatorCount OR unexpectedRows>0 THEN 'HIGH'
                    ELSE 'INFO'
                END,
                CASE
                    WHEN comparatorMode IN ('NONE','WIDE') THEN 'Comparator-long coverage is not applicable to this App object.'
                    WHEN comparatorMode='AVAILABLE_ONLY' AND expectedComparatorCount=0 AND actualComparatorCount=0 THEN 'No comparator rows are expected because analytical comparison history is unavailable.'
                    WHEN expectedComparatorCount<>actualComparatorCount THEN 'App comparator coverage does not match analytical comparator availability.'
                    WHEN unexpectedRows>0 THEN 'Unexpected comparisonType values exist in the App object.'
                    ELSE 'App comparator coverage matches its contract.'
                END,
                CASE
                    WHEN expectedComparatorCount<>actualComparatorCount THEN 'Available comparison ingredients and App comparison rows are out of sync.'
                    WHEN unexpectedRows>0 THEN 'Unsupported comparisonType values were produced.'
                END,
                CASE
                    WHEN expectedComparatorCount<>actualComparatorCount OR unexpectedRows>0 THEN 'Compare App comparisonType rows to analytical Gold availability flags and rerun the App procedure if needed.'
                END
            FROM derived
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','GOLD_APP',cast(targetWeekStartDate AS STRING),objectName,checkName),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','GOLD_APP','GOLD_APP',
            concat('prdrzranalytics.lab42.',objectName),
            'weekStartDate',targetWeekStartDate,targetWeekStartDate,targetWeekStartDate,
            NULL,NULL,NULL,NULL,NULL,NULL,
            checkName,checkType,issueType,
            expectedValue,actualValue,actualValue-expectedValue,NULL,
            checkStatus,severity,'PROCEED',FALSE,sourceTs,appTs,
            description,likelyCause,nextSteps,'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM checks;

        -- Compact metric-level App facts for future drill-down views.
        INSERT INTO prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WITH metricRows AS (
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewCards_wide' AS objectName,metricName,NULL AS comparisonType,NULL AS breakoutType,NULL AS pairKey,NULL AS displaySize,count(*) AS rowCount,max(goldProcessedAt) AS goldTs,max(appProcessedAt) AS appTs
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewCards_wide WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewTrend_long',metricName,comparisonType,NULL,NULL,NULL,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewTrend_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewToplineMovers_long',metricName,comparisonType,breakoutType,NULL,NULL,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewToplineMovers_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType,breakoutType
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appOverviewConversionFunnel_long',metricName,comparisonType,NULL,NULL,NULL,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appOverviewConversionFunnel_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsComparisonTable_long',metricName,comparisonType,breakoutType,NULL,displaySize,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsComparisonTable_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType,breakoutType,displaySize
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsWaterfall_long',metricName,comparisonType,breakoutType,NULL,displaySize,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsWaterfall_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType,breakoutType,displaySize
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long',metricName,comparisonType,breakoutType,NULL,displaySize,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appBreakoutsAbsoluteTrend_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType,breakoutType,displaySize
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appCrosstabsMatrix_long',metricName,comparisonType,NULL,pairKey,displaySize,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsMatrix_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType,pairKey,displaySize
            UNION ALL
            SELECT targetWeekStartDate,'sdi_tbl_mip_gold_appCrosstabsRankedPairs_long',metricName,comparisonType,NULL,pairKey,NULL,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appCrosstabsRankedPairs_long WHERE targetWeekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY targetWeekStartDate,metricName,comparisonType,pairKey
            UNION ALL
            SELECT weekStartDate,'sdi_tbl_mip_gold_appExploreRankedPairs_long',metricName,comparisonType,NULL,pairKey,NULL,count(*),max(goldProcessedAt),max(appProcessedAt)
            FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_appExploreRankedPairs_long WHERE weekStartDate BETWEEN v_weekFrom AND v_weekTo GROUP BY weekStartDate,metricName,comparisonType,pairKey
        )
        SELECT
            concat('VAL_',upper(substr(sha2(concat_ws('|',v_runId,'POST','GOLD_APP',cast(targetWeekStartDate AS STRING),objectName,coalesce(metricName,''),coalesce(comparisonType,''),coalesce(breakoutType,''),coalesce(pairKey,''),coalesce(displaySize,''),'appMetricRows'),256),1,24))),
            v_runId,v_checkedAt,v_asOfDate,'POST','GOLD_APP','GOLD_APP',
            concat('prdrzranalytics.lab42.',objectName),
            'weekStartDate',targetWeekStartDate,targetWeekStartDate,targetWeekStartDate,
            metricName,comparisonType,breakoutType,NULL,pairKey,displaySize,
            'appMetricRows','VOLUME','DATA_AVAILABILITY',
            1D,cast(rowCount AS DOUBLE),cast(rowCount-1 AS DOUBLE),NULL,
            'HEALTHY','INFO','PROCEED',FALSE,goldTs,appTs,
            'App metric/dimension combination contains rows.',
            NULL,NULL,'MIP Data Engineering',NULL,NULL,NULL,NULL
        FROM metricRows;
    END IF;

    SET v_stopCount = (
        SELECT count(*)
        FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
        WHERE runId=v_runId
          AND validationPhase='POST'
          AND stageName=v_stageName
          AND gateAction='STOP'
    );

    SELECT
        v_runId AS runId,
        v_stageName AS stageName,
        'POST' AS validationPhase,
        count(*) AS checkCount,
        sum(CASE WHEN checkStatus='INFO' THEN 1 ELSE 0 END) AS infoCount,
        sum(CASE WHEN checkStatus='WARNING' THEN 1 ELSE 0 END) AS warningCount,
        sum(CASE WHEN checkStatus IN ('FAILED','ERROR') THEN 1 ELSE 0 END) AS failedOrErrorCount,
        sum(CASE WHEN gateAction='STOP' THEN 1 ELSE 0 END) AS stopCount,
        CASE WHEN v_stopCount>0 THEN 'STOP' ELSE 'PROCEED' END AS gateAction
    FROM prdrzranalytics.lab42.sdi_tbl_mip_validation_checkHistory_perRun
    WHERE runId=v_runId
      AND validationPhase='POST'
      AND stageName=v_stageName;

    IF v_stopCount>0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'MIP_POST_VALIDATION_STOP: severe POST-stage validation failed. Review sdi_tbl_mip_validation_checkHistory_perRun.';
    END IF;
END;
