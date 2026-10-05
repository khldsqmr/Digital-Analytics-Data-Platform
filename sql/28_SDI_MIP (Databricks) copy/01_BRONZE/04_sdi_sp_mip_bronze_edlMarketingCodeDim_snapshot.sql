-- ============================================================================
-- FILE  : 04_sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot.sql
-- LAYER : BRONZE / REFERENCE
-- SOURCE: prdrzranalytics.lab42.dim_marketing_code
-- PURPOSE: Persist the small MIP-required marketing-code reference as exactly one resolved row per MKT_CODE.
-- RESOLUTION CONTRACT: Preserve the prior Silver 01 duplicate-resolution rule: prefer an active row, then the lexicographically greatest MKT_CODE_NAME. Name/category/active remain together in one STRUCT so a single source row supplies all three values.
-- DEPENDENCY: Independent of dated UDI/SEF/SSF loads. Must exist before Silver 01 campaign enrichment.
-- PHYSICAL DESIGN: No clustering; this is a small reference snapshot and clustering would add maintenance without meaningful pruning benefit.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze/reference MIP marketing-code snapshot resolved to one row per MKT_CODE.'
AS
BEGIN
    DECLARE v_sourceRowCount BIGINT DEFAULT 0;
    DECLARE v_resolvedCodeRowCount BIGINT DEFAULT 0;
    SET v_sourceRowCount=(SELECT COUNT(*) FROM prdrzranalytics.lab42.dim_marketing_code);
    SET v_resolvedCodeRowCount=(
        SELECT COUNT(*)
        FROM (SELECT MKT_CODE FROM prdrzranalytics.lab42.dim_marketing_code GROUP BY MKT_CODE) codes
    );
    IF v_sourceRowCount=0 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='dim_marketing_code is empty. Bronze was not created or overwritten.';
    END IF;
    IF p_validateOnly THEN
        SELECT 'VALIDATION_ONLY' AS status,v_sourceRowCount AS sourceRowCount,v_resolvedCodeRowCount AS resolvedCodeRows,'prdrzranalytics.lab42.dim_marketing_code' AS sourceObject,'No Bronze table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot
        USING DELTA
        COMMENT 'Bronze/reference snapshot of MIP-required dim_marketing_code fields; one resolved row per MKT_CODE.'
        AS
        SELECT MKT_CODE,MKT_CODE_NAME,Category,is_active,current_timestamp() AS _ingestedAt
        FROM prdrzranalytics.lab42.dim_marketing_code
        WHERE 1=0;
        INSERT OVERWRITE TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot
        SELECT
            MKT_CODE,
            chosen.MKT_CODE_NAME AS MKT_CODE_NAME,
            chosen.Category AS Category,
            chosen.is_active AS is_active,
            current_timestamp() AS _ingestedAt
        FROM (
            SELECT
                MKT_CODE,
                max_by(
                    named_struct('MKT_CODE_NAME',MKT_CODE_NAME,'Category',Category,'is_active',is_active),
                    struct(CASE WHEN try_cast(is_active AS BOOLEAN)=TRUE THEN 1 ELSE 0 END,coalesce(cast(MKT_CODE_NAME AS STRING),''))
                ) AS chosen
            FROM prdrzranalytics.lab42.dim_marketing_code
            GROUP BY MKT_CODE
        ) resolved;
        SELECT 'SUCCESS' AS status,v_sourceRowCount AS sourceRows,v_resolvedCodeRowCount AS loadedResolvedCodeRows,'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot' AS targetObject;
    END IF;
END;
-- DEVELOPMENT / TEST EXAMPLES
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(p_validateOnly=>TRUE);
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(p_validateOnly=>FALSE);
-- SELECT COUNT(*) AS rows,COUNT(DISTINCT MKT_CODE) AS distinctCodes FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot;
-- SELECT MKT_CODE,COUNT(*) AS rowCount FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot GROUP BY MKT_CODE HAVING COUNT(*)>1;
