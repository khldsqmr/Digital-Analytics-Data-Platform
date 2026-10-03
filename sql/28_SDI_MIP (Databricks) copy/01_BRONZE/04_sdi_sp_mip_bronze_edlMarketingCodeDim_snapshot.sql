-- ============================================================================
-- FILE  : 04_sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot.sql
-- LAYER : BRONZE
-- SOURCE: prdrzranalytics.lab42.dim_marketing_code
-- PURPOSE:
--   Persist the small MIP-required marketing-code reference snapshot.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze/reference MIP snapshot of dim_marketing_code: code, name, category and active flag.'
AS
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM prdrzranalytics.lab42.dim_marketing_code LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='dim_marketing_code is empty. Bronze was not created or overwritten.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            'prdrzranalytics.lab42.dim_marketing_code' AS sourceObject,
            'No Bronze table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot
        USING DELTA
        COMMENT 'Bronze/reference snapshot of MIP-required dim_marketing_code fields.'
        AS
        SELECT
            MKT_CODE,
            MKT_CODE_NAME,
            Category,
            is_active,
            current_timestamp() AS _ingestedAt
        FROM prdrzranalytics.lab42.dim_marketing_code
        WHERE 1=0;
        INSERT OVERWRITE TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot
        SELECT
            MKT_CODE,
            MKT_CODE_NAME,
            Category,
            is_active,
            current_timestamp() AS _ingestedAt
        FROM prdrzranalytics.lab42.dim_marketing_code;
        SELECT
            'SUCCESS' AS status,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot' AS targetObject;
    END IF;
END;
-- ============================================================================
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(p_validateOnly=>TRUE);
-- B. EXECUTE
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodeDim_snapshot(p_validateOnly=>FALSE);
-- C. VALIDATION
-- SELECT COUNT(*) AS rows,COUNT(DISTINCT MKT_CODE) AS distinctCodes
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot;
