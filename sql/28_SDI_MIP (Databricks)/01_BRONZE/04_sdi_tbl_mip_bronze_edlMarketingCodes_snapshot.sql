-- ============================================================================
-- FILE  : 04_sdi_sp_mip_bronze_edlMarketingCodes_snapshot.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Persist a small full snapshot of dim_marketing_code.
--
-- DESIGN:
--   - One top-level SQL statement per file.
--   - No required run/job ID during development.
--   - Validates the source before creating or overwriting the Bronze target.
--   - p_validateOnly = TRUE performs preflight only; no table is created/written.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze/reference full snapshot of dim_marketing_code. Validates source availability before overwrite.'
AS
BEGIN
    -- ------------------------------------------------------------------------
    -- 1. Source preflight
    -- ------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.dim_marketing_code
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'dim_marketing_code is empty. Bronze was not created or overwritten.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 2. Validation-only mode
    -- ------------------------------------------------------------------------
    IF p_validateOnly THEN

        SELECT
            'VALIDATION_ONLY' AS status,
            'prdrzranalytics.lab42.dim_marketing_code' AS sourceObject,
            'No Bronze table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 3. Create target only after preflight passes.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
        USING DELTA
        COMMENT 'Bronze/reference snapshot of dim_marketing_code. Small full overwrite.'
        AS
        SELECT
            MKT_CODE,
            MKT_CODE_NAME,
            Category,
            is_active,
            current_timestamp() AS _ingestedAt

        FROM prdrzranalytics.lab42.dim_marketing_code
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 4. Small reference table: full overwrite is intentional.
        -- --------------------------------------------------------------------
        INSERT OVERWRITE TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
        SELECT
            MKT_CODE,
            MKT_CODE_NAME,
            Category,
            is_active,
            current_timestamp() AS _ingestedAt

        FROM prdrzranalytics.lab42.dim_marketing_code;

        SELECT
            'SUCCESS' AS status,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodes_snapshot' AS targetObject;

    END IF;
END;

-- Development examples (run separately after deploying the procedure):
--
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
--     p_validateOnly => TRUE
-- );
--
-- Refresh the full reference snapshot:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlMarketingCodes_snapshot();
