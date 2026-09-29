
-- ###########################################################################
-- BEGIN bronze/04_sdi_tbl_mip_bronze_edlMarketingCodes_snapshot.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 04_sdi_tbl_mip_bronze_edlMarketingCodes_snapshot.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Small full snapshot of dim_marketing_code.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Marketing-code reference snapshot
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
COMMENT 'Bronze/reference snapshot of dim_marketing_code. Small full overwrite.'
AS
SELECT
  MKT_CODE,
  MKT_CODE_NAME,
  Category,
  is_active,
  cast(NULL AS STRING) AS _runId,
  current_timestamp() AS _ingestedAt
FROM prdrzranalytics.lab42.dim_marketing_code
WHERE 1 = 0;

CREATE OR REPLACE PROCEDURE sdi_sp_mip_bronze_edlMarketingCodes_snapshot(
  p_runId STRING
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.dim_marketing_code
    LIMIT 1
  ) THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'dim_marketing_code is empty; Bronze snapshot was not overwritten.';
  END IF;

  INSERT OVERWRITE sdi_tbl_mip_bronze_edlMarketingCodes_snapshot
  SELECT
    MKT_CODE,
    MKT_CODE_NAME,
    Category,
    is_active,
    p_runId AS _runId,
    current_timestamp() AS _ingestedAt
  FROM prdrzranalytics.lab42.dim_marketing_code;
END;


-- ###########################################################################
-- END bronze/04_sdi_tbl_mip_bronze_edlMarketingCodes_snapshot.sql
-- ###########################################################################

