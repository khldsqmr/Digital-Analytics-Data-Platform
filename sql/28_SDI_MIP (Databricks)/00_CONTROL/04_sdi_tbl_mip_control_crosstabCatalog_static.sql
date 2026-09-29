-- ============================================================================
-- FILE  : 04_sdi_tbl_mip_control_crosstabCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Supported prebuilt crosstab dimension pairs.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

CREATE TABLE IF NOT EXISTS sdi_tbl_mip_control_crosstabCatalog_static (
    pairKey             STRING,
    rowBreakoutType     STRING,
    columnBreakoutType  STRING,
    pairLabel           STRING,
    isActive            BOOLEAN,
    sortOrder           INT,
    notes               STRING
)
USING DELTA
COMMENT 'Control: supported prebuilt crosstab pairs; pair values come from data.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_control_crosstabCatalog_static()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Refreshes the static MIP crosstab catalog.'
AS
BEGIN
    INSERT OVERWRITE TABLE prdrzranalytics.lab42.sdi_tbl_mip_control_crosstabCatalog_static (
        pairKey,
        rowBreakoutType,
        columnBreakoutType,
        pairLabel,
        isActive,
        sortOrder,
        notes
    )
    VALUES
        ('channel__entryPage',     'channel',     'entryPage',    'Channel × Entry page',         true, 10, ''),
        ('authState__entryPage',   'authState',   'entryPage',    'Visitor type × Entry page',    true, 20, ''),
        ('utmSource__channel',     'utmSource',   'channel',      'UTM source × Channel',          true, 30, ''),
        ('utmMedium__authState',   'utmMedium',   'authState',    'UTM medium × Visitor type',     true, 40, ''),
        ('buyFlowStep__authState', 'buyFlowStep', 'authState',    'Buy flow step × Visitor type',  true, 50, ''),
        ('utmCampaign__device',    'utmCampaign', 'device',       'UTM campaign × Device',         true, 60, ''),
        ('device__entryPage',      'device',      'entryPage',    'Device × Entry page',           true, 70, ''),
        ('channel__device',        'channel',     'device',       'Channel × Device',              true, 80, '');
END;

-- Optional initial load:
-- CALL sdi_sp_mip_control_crosstabCatalog_static();
