-- ============================================================================
-- FILE  : 04_sdi_vw_mip_control_crosstabCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Supported prebuilt crosstab dimension pairs.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

CREATE OR REPLACE VIEW sdi_vw_mip_control_crosstabCatalog_static
COMMENT 'Control view: supported prebuilt crosstab pairs; pair values come from data.'
AS

SELECT *
FROM VALUES
    ('channel__entryPage',     'channel',     'entryPage', 'Channel × Entry page',        true, 10, ''),
    ('authState__entryPage',   'authState',   'entryPage', 'Visitor type × Entry page',   true, 20, ''),
    ('utmSource__channel',     'utmSource',   'channel',   'UTM source × Channel',         true, 30, ''),
    ('utmMedium__authState',   'utmMedium',   'authState', 'UTM medium × Visitor type',    true, 40, ''),
    ('buyFlowStep__authState', 'buyFlowStep', 'authState', 'Buy flow step × Visitor type', true, 50, ''),
    ('utmCampaign__device',    'utmCampaign', 'device',    'UTM campaign × Device',        true, 60, ''),
    ('device__entryPage',      'device',      'entryPage', 'Device × Entry page',          true, 70, ''),
    ('channel__device',        'channel',     'device',    'Channel × Device',             true, 80, '')

AS t(
    pairKey,
    rowBreakoutType,
    columnBreakoutType,
    pairLabel,
    isActive,
    sortOrder,
    notes
);
