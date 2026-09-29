
-- ###########################################################################
-- BEGIN control/03_sdi_tbl_mip_control_breakoutCatalog_static.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 03_sdi_tbl_mip_control_breakoutCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Breakout availability and Top-N settings; actual category values come from data.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Breakout catalog
-- Actual category values come from the data.
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_control_breakoutCatalog_static (
  breakoutType       STRING,
  breakoutLabel      STRING,
  topN               INT COMMENT 'NULL = keep all values',
  pairTopN           INT COMMENT 'NULL = keep all values in crosstabs',
  isPrebuiltBreakout BOOLEAN,
  isExploreDimension BOOLEAN,
  definitionStatus   STRING COMMENT 'final | proposed | placeholder',
  isActive           BOOLEAN,
  sortOrder          INT,
  notes              STRING
)
USING DELTA
COMMENT 'Control: breakout availability, labels and Top-N limits. Category values come from data.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_control_breakoutCatalog_static()
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  INSERT OVERWRITE sdi_tbl_mip_control_breakoutCatalog_static
  SELECT * FROM VALUES
    ('channel',        'Channel',        NULL, 25,  true, true, 'final',       true, 10, 'channel_name; weekly serving value is one attributed value'),
    ('authState',      'Visitor type',   NULL, NULL,true, true, 'final',       true, 20, 'user_auth_state; strongest weekly state'),
    ('prospectVsBase', 'Prospect vs Base',NULL,NULL,true,true, 'final',        true, 30, 'Customer/Care/Prospect kept distinct upstream'),
    ('entryPage',      'Entry page',     100, 25,  true, true, 'final',       true, 40, 'SSF entry_page_url_path'),
    ('pageCategory',   'Page category',  100, 25,  true, true, 'final',       true, 50, 'site_sub_section'),
    ('device',         'Device',         NULL, NULL,true, true, 'proposed',    true, 60, 'Temporary implementation from page_layout_state / OS; confirm definition'),
    ('region',         'Region',         NULL, NULL,false,false,'placeholder', false,70, 'Pending geo solution'),
    ('utmSource',      'UTM source',     100, 25,  true, true, 'final',       true, 80, 'Parsed from SSF entry_page_url_full'),
    ('utmMedium',      'UTM medium',     100, 25,  true, true, 'final',       true, 90, 'Parsed from SSF entry_page_url_full'),
    ('utmCampaign',    'UTM campaign',   100, 25,  true, true, 'final',       true,100, 'Parsed from SSF entry_page_url_full'),
    ('buyFlowStep',    'Buy flow step',  NULL, NULL,true, true, 'final',       true,110, 'Mapped from site_sub_section'),
    ('campaign',       'Campaign',       100, 25,  true, true, 'final',       true,120, '4th token of external_campaign_code + marketing code name'),
    ('lob',            'LOB',            NULL, NULL,true, true, 'final',       true,130, 'Session keeps lobList; weekly dashboard uses one attributed LOB'),
    ('platform',       'Platform',       NULL, NULL,true, true, 'final',       true,140, 'page_app_type')
  AS t(
    breakoutType, breakoutLabel, topN, pairTopN,
    isPrebuiltBreakout, isExploreDimension,
    definitionStatus, isActive, sortOrder, notes
  );
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END control/03_sdi_tbl_mip_control_breakoutCatalog_static.sql
-- ###########################################################################

