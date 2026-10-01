-- ============================================================================
-- FILE  : 03_sdi_vw_mip_control_breakoutCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Breakout availability and Top-N settings.
--   Actual category values come from data.
--
-- APP RULE:
--   Breakouts App Gold only consumes rows where:
--     isActive = TRUE
--     AND isPrebuiltBreakout = TRUE
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

CREATE OR REPLACE VIEW sdi_vw_mip_control_breakoutCatalog_static
COMMENT 'Control view: breakout availability, labels, Top-N limits and App Gold eligibility.'
AS

SELECT *
FROM VALUES
    ('channel',        'Channel',          CAST(NULL AS INT),25,                true,true, 'final',       true, 10, 'channel_name; weekly serving value is one attributed value'),
    ('authState',      'Visitor type',     CAST(NULL AS INT),CAST(NULL AS INT), true,true, 'final',       true, 20, 'user_auth_state; strongest weekly state'),
    ('prospectVsBase', 'Prospect vs Base', CAST(NULL AS INT),CAST(NULL AS INT), true,true, 'final',       true, 30, 'Customer/Care/Prospect kept distinct upstream'),
    ('entryPage',      'Entry page',       100,              25,                true,true, 'final',       true, 40, 'SSF entry_page_url_path'),
    ('pageCategory',   'Page category',    100,              25,                true,true, 'final',       true, 50, 'site_sub_section'),
    ('device',         'Device',           CAST(NULL AS INT),CAST(NULL AS INT), true,true, 'proposed',    true, 60, 'Temporary implementation from page_layout_state / OS; confirm definition'),

    -- Region intentionally excluded from App Gold until geo is implemented.
    ('region',         'Region',           CAST(NULL AS INT),CAST(NULL AS INT), false,false,'placeholder',false,70, 'Pending geo solution'),

    ('utmSource',      'UTM source',       100,              25,                true,true, 'final',       true, 80, 'Parsed from SSF entry_page_url_full'),
    ('utmMedium',      'UTM medium',       100,              25,                true,true, 'final',       true, 90, 'Parsed from SSF entry_page_url_full'),
    ('utmCampaign',    'UTM campaign',     100,              25,                true,true, 'final',       true,100, 'Parsed from SSF entry_page_url_full'),
    ('buyFlowStep',    'Buy flow step',    CAST(NULL AS INT),CAST(NULL AS INT), true,true, 'final',       true,110, 'Mapped from site_sub_section'),
    ('campaign',       'Campaign',         100,              25,                true,true, 'final',       true,120, '4th token of external_campaign_code + marketing code name'),
    ('lob',            'LOB',              CAST(NULL AS INT),CAST(NULL AS INT), true,true, 'final',       true,130, 'Session keeps lobList; weekly dashboard uses one attributed LOB'),
    ('platform',       'Platform',         CAST(NULL AS INT),CAST(NULL AS INT), true,true, 'final',       true,140, 'page_app_type')

AS t(
    breakoutType,
    breakoutLabel,
    topN,
    pairTopN,
    isPrebuiltBreakout,
    isExploreDimension,
    definitionStatus,
    isActive,
    sortOrder,
    notes
);