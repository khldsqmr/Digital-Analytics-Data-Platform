-- ============================================================================
-- FILE  : 03_sdi_vw_mip_control_breakoutCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Breakout availability, labels, Top-N settings and App Gold eligibility.
--
-- REGION CONTRACT:
--   The key remains 'region' for downstream compatibility, but the current
--   implementation is intentionally a TEMPORARY mixed geography placeholder:
--     Web = geo_postal_code
--     App = attribute_country
--   Session Silver prefers hitNumberInSession=1 and otherwise earliest non-null.
--   Visitor/week Silver attributes the value by NBV-session page views.
--   This is NOT a normalized geographic region or T-Mobile internal region.
-- ============================================================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static
COMMENT 'Control view: breakout availability, labels, Top-N limits and App Gold eligibility.'
AS
SELECT *
FROM VALUES
    ('channel','Channel',CAST(NULL AS INT),25,true,true,'final',true,10,
     'channel_name; weekly serving value is one attributed value'),

    ('authState','Visitor type',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,20,
     'user_auth_state; strongest weekly state'),

    ('prospectVsBase','Prospect vs Base',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,30,
     'Customer/Care/Prospect kept distinct upstream'),

    ('entryPage','Entry page',100,25,true,true,'final',true,40,
     'SESSION_SUMMARY_FACT entry_page_url_path'),

    ('pageCategory','Page category',100,25,true,true,'final',true,50,
     'site_sub_section'),

    ('device','Device',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,60,
     'Derived in Silver from source_table + page_app_type + page_layout_state + attribute_os_name; Web app-like page_app_type values are App Web View'),

    ('region','Region',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'temporary',true,70,
     'Temporary mixed geography placeholder: Web=geo_postal_code; App=attribute_country; session uses first-hit/earliest non-null; visitor/week weighted by NBV-session page views; not normalized geographic/T-Mobile region'),

    ('utmSource','UTM source',100,25,true,true,'final',true,80,
     'Parsed from SESSION_SUMMARY_FACT entry_page_url_full'),

    ('utmMedium','UTM medium',100,25,true,true,'final',true,90,
     'Parsed from SESSION_SUMMARY_FACT entry_page_url_full'),

    ('utmCampaign','UTM campaign',100,25,true,true,'final',true,100,
     'Parsed from SESSION_SUMMARY_FACT entry_page_url_full'),

    ('buyFlowStep','Buy flow step',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,110,
     'Mapped from site_sub_section'),

    ('campaign','Campaign',100,25,true,true,'final',true,120,
     '4th token of external_campaign_code + marketing code name'),

    ('lob','LOB',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,130,
     'Session keeps lobList; weekly dashboard uses one attributed LOB'),

    ('platform','Platform',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,140,
     'page_app_type business/property breakout; separate from device')
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

