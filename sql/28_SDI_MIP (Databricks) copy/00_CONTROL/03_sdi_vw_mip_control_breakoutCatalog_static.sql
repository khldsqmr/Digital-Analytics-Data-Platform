-- ============================================================================
-- FILE  : 03_sdi_vw_mip_control_breakoutCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE: Breakout availability, labels, Top-N settings and App Gold eligibility.
-- ============================================================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_control_breakoutCatalog_static
COMMENT 'Control view: breakout availability, labels, Top-N limits and App Gold eligibility.'
AS
SELECT *
FROM VALUES
    ('channel','Channel',CAST(NULL AS INT),25,true,true,'final',true,10,'channel_name; weekly serving value is one attributed value'),
    ('authState','Visitor type',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,20,'user_auth_state; strongest weekly state'),
    ('prospectVsBase','Prospect vs Base',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,30,'Customer/Care/Prospect kept distinct upstream'),
    ('entryPage','Entry page',100,25,true,true,'final',true,40,'SESSION_SUMMARY_FACT entry_page_url_path'),
    ('pageCategory','Page category',100,25,true,true,'final',true,50,'site_sub_section'),
    ('device','Device',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,60,'Derived from source_table + page_app_type + device_type + device_operating_system; app-like page_app_type values on t_web_interactions are classified as App Web View'),
    ('region','Region',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,70,'UDI geo_region; IP-derived state/province resolved from session-entry geography and attributed to visitor/week downstream'),
    ('utmSource','UTM source',100,25,true,true,'final',true,80,'Parsed from SESSION_SUMMARY_FACT entry_page_url_full'),
    ('utmMedium','UTM medium',100,25,true,true,'final',true,90,'Parsed from SESSION_SUMMARY_FACT entry_page_url_full'),
    ('utmCampaign','UTM campaign',100,25,true,true,'final',true,100,'Parsed from SESSION_SUMMARY_FACT entry_page_url_full'),
    ('buyFlowStep','Buy flow step',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,110,'Mapped from site_sub_section'),
    ('campaign','Campaign',100,25,true,true,'final',true,120,'4th token of external_campaign_code + marketing code name'),
    ('lob','LOB',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,130,'Session keeps lobList; weekly dashboard uses one attributed LOB'),
    ('platform','Platform',CAST(NULL AS INT),CAST(NULL AS INT),true,true,'final',true,140,'page_app_type business/property breakout; separate from device')
AS t(breakoutType,breakoutLabel,topN,pairTopN,isPrebuiltBreakout,isExploreDimension,definitionStatus,isActive,sortOrder,notes);
