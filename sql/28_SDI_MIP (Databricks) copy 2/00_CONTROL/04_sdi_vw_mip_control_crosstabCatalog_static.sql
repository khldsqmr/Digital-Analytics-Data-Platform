-- ============================================================================
-- FILE  : 04_sdi_vw_mip_control_crosstabCatalog_static.sql
-- LAYER : CONTROL
-- PURPOSE:
--   Crosstab pair universe used by analytical Gold and Crosstabs App Gold.
--
-- REGION CONTRACT:
--   Region pairs keep their existing pairKey/orientation for downstream
--   compatibility. Values follow the temporary mixed geography contract:
--     Web = geo_postal_code
--     App = attribute_country
--   Region pairs stay active analytical pairs but are not shortcut/prebuilt
--   pairs unless explicitly promoted later.
-- ============================================================================
CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_mip_control_crosstabCatalog_static
COMMENT 'Control view: active Crosstab pair universe. isPrebuiltPair identifies shortcut/prebuilt Crosstab pairs.'
AS
SELECT *
FROM VALUES
    ('channel__authState','channel','authState','Channel × Visitor type',true,false,10,''),
    ('channel__entryPage','channel','entryPage','Channel × Entry page',true,true,20,''),
    ('channel__pageCategory','channel','pageCategory','Channel × Page category',true,false,30,''),
    ('channel__device','channel','device','Channel × Device',true,true,40,''),
    ('utmSource__channel','utmSource','channel','UTM source × Channel',true,true,50,''),
    ('channel__utmMedium','channel','utmMedium','Channel × UTM medium',true,false,60,''),
    ('channel__utmCampaign','channel','utmCampaign','Channel × UTM campaign',true,false,70,''),
    ('channel__buyFlowStep','channel','buyFlowStep','Channel × Buy flow step',true,false,80,''),

    ('authState__entryPage','authState','entryPage','Visitor type × Entry page',true,true,90,''),
    ('authState__pageCategory','authState','pageCategory','Visitor type × Page category',true,false,100,''),
    ('authState__device','authState','device','Visitor type × Device',true,false,110,''),
    ('authState__utmSource','authState','utmSource','Visitor type × UTM source',true,false,120,''),
    ('utmMedium__authState','utmMedium','authState','UTM medium × Visitor type',true,true,130,''),
    ('authState__utmCampaign','authState','utmCampaign','Visitor type × UTM campaign',true,false,140,''),
    ('buyFlowStep__authState','buyFlowStep','authState','Buy flow step × Visitor type',true,true,150,''),

    ('entryPage__pageCategory','entryPage','pageCategory','Entry page × Page category',true,false,160,''),
    ('device__entryPage','device','entryPage','Device × Entry page',true,true,170,''),
    ('entryPage__utmSource','entryPage','utmSource','Entry page × UTM source',true,false,180,''),
    ('entryPage__utmMedium','entryPage','utmMedium','Entry page × UTM medium',true,false,190,''),
    ('entryPage__utmCampaign','entryPage','utmCampaign','Entry page × UTM campaign',true,false,200,''),
    ('entryPage__buyFlowStep','entryPage','buyFlowStep','Entry page × Buy flow step',true,false,210,''),

    ('pageCategory__device','pageCategory','device','Page category × Device',true,false,220,''),
    ('pageCategory__utmSource','pageCategory','utmSource','Page category × UTM source',true,false,230,''),
    ('pageCategory__utmMedium','pageCategory','utmMedium','Page category × UTM medium',true,false,240,''),
    ('pageCategory__utmCampaign','pageCategory','utmCampaign','Page category × UTM campaign',true,false,250,''),
    ('pageCategory__buyFlowStep','pageCategory','buyFlowStep','Page category × Buy flow step',true,false,260,''),

    ('device__utmSource','device','utmSource','Device × UTM source',true,false,270,''),
    ('device__utmMedium','device','utmMedium','Device × UTM medium',true,false,280,''),
    ('utmCampaign__device','utmCampaign','device','UTM campaign × Device',true,true,290,''),
    ('device__buyFlowStep','device','buyFlowStep','Device × Buy flow step',true,false,300,''),

    ('utmSource__utmMedium','utmSource','utmMedium','UTM source × UTM medium',true,false,310,''),
    ('utmSource__utmCampaign','utmSource','utmCampaign','UTM source × UTM campaign',true,false,320,''),
    ('utmSource__buyFlowStep','utmSource','buyFlowStep','UTM source × Buy flow step',true,false,330,''),
    ('utmMedium__utmCampaign','utmMedium','utmCampaign','UTM medium × UTM campaign',true,false,340,''),
    ('utmMedium__buyFlowStep','utmMedium','buyFlowStep','UTM medium × Buy flow step',true,false,350,''),
    ('utmCampaign__buyFlowStep','utmCampaign','buyFlowStep','UTM campaign × Buy flow step',true,false,360,''),

    ('channel__region','channel','region','Channel × Region',true,false,370,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('authState__region','authState','region','Visitor type × Region',true,false,380,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('entryPage__region','entryPage','region','Entry page × Region',true,false,390,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('pageCategory__region','pageCategory','region','Page category × Region',true,false,400,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('device__region','device','region','Device × Region',true,false,410,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('utmSource__region','utmSource','region','UTM source × Region',true,false,420,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('utmMedium__region','utmMedium','region','UTM medium × Region',true,false,430,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('utmCampaign__region','utmCampaign','region','UTM campaign × Region',true,false,440,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country'),
    ('region__buyFlowStep','region','buyFlowStep','Region × Buy flow step',true,false,450,
     'Region uses temporary mixed geography: Web=geo_postal_code; App=attribute_country')
AS t(
    pairKey,
    rowBreakoutType,
    columnBreakoutType,
    pairLabel,
    isActive,
    isPrebuiltPair,
    sortOrder,
    notes
);

