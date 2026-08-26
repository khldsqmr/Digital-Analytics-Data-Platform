/* =================================================================================================
FILE:         03_vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW:         vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByLtcGroups_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly

PURPOSE:
  Silver view for BYOD funnel entry page metrics by ChannelGroup plus ALL.
  Combines Bronze 07 (ALL row) and Bronze 08 (6 channel group rows) via UNION ALL.
  One row per WeekSunSat + ChannelGroup (7 rows/week total).

  Entry page metrics track where BYOD visitors entered the site:
    ByodUvnbVisitors                 — total unique non-bounced BYOD visitors
    ByodEntryByodPageVisitors        — entered on BYOD page
    ByodEntryHomePageVisitors        — entered on homepage
    ByodEntryDevicePageVisitors      — entered on device page
    ByodEntryPlansPageVisitors       — entered on plans page
    ByodEntryOtherPageVisitors       — entered on other page (NEW: otherpage2 only — true residual)
    ByodEntryStorePageVisitors       — entered on store page (NEW)
    ByodEntryByodLandingPageVisitors — entered on BYOD landing page (NEW)
    ByodEntryOffersSwitchVisitors    — entered on offers/switch page (NEW)

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL row comes from Bronze 07 (ChannelGroup = 'ALL')
  - Channel group rows come from Bronze 08 (ChannelGroup IN PAID SEARCH, ORGANIC SEARCH,
    DIRECT, PROGRAMMATIC, SOCIAL, OTHER)
  - ReportingGrain is fixed as 'CHANNEL_GROUP' for all rows
  - No aggregation or derivation applied — metrics passed through as-is from Bronze
  - NULLs preserved — no fake zeroes

CHANGES:
  2026-06-XX — Other segregation:
               ByodEntryOtherPageVisitors now = otherpage2 (true residual) — passed through from Bronze
               Added ByodEntryStorePageVisitors, ByodEntryByodLandingPageVisitors,
               ByodEntryOffersSwitchVisitors — passed through from Bronze

DOWNSTREAM:
  Gold 01: vw_sdi_adobe_gold_flowPerformanceByChannelGroups_weekly
  Pulse Silver 07: vw_sdi_pulseByod_silver_adobeByodEntryPages_weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowEntryPagesByChannelGroupsPlusAll_Weekly`
AS

SELECT
  WeekSunSat,
  'CHANNEL_GROUP'                AS ReportingGrain,
  ChannelGroup,
  ByodUvnbVisitors,
  ByodEntryByodPageVisitors,
  ByodEntryHomePageVisitors,
  ByodEntryDevicePageVisitors,
  ByodEntryPlansPageVisitors,
  ByodEntryOtherPageVisitors,        -- NEW: otherpage2 only (true residual)
  ByodEntryStorePageVisitors,        -- NEW
  ByodEntryByodLandingPageVisitors,  -- NEW
  ByodEntryOffersSwitchVisitors      -- NEW
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByAll_Weekly`

UNION ALL

SELECT
  WeekSunSat,
  'CHANNEL_GROUP'                AS ReportingGrain,
  ChannelGroup,
  ByodUvnbVisitors,
  ByodEntryByodPageVisitors,
  ByodEntryHomePageVisitors,
  ByodEntryDevicePageVisitors,
  ByodEntryPlansPageVisitors,
  ByodEntryOtherPageVisitors,        -- NEW: otherpage2 only (true residual)
  ByodEntryStorePageVisitors,        -- NEW
  ByodEntryByodLandingPageVisitors,  -- NEW
  ByodEntryOffersSwitchVisitors      -- NEW
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowEntryPagesByLtcGroups_Weekly`;