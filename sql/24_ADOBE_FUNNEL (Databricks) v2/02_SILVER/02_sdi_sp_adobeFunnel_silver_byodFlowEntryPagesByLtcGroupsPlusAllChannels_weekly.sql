/* =================================================================================================
FILE:         02_sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly.sql
LAYER:        Silver Table (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly
PROCEDURE:    sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly

SOURCES:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByAllChannels_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly

PURPOSE:
  Silver table for BYOD funnel entry page metrics by ChannelGroup plus ALL.
  Combines Bronze byodFlowEntryPagesByAllChannel (ALL row) and Bronze
  byodFlowEntryPagesByLtcGroups (6 channel group rows) via UNION ALL.
  One row per WeekSunSat + ChannelGroup (7 rows/week total).

  Entry page metrics track where BYOD visitors entered the site:
    ByodUpvVisitors                  — total unique non-bounced BYOD visitors
    ByodEntryByodPageVisitors        — entered on BYOD page
    ByodEntryHomePageVisitors        — entered on homepage
    ByodEntryDevicePageVisitors      — entered on device page
    ByodEntryPlansPageVisitors       — entered on plans page
    ByodEntryOtherPageVisitors       — entered on other page (otherpage2 only — true residual)
    ByodEntryStorePageVisitors       — entered on store page
    ByodEntryByodLandingPageVisitors — entered on BYOD landing page
    ByodEntryOffersSwitchVisitors    — entered on offers/switch page

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL row comes from Bronze byodFlowEntryPagesByAllChannel (ChannelGroup = 'All Channels')
  - Channel group rows come from Bronze byodFlowEntryPagesByLtcGroups (ChannelGroup IN
    PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER)
  - ReportingGrain is fixed as 'CHANNEL_GROUP' for all rows
  - No aggregation or derivation applied — metrics passed through as-is from Bronze
  - NULLs preserved — no fake zeroes

DOWNSTREAM:
  Gold: sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
  Pulse Silver 07: vw_sdi_pulseByod_silver_adobeByodEntryPages_weekly (separate pipeline, not yet ported)
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly
  USING DELTA
  AS

  SELECT
    WeekSunSat,
    'CHANNEL_GROUP'                AS ReportingGrain,
    ChannelGroup,
    ByodUpvVisitors,
    ByodEntryByodPageVisitors,
    ByodEntryHomePageVisitors,
    ByodEntryDevicePageVisitors,
    ByodEntryPlansPageVisitors,
    ByodEntryOtherPageVisitors,        -- otherpage2 only (true residual)
    ByodEntryStorePageVisitors,
    ByodEntryByodLandingPageVisitors,
    ByodEntryOffersSwitchVisitors
  FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByAllChannels_weekly

  UNION ALL

  SELECT
    WeekSunSat,
    'CHANNEL_GROUP'                AS ReportingGrain,
    ChannelGroup,
    ByodUpvVisitors,
    ByodEntryByodPageVisitors,
    ByodEntryHomePageVisitors,
    ByodEntryDevicePageVisitors,
    ByodEntryPlansPageVisitors,
    ByodEntryOtherPageVisitors,        -- otherpage2 only (true residual)
    ByodEntryStorePageVisitors,
    ByodEntryByodLandingPageVisitors,
    ByodEntryOffersSwitchVisitors
  FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly;

END;