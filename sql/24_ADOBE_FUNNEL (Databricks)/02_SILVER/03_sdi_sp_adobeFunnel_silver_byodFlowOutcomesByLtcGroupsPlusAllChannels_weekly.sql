/* =================================================================================================
FILE:         03_sdi_sp_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly.sql
LAYER:        Silver Table (via Stored Procedure)
CATALOG.SCHEMA: prdrzranalytics.lab42
TABLE:        sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly
PROCEDURE:    sdi_sp_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly

SOURCES:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByAllChannel_weekly
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly

PURPOSE:
  Silver table for BYOD funnel outcome metrics by ChannelGroup plus ALL.
  Combines Bronze byodFlowOutcomesByAllChannel (ALL row) and Bronze
  byodFlowOutcomesByLtcGroups (6 channel group rows) via UNION ALL.
  One row per WeekSunSat + ChannelGroup (7 rows/week total).

  Outcome metrics track what BYOD visitors did after entering the site:
    ByodVrChatVisitors      — engaged via virtual/chat support
    ByodCallVisitors        — engaged via phone call
    ByodStoreLocatorVisitors — used store locator
    ByodInternalTmoVisitors — identified as internal T-Mobile traffic
    ByodBouncersVisitors    — bounced without meaningful engagement
    ByodOrders              — completed a BYOD order

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      ChannelGroup

BUSINESS RULES:
  - ALL row comes from Bronze byodFlowOutcomesByAllChannel (ChannelGroup = 'All Channels')
  - Channel group rows come from Bronze byodFlowOutcomesByLtcGroups (ChannelGroup IN
    PAID SEARCH, ORGANIC SEARCH, DIRECT, PROGRAMMATIC, SOCIAL, OTHER)
  - ReportingGrain is fixed as 'CHANNEL_GROUP' for all rows
  - No aggregation or derivation applied — metrics passed through as-is from Bronze
  - NULLs preserved — no fake zeroes

DOWNSTREAM:
  Gold: sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly
  USING DELTA
  AS

  SELECT
    WeekSunSat,
    'CHANNEL_GROUP'          AS ReportingGrain,
    ChannelGroup,
    ByodVrChatVisitors,
    ByodCallVisitors,
    ByodStoreLocatorVisitors,
    ByodInternalTmoVisitors,
    ByodBouncersVisitors,
    ByodOrders
  FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByAllChannel_weekly

  UNION ALL

  SELECT
    WeekSunSat,
    'CHANNEL_GROUP'          AS ReportingGrain,
    ChannelGroup,
    ByodVrChatVisitors,
    ByodCallVisitors,
    ByodStoreLocatorVisitors,
    ByodInternalTmoVisitors,
    ByodBouncersVisitors,
    ByodOrders
  FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly;

END;