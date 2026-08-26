/* =================================================================================================
FILE:         04_vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly.sql
LAYER:        Silver View
DATASET:      prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW:         vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByAll_Weekly
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByLtcGroups_Weekly

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly

PURPOSE:
  Silver view for BYOD funnel outcome metrics by ChannelGroup plus ALL.
  Combines Bronze 09 (ALL row) and Bronze 10 (6 channel group rows) via UNION ALL.
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
  - ALL row comes from Bronze 09 (ChannelGroup = 'ALL')
  - Channel group rows come from Bronze 10 (ChannelGroup IN PAID SEARCH, ORGANIC SEARCH,
    DIRECT, PROGRAMMATIC, SOCIAL, OTHER)
  - ReportingGrain is fixed as 'CHANNEL_GROUP' for all rows
  - No aggregation or derivation applied — metrics passed through as-is from Bronze
  - NULLs preserved — no fake zeroes

DOWNSTREAM:
  Gold 01: vw_sdi_adobe_gold_flowPerformanceByChannelGroups_weekly
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_silver_byodFlowOutcomesByChannelGroupsPlusAll_Weekly`
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
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByAll_Weekly`

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
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_byodFlowOutcomesByLtcGroups_Weekly`;