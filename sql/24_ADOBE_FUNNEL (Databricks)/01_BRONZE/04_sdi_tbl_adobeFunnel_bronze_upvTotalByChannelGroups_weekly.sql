/* =================================================================================================
FILE: 04_sdi_tbl_adobeFunnel_bronze_upvTotalByChannelGroups_weekly.sql
LAYER: Bronze Table (via Stored Procedure)
DATASET: prdrzranalytics.lab42
TABLE: sdi_tbl_adobeFunnel_bronze_upvTotalByChannelGroups_weekly
PROCEDURE: sdi_sp_adobeFunnel_bronze_upvTotalByChannelGroups_weekly

SOURCES:
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo

DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByChannelGroups_weekly

PURPOSE:
  Canonical Bronze weekly Adobe total UPV source at LTC_GROUPS granularity.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LtcGroup

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as LTC_GROUPS.
  - LtcGroup is derived from the raw table family.
  - UpvTotalAdobe is sourced directly from group-level uvnb_visitors raw tables.
  - UpvTotalAdobe is not calculated from UpvPostpaid + UpvHsi + UpvByod.
  - UpvTotalAdobe is not derived from detailed LastTouchChannel.
  - Missing values remain NULL.

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly + LtcGroup grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC

================================================================================================= */

CREATE OR REPLACE PROCEDURE
`prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByChannelGroups_weekly`()
LANGUAGE SQL
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  `prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByChannelGroups_weekly`
  AS

WITH RawUnion AS (

  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'DIRECT' AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'ORGANIC SEARCH' AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'OTHER' AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'PAID SEARCH' AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'PROGRAMMATIC' AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'SOCIAL' AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo`
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
      LtcGroup,
      SourceTable
    ORDER BY
      FileLoadDatetime DESC,
      Filename DESC,
      InsertDate DESC
  ) = 1
)

SELECT
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup,
  UpvTotalAdobe,
  SourceTable AS SourceTablesUsed,
  FileLoadDatetime AS MaxFileLoadDatetime,
  Filename AS FilenamesUsed
FROM Deduped;

END;