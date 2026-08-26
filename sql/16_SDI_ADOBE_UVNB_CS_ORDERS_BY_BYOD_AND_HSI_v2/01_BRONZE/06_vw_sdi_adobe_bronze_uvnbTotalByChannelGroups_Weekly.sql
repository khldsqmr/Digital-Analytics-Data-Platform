/* =================================================================================================
FILE: 06_vw_sdi_adobe_bronze_uvnbTotalByChannelGroups_Weekly.sql
LAYER: Bronze View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_adobe_bronze_uvnbTotalByChannelGroups_Weekly

SOURCES:
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo
  prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannelGroups_Weekly

PURPOSE:
  Canonical Bronze weekly Adobe total UVNB source at LTC_GROUPS granularity.

BUSINESS GRAIN:
  One row per:
      WeekSunSat
      LtcGroup

BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as LTC_GROUPS.
  - LtcGroup is derived from the raw table family.
  - UvnbTotalAdobe is sourced directly from group-level uvnb_visitors raw tables.
  - UvnbTotalAdobe is not calculated from UvnbPostpaid + UvnbHsi + UvnbByod.
  - UvnbTotalAdobe is not derived from detailed LastTouchChannel.
  - Missing values remain NULL.

KEY DEDUPE RULE:
  - Deduplicate each source table at weekly + LtcGroup grain using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC

================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobe_bronze_uvnbTotalByChannelGroups_Weekly`
AS

WITH RawUnion AS (

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'DIRECT' AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'ORGANIC SEARCH' AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'OTHER' AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'PAID SEARCH' AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'PROGRAMMATIC' AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo`

  UNION ALL

  SELECT
    DATE_ADD(PARSE_DATE('%Y%m%d', date_yyyymmdd), INTERVAL 6 DAY) AS WeekSunSat,
    'LTC_GROUPS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    'SOCIAL' AS LtcGroup,
    SAFE_CAST(visitors AS FLOAT64) AS UvnbTotalAdobe,
    'sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo`
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
  UvnbTotalAdobe,
  SourceTable AS SourceTablesUsed,
  FileLoadDatetime AS MaxFileLoadDatetime,
  Filename AS FilenamesUsed
FROM Deduped;