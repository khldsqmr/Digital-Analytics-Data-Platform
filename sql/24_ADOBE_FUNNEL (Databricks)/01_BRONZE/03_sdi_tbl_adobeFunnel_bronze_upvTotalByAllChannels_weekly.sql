/* =================================================================================================
FILE: 03_sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly.sql
LAYER: Bronze Table (via Stored Procedure)
DATASET: prdrzranalytics.lab42
TABLE: sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly
PROCEDURE: sdi_sp_adobeFunnel_bronze_upvTotalByAllChannels_weekly
SOURCE:
  prd_dbi_analytics.improvado.sdi_raw_pp_pro_uvnb_weekly_tmo
DESTINATION:
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly
PURPOSE:
  Canonical Bronze weekly Adobe total UPV source at ALL_CHANNELS granularity.
BUSINESS GRAIN:
  One row per:
      WeekSunSat
BUSINESS RULES:
  - WeekSunSat is derived from date_yyyymmdd by adding 6 days because raw date_yyyymmdd is week-starting Sunday.
  - DataGranularity is fixed as ALL_CHANNELS.
  - UpvTotalAdobe is sourced directly from the total UPV raw table.
  - UpvTotalAdobe is not calculated from UpvPostpaid + UpvHsi + UpvByod.
  - Missing values remain NULL.
KEY DEDUPE RULE:
  - Deduplicate using latest:
      File_Load_datetime DESC
      Filename DESC
      __insert_date DESC
================================================================================================= */
CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByAllChannels_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_upvTotalByAllChannels_weekly
  USING DELTA
  AS
WITH RawBase AS (
  SELECT
    DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat,
    'ALL_CHANNELS' AS DataGranularity,
    CAST(NULL AS STRING) AS LastTouchChannel,
    CAST(NULL AS STRING) AS LtcGroup,
    TRY_CAST(visitors AS DOUBLE) AS UpvTotalAdobe,
    'sdi_raw_pp_pro_uvnb_weekly_tmo' AS SourceTable,
    __insert_date AS InsertDate,
    File_Load_datetime AS FileLoadDatetime,
    Filename
  FROM prd_dbi_analytics.improvado.sdi_raw_pp_pro_uvnb_weekly_tmo
),
Deduped AS (
  SELECT *
  FROM RawBase
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
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