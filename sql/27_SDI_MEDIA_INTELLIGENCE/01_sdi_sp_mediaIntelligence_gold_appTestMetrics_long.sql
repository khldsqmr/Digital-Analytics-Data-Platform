/* =================================================================================================
FILE:         sdi_sp_mediaIntelligence_gold_appTestMetrics_long.sql
PLATFORM:     Databricks
LAYER:        Gold
PROCEDURE:    sdi_sp_mediaIntelligence_gold_appTestMetrics_long
TARGET TABLE: sdi_tbl_mediaIntelligence_gold_appTestMetrics_long

PURPOSE:
  Creates / refreshes the Media Intelligence application test Gold Long table.

  This test dataset reuses the existing PulseTMS Silver architecture and exposes
  only the sources and dimensions required for the initial application / API test.

CURRENT DATA SOURCES:

  1. ADOBE

     Source:
       prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly

     Includes:
       - UPV
       - Cart Start
       - Orders
       - Adobe CVR
       - WoW
       - YoY
       - Existing period metadata

     LOB:
       Postpaid + Broadband

  2. MFC_SPEND_CHANNEL

     Source:
       prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly

     Includes:
       - Actual spend only
       - Channel grain only
       - POSTPAID only
       - BROADBAND only
       - WoW
       - YoY
       - Existing period metadata

     LOB normalization:
       CONSUMER POSTPAID / POSTPAID -> POSTPAID
       HSI / BROADBAND              -> BROADBAND

INTENTIONALLY EXCLUDED:

  - QGP_SCORECARD
  - MFC_SPEND_GRANULAR
  - MFC forecast
  - TFB / TBG MFC LOB
  - PLATFORM_SPEND_CHANNEL
  - BIDDABLE_SPEND_CHANNEL
  - UPV_FORECAST

NOT INCLUDED IN THIS TEST SCHEMA:

  - true_lob
  - mfc_channel
  - mfc_tactic
  - mfc_message_type
  - mfc_agency
  - allocation_ratio

OUTPUT SCHEMA:

   1.  data_source
   2.  qgp_date
   3.  week_type
   4.  qgp_quarter
   5.  days_in_period
   6.  is_complete_period
   7.  lob
   8.  channel_group
   9.  metric_name
   10. metric_type
   11. metric_value
   12. metric_value_ly
   13. wow_numerator
   14. wow_denominator
   15. wow_pct
   16. yoy_numerator
   17. yoy_denominator
   18. yoy_pct
   19. max_date
   20. adobe_cvr_value
   21. adobe_cvr_numerator
   22. adobe_cvr_denominator

================================================================================================= */


CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mediaIntelligence_gold_appTestMetrics_long()

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Refreshes Media Intelligence Gold application test Long table using Adobe and channel-level MFC actual spend for Postpaid and Broadband.'

AS

BEGIN


  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mediaIntelligence_gold_appTestMetrics_long

  USING DELTA

  AS


  WITH


  /* =============================================================================================
     CTE 1: ADOBE
     ============================================================================================= */

  AdobeVolume AS (

    SELECT

      /* -----------------------------------------------------------------------------------------
         SOURCE
         ----------------------------------------------------------------------------------------- */

      'ADOBE' AS data_source,


      /* -----------------------------------------------------------------------------------------
         PERIOD
         ----------------------------------------------------------------------------------------- */

      CAST(s.qgp_date AS DATE) AS qgp_date,

      s.week_type,
      s.qgp_quarter,
      s.days_in_period,
      s.is_complete_period,


      /* -----------------------------------------------------------------------------------------
         LOB

         Adobe remains represented as the combined business-facing LOB.
         ----------------------------------------------------------------------------------------- */

      'Postpaid + Broadband' AS lob,


      /* -----------------------------------------------------------------------------------------
         CHANNEL / METRIC
         ----------------------------------------------------------------------------------------- */

      s.channel_group,
      s.metric_name,
      s.metric_type,


      /* -----------------------------------------------------------------------------------------
         VALUES
         ----------------------------------------------------------------------------------------- */

      s.metric_value,
      s.metric_value_ly,


      /* -----------------------------------------------------------------------------------------
         WOW
         ----------------------------------------------------------------------------------------- */

      s.wow_numerator,
      s.wow_denominator,
      s.wow_pct,


      /* -----------------------------------------------------------------------------------------
         YOY
         ----------------------------------------------------------------------------------------- */

      s.yoy_numerator,
      s.yoy_denominator,
      s.yoy_pct,


      /* -----------------------------------------------------------------------------------------
         MAX SOURCE DATE
         ----------------------------------------------------------------------------------------- */

      CAST(s.max_date AS DATE) AS max_date,


      /* -----------------------------------------------------------------------------------------
         ADOBE CVR
         ----------------------------------------------------------------------------------------- */

      s.adobe_cvr_value,
      s.adobe_cvr_numerator,
      s.adobe_cvr_denominator


    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly s


    WHERE
      s.metric_type = 'ADOBE_VOLUME'

  ),



  /* =============================================================================================
     CTE 2: MFC SPEND — CHANNEL GRAIN — ACTUAL ONLY

     INCLUDED:

       data_source:
         MFC_SPEND_CHANNEL

       metric:
         mfcSpendActual

       LOB:
         POSTPAID
         BROADBAND

     EXCLUDED:

       MFC_SPEND_GRANULAR
       mfcSpendForecast
       TFB
       TBG
       all other MFC LOBs
     ============================================================================================= */

  MfcChannelActual AS (

    SELECT

      /* -----------------------------------------------------------------------------------------
         SOURCE
         ----------------------------------------------------------------------------------------- */

      s.data_source,


      /* -----------------------------------------------------------------------------------------
         PERIOD
         ----------------------------------------------------------------------------------------- */

      CAST(s.qgp_date AS DATE) AS qgp_date,

      s.week_type,
      s.qgp_quarter,
      s.days_in_period,
      s.is_complete_period,


      /* -----------------------------------------------------------------------------------------
         LOB

         Only POSTPAID and BROADBAND can enter because the WHERE clause below
         explicitly restricts the source LOB values.
         ----------------------------------------------------------------------------------------- */

      CASE

        WHEN UPPER(TRIM(s.lob_mfc)) IN (
          'CONSUMER POSTPAID',
          'POSTPAID'
        )
          THEN 'POSTPAID'


        WHEN UPPER(TRIM(s.lob_mfc)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'


        ELSE CAST(NULL AS STRING)

      END AS lob,


      /* -----------------------------------------------------------------------------------------
         CHANNEL / METRIC
         ----------------------------------------------------------------------------------------- */

      s.channel_group,

      s.metric_name,


      /* -----------------------------------------------------------------------------------------
         METRIC TYPE

         Only actual spend is allowed into this CTE.
         ----------------------------------------------------------------------------------------- */

      'MFC_SPEND_ACTUAL' AS metric_type,


      /* -----------------------------------------------------------------------------------------
         VALUES
         ----------------------------------------------------------------------------------------- */

      s.metric_value,
      s.metric_value_ly,


      /* -----------------------------------------------------------------------------------------
         WOW
         ----------------------------------------------------------------------------------------- */

      s.wow_numerator,
      s.wow_denominator,
      s.wow_pct,


      /* -----------------------------------------------------------------------------------------
         YOY
         ----------------------------------------------------------------------------------------- */

      s.yoy_numerator,
      s.yoy_denominator,
      s.yoy_pct,


      /* -----------------------------------------------------------------------------------------
         MAX SOURCE DATE
         ----------------------------------------------------------------------------------------- */

      CAST(s.max_date AS DATE) AS max_date,


      /* -----------------------------------------------------------------------------------------
         ADOBE CVR

         Not applicable to MFC.
         ----------------------------------------------------------------------------------------- */

      CAST(NULL AS DOUBLE) AS adobe_cvr_value,
      CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
      CAST(NULL AS DOUBLE) AS adobe_cvr_denominator


    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s


    WHERE
          s.data_source = 'MFC_SPEND_CHANNEL'

      AND s.metric_name = 'mfcSpendActual'

      AND UPPER(TRIM(s.lob_mfc)) IN (
            'CONSUMER POSTPAID',
            'POSTPAID',
            'HSI',
            'BROADBAND'
          )

  ),



  /* =============================================================================================
     FINAL UNIFIED MEDIA INTELLIGENCE TEST DATASET

     Both CTEs return exactly 22 columns in the same positional order.

     CURRENT DATA SOURCES:

       ADOBE
       MFC_SPEND_CHANNEL

     CURRENT LOBS:

       ADOBE:
         Postpaid + Broadband

       MFC:
         POSTPAID
         BROADBAND
     ============================================================================================= */

  UnifiedMediaIntelligence AS (

    SELECT *
    FROM AdobeVolume


    UNION ALL


    SELECT *
    FROM MfcChannelActual

  )


  SELECT *
  FROM UnifiedMediaIntelligence
  ;


END;