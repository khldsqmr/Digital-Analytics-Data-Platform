/* =================================================================================================
FILE:         sdi_sp_mediaIntelligence_gold_appTestMetrics_wide.sql
PLATFORM:     Databricks
LAYER:        Gold — Wide / Validation
PROCEDURE:    sdi_sp_mediaIntelligence_gold_appTestMetrics_wide
TARGET TABLE: sdi_tbl_mediaIntelligence_gold_appTestMetrics_wide

PURPOSE:
  Creates / refreshes the Media Intelligence application test Gold Wide table.

  Intended primarily for:

    - Validation
    - Sense-checking
    - Application testing
    - API development testing

CURRENT DATA SOURCES:

  ADOBE

    Includes:
      - UPV
      - Cart Start
      - Orders
      - Adobe CVRs

  MFC_SPEND_CHANNEL

    Includes:
      - Actual spend only
      - POSTPAID only
      - BROADBAND only

    Wide output:
      - mfcSpendActualPostpaid
      - mfcSpendActualBroadband
      - mfcSpendActualTotal

MFC TOTAL:

  mfcSpendActualTotal
    = POSTPAID + BROADBAND

INTENTIONALLY EXCLUDED:

  - QGP_SCORECARD
  - MFC_SPEND_GRANULAR
  - MFC forecast
  - TFB / TBG
  - PLATFORM_SPEND_CHANNEL
  - BIDDABLE_SPEND_CHANNEL
  - UPV_FORECAST

GRAIN:

  qgp_date
  x channel_group

PERIOD METADATA:

  Derived from the existing Adobe / MFC Silver outputs.

  No direct QGP Scorecard source is queried.

================================================================================================= */


CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_mediaIntelligence_gold_appTestMetrics_wide()

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Refreshes Media Intelligence Gold application test Wide table using Adobe and channel-level MFC actual spend for Postpaid and Broadband.'

AS

BEGIN


  CREATE OR REPLACE TABLE
    prdrzranalytics.lab42.sdi_tbl_mediaIntelligence_gold_appTestMetrics_wide

  USING DELTA

  AS


  WITH


  /* =============================================================================================
     PERIOD METADATA

     Period metadata is sourced from the same included Silver datasets.

     Adobe receives priority when both sources contain the same qgp_date.

     MFC acts as the fallback.

     No QGP Scorecard dataset is queried.
     ============================================================================================= */

  PeriodMetaAdobe AS (

    SELECT

      CAST(qgp_date AS DATE) AS qgp_date,

      MAX(week_type) AS week_type,
      MAX(qgp_quarter) AS qgp_quarter,
      MAX(days_in_period) AS days_in_period,
      MAX(is_complete_period) AS is_complete_period

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly

    WHERE
      metric_type = 'ADOBE_VOLUME'

    GROUP BY
      qgp_date

  ),



  PeriodMetaMfc AS (

    SELECT

      CAST(qgp_date AS DATE) AS qgp_date,

      MAX(week_type) AS week_type,
      MAX(qgp_quarter) AS qgp_quarter,
      MAX(days_in_period) AS days_in_period,
      MAX(is_complete_period) AS is_complete_period

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly

    WHERE
          data_source = 'MFC_SPEND_CHANNEL'

      AND metric_name = 'mfcSpendActual'

      AND UPPER(TRIM(lob_mfc)) IN (
            'CONSUMER POSTPAID',
            'POSTPAID',
            'HSI',
            'BROADBAND'
          )

    GROUP BY
      qgp_date

  ),



  PeriodDateSpine AS (

    SELECT qgp_date
    FROM PeriodMetaAdobe

    UNION

    SELECT qgp_date
    FROM PeriodMetaMfc

  ),



  PeriodMeta AS (

    SELECT

      spine.qgp_date,

      COALESCE(
        adobe.week_type,
        mfc.week_type
      ) AS week_type,

      COALESCE(
        adobe.qgp_quarter,
        mfc.qgp_quarter
      ) AS qgp_quarter,

      COALESCE(
        adobe.days_in_period,
        mfc.days_in_period
      ) AS days_in_period,

      COALESCE(
        adobe.is_complete_period,
        mfc.is_complete_period
      ) AS is_complete_period


    FROM
      PeriodDateSpine spine


    LEFT JOIN PeriodMetaAdobe adobe

      ON adobe.qgp_date = spine.qgp_date


    LEFT JOIN PeriodMetaMfc mfc

      ON mfc.qgp_date = spine.qgp_date

  ),



  /* =============================================================================================
     ADOBE
     ============================================================================================= */

  Adobe AS (

    SELECT

      CAST(qgp_date AS DATE) AS qgp_date,

      channel_group,


      /* -----------------------------------------------------------------------------------------
         UPV
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'upvPostpaid',
          metric_value,
          NULL
        )
      ) AS upvPostpaid,


      MAX(
        IF(
          metric_name = 'upvHsi',
          metric_value,
          NULL
        )
      ) AS upvHsi,


      MAX(
        IF(
          metric_name = 'upvByod',
          metric_value,
          NULL
        )
      ) AS upvByod,


      MAX(
        IF(
          metric_name = 'upvFlowTotal',
          metric_value,
          NULL
        )
      ) AS upvFlowTotal,


      MAX(
        IF(
          metric_name = 'upvTotalAdobe',
          metric_value,
          NULL
        )
      ) AS upvTotalAdobe,



      /* -----------------------------------------------------------------------------------------
         CART START
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'cartstartPostpaid',
          metric_value,
          NULL
        )
      ) AS cartstartPostpaid,


      MAX(
        IF(
          metric_name = 'cartstartHsi',
          metric_value,
          NULL
        )
      ) AS cartstartHsi,


      MAX(
        IF(
          metric_name = 'cartstartByod',
          metric_value,
          NULL
        )
      ) AS cartstartByod,


      MAX(
        IF(
          metric_name = 'cartstartTotal',
          metric_value,
          NULL
        )
      ) AS cartstartTotal,



      /* -----------------------------------------------------------------------------------------
         ORDERS — UNASSISTED
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'ordersUnassistedPostpaid',
          metric_value,
          NULL
        )
      ) AS ordersUnassistedPostpaid,


      MAX(
        IF(
          metric_name = 'ordersUnassistedHsi',
          metric_value,
          NULL
        )
      ) AS ordersUnassistedHsi,


      MAX(
        IF(
          metric_name = 'ordersUnassistedByod',
          metric_value,
          NULL
        )
      ) AS ordersUnassistedByod,


      MAX(
        IF(
          metric_name = 'ordersUnassistedTotal',
          metric_value,
          NULL
        )
      ) AS ordersUnassistedTotal,



      /* -----------------------------------------------------------------------------------------
         ORDERS — ASSISTED
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'ordersAssistedPostpaid',
          metric_value,
          NULL
        )
      ) AS ordersAssistedPostpaid,


      MAX(
        IF(
          metric_name = 'ordersAssistedHsi',
          metric_value,
          NULL
        )
      ) AS ordersAssistedHsi,


      MAX(
        IF(
          metric_name = 'ordersAssistedByod',
          metric_value,
          NULL
        )
      ) AS ordersAssistedByod,


      MAX(
        IF(
          metric_name = 'ordersAssistedTotal',
          metric_value,
          NULL
        )
      ) AS ordersAssistedTotal,



      /* -----------------------------------------------------------------------------------------
         ORDERS — TOTAL
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'ordersTotal',
          metric_value,
          NULL
        )
      ) AS ordersTotal,



      /* -----------------------------------------------------------------------------------------
         ADOBE CVR — UPV
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'upvFlowTotal',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrUpvFlow,


      MAX(
        IF(
          metric_name = 'upvPostpaid',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrUpvPostpaid,


      MAX(
        IF(
          metric_name = 'upvHsi',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrUpvHsi,


      MAX(
        IF(
          metric_name = 'upvByod',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrUpvByod,



      /* -----------------------------------------------------------------------------------------
         ADOBE CVR — CART START
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'cartstartTotal',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrCartstartTotal,


      MAX(
        IF(
          metric_name = 'cartstartPostpaid',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrCartstartPostpaid,


      MAX(
        IF(
          metric_name = 'cartstartHsi',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrCartstartHsi,


      MAX(
        IF(
          metric_name = 'cartstartByod',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrCartstartByod,



      /* -----------------------------------------------------------------------------------------
         ADOBE CVR — ORDERS
         ----------------------------------------------------------------------------------------- */

      MAX(
        IF(
          metric_name = 'ordersTotal',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersTotal,


      MAX(
        IF(
          metric_name = 'ordersUnassistedTotal',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersUnassistedTotal,


      MAX(
        IF(
          metric_name = 'ordersAssistedTotal',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersAssistedTotal,


      MAX(
        IF(
          metric_name = 'ordersUnassistedPostpaid',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersUnassistedPostpaid,


      MAX(
        IF(
          metric_name = 'ordersAssistedPostpaid',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersAssistedPostpaid,


      MAX(
        IF(
          metric_name = 'ordersUnassistedHsi',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersUnassistedHsi,


      MAX(
        IF(
          metric_name = 'ordersAssistedHsi',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersAssistedHsi,


      MAX(
        IF(
          metric_name = 'ordersUnassistedByod',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersUnassistedByod,


      MAX(
        IF(
          metric_name = 'ordersAssistedByod',
          adobe_cvr_value,
          NULL
        )
      ) AS cvrOrdersAssistedByod


    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly


    WHERE
      metric_type = 'ADOBE_VOLUME'


    GROUP BY

      qgp_date,
      channel_group

  ),



  /* =============================================================================================
     MFC BASE — CHANNEL GRAIN — ACTUAL ONLY

     Only POSTPAID and BROADBAND are retained.
     ============================================================================================= */

  MfcBase AS (

    SELECT

      CAST(qgp_date AS DATE) AS qgp_date,

      channel_group,

      metric_name,


      CASE

        WHEN UPPER(TRIM(lob_mfc)) IN (
          'CONSUMER POSTPAID',
          'POSTPAID'
        )
          THEN 'POSTPAID'


        WHEN UPPER(TRIM(lob_mfc)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'


        ELSE CAST(NULL AS STRING)

      END AS lob,


      metric_value


    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly


    WHERE
          data_source = 'MFC_SPEND_CHANNEL'

      AND metric_name = 'mfcSpendActual'

      AND UPPER(TRIM(lob_mfc)) IN (
            'CONSUMER POSTPAID',
            'POSTPAID',
            'HSI',
            'BROADBAND'
          )

  ),



  /* =============================================================================================
     MFC ACTUAL SPEND

     POSTPAID
     BROADBAND

     TOTAL:
       POSTPAID + BROADBAND
     ============================================================================================= */

  Mfc AS (

    SELECT

      qgp_date,

      channel_group,


      /* -----------------------------------------------------------------------------------------
         POSTPAID
         ----------------------------------------------------------------------------------------- */

      SUM(

        CASE

          WHEN lob = 'POSTPAID'

          THEN metric_value

        END

      ) AS mfcSpendActualPostpaid,


      /* -----------------------------------------------------------------------------------------
         BROADBAND
         ----------------------------------------------------------------------------------------- */

      SUM(

        CASE

          WHEN lob = 'BROADBAND'

          THEN metric_value

        END

      ) AS mfcSpendActualBroadband,


      /* -----------------------------------------------------------------------------------------
         TOTAL = POSTPAID + BROADBAND
         ----------------------------------------------------------------------------------------- */

      SUM(

        CASE

          WHEN lob IN (
            'POSTPAID',
            'BROADBAND'
          )

          THEN metric_value

        END

      ) AS mfcSpendActualTotal


    FROM
      MfcBase


    GROUP BY

      qgp_date,
      channel_group

  ),



  /* =============================================================================================
     CHANNEL SPINE

     One qgp_date x channel_group key-space across:

       Adobe
       MFC

     UNION intentionally removes duplicate keys.

     This prevents a valid Adobe or MFC row from disappearing simply because
     the other source does not contain the same key.
     ============================================================================================= */

  ChannelSpine AS (

    SELECT

      qgp_date,
      channel_group

    FROM
      Adobe

    WHERE
      channel_group IS NOT NULL


    UNION


    SELECT

      qgp_date,
      channel_group

    FROM
      Mfc

    WHERE
      channel_group IS NOT NULL

  )



  /* =============================================================================================
     FINAL WIDE DATASET
     ============================================================================================= */

  SELECT


    /* -------------------------------------------------------------------------------------------
       DIMENSIONS / PERIOD
       ------------------------------------------------------------------------------------------- */

    spine.qgp_date,

    period.week_type,
    period.qgp_quarter,
    period.days_in_period,
    period.is_complete_period,

    spine.channel_group,



    /* -------------------------------------------------------------------------------------------
       ADOBE — UPV
       ------------------------------------------------------------------------------------------- */

    a.upvPostpaid,
    a.upvHsi,
    a.upvByod,
    a.upvFlowTotal,
    a.upvTotalAdobe,



    /* -------------------------------------------------------------------------------------------
       ADOBE — CART START
       ------------------------------------------------------------------------------------------- */

    a.cartstartPostpaid,
    a.cartstartHsi,
    a.cartstartByod,
    a.cartstartTotal,



    /* -------------------------------------------------------------------------------------------
       ADOBE — ORDERS
       ------------------------------------------------------------------------------------------- */

    a.ordersUnassistedPostpaid,
    a.ordersUnassistedHsi,
    a.ordersUnassistedByod,
    a.ordersUnassistedTotal,

    a.ordersAssistedPostpaid,
    a.ordersAssistedHsi,
    a.ordersAssistedByod,
    a.ordersAssistedTotal,

    a.ordersTotal,



    /* -------------------------------------------------------------------------------------------
       MFC — ACTUAL SPEND
       ------------------------------------------------------------------------------------------- */

    m.mfcSpendActualPostpaid,
    m.mfcSpendActualBroadband,
    m.mfcSpendActualTotal,



    /* -------------------------------------------------------------------------------------------
       ADOBE — CVR
       ------------------------------------------------------------------------------------------- */

    a.cvrUpvFlow,
    a.cvrUpvPostpaid,
    a.cvrUpvHsi,
    a.cvrUpvByod,

    a.cvrCartstartTotal,
    a.cvrCartstartPostpaid,
    a.cvrCartstartHsi,
    a.cvrCartstartByod,

    a.cvrOrdersTotal,
    a.cvrOrdersUnassistedTotal,
    a.cvrOrdersAssistedTotal,

    a.cvrOrdersUnassistedPostpaid,
    a.cvrOrdersAssistedPostpaid,

    a.cvrOrdersUnassistedHsi,
    a.cvrOrdersAssistedHsi,

    a.cvrOrdersUnassistedByod,
    a.cvrOrdersAssistedByod


  FROM
    ChannelSpine spine


  LEFT JOIN PeriodMeta period

    ON period.qgp_date = spine.qgp_date


  LEFT JOIN Adobe a

    ON  a.qgp_date = spine.qgp_date
    AND a.channel_group = spine.channel_group


  LEFT JOIN Mfc m

    ON  m.qgp_date = spine.qgp_date
    AND m.channel_group = spine.channel_group

  ;


END;