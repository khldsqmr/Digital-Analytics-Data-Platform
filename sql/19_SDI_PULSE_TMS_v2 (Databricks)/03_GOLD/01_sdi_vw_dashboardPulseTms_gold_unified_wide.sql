/* =================================================================================================
FILE:         09_sdi_vw_dashboardPulseTms_gold_unified_wide.sql
PLATFORM:     Databricks
LAYER:        Gold View — Wide / Sense Check
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_unified_wide

PURPOSE:
  Wide pivot view for quick sense-checking of the PulseTMS pipeline.

  Grain:
    qgp_date x channel_group

  This view is intended for validation / sense-checking only.

  Use:
    sdi_vw_dashboardPulseTms_gold_unified_long

  for production Tableau reporting.


ACTIVE CHANNEL-GRAIN SOURCES:

  ADOBE
    - UPV
    - Cart Start
    - Orders
    - Pre-computed Adobe CVRs

  MFC_SPEND_CHANNEL
    - mfcSpendActualPostpaid
    - mfcSpendActualBroadband
    - mfcSpendActualTotal
    - mfcSpendForecastPostpaid
    - mfcSpendForecastBroadband
    - mfcSpendForecastTotal

  PLATFORM_SPEND_CHANNEL
    - platformSpendPostpaid
    - platformSpendBroadband
    - platformSpendTotal

  BIDDABLE_SPEND_CHANNEL
    - biddableSpendPostpaid
    - biddableSpendBroadband
    - biddableSpendFiber
    - biddableSpendTotal

  UPV_FORECAST
    - upvForecast
    - upvWebAppForecast


DATE-LEVEL SOURCE:

  QGP_SCORECARD
    - 10 QGP Actual/Target metric pairs
    - no channel_group dimension
    - joined by qgp_date and repeated across each channel row


NOT INCLUDED:

  MFC_SPEND_GRANULAR

    Use:
      sdi_vw_dashboardPulseTms_gold_unified_long

    for granular MFC analysis.


---------------------------------------------------------------------------------------------------
SPEND LOB SCOPE — IMPORTANT
---------------------------------------------------------------------------------------------------

Spend Total definitions are source-specific.


MFC:

  mfcSpendActualTotal
    = POSTPAID + BROADBAND

  mfcSpendForecastTotal
    = POSTPAID + BROADBAND


PLATFORM:

  platformSpendTotal
    = POSTPAID + BROADBAND


BIDDABLE:

  biddableSpendTotal
    = ALL
    = POSTPAID + BROADBAND + FIBER


Biddable Silver already creates the synthetic:

  lob = ALL

Therefore the Wide Biddable Total uses the ALL row directly.

Do NOT calculate:

  ALL
    +
  POSTPAID
    +
  BROADBAND
    +
  FIBER

because ALL already contains the three component LOBs.


Other source LOBs such as:

  TFB
  PREPAID
  METRO
  TMONEY
  etc.

remain outside the current approved spend totals unless explicitly added.


FUTURE LOB EXPANSION:

  When additional LOBs such as TFB or PREPAID are formally added:

    1. Add explicit dedicated Wide columns.

    2. Update the appropriate source-specific Total definition.

    3. Do NOT blindly SUM every LOB available in an upstream source.

  Each Total must remain an explicitly controlled business reporting definition.


---------------------------------------------------------------------------------------------------
BIDDABLE SOURCE ARCHITECTURE
---------------------------------------------------------------------------------------------------

BIDDABLE_SPEND_CHANNEL is produced upstream from:

  Programmatic:
    prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

  Paid Social:
    prdrzranalytics.lab42.media_analytics_integrated_snapshot

  Paid Search:
    prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily


Bronze handles source selection.

Current source-specific LOB scope:

  Programmatic:
    POSTPAID
    HSI / BROADBAND

  Paid Social:
    Paid Social
    + Agency = InHouse
    + POSTPAID / HSI / BROADBAND

  Paid Search:
    Google / Bing
    + POSTPAID / HSI / BROADBAND / FIBER


Paid Social does NOT use an individual platform whitelist.

All qualifying Paid Social platforms flow through when they meet the approved:
  channel group
  agency
  LOB

scope.


Biddable Silver canonicalizes:

  POSTPAID / CONSUMER POSTPAID
    -> POSTPAID

  HSI / BROADBAND
    -> BROADBAND

  FIBER
    -> FIBER


Biddable Silver also creates:

  ALL
    = POSTPAID + BROADBAND + FIBER


Therefore this Wide view expects Biddable LOB values:

  ALL
  POSTPAID
  BROADBAND
  FIBER


Gold Wide does NOT perform HSI -> BROADBAND remapping for Biddable.


---------------------------------------------------------------------------------------------------
BIDDABLE CHANNEL REPORTING
---------------------------------------------------------------------------------------------------

Biddable Silver may expose:

  All Channels

  Paid Search - All
  Paid Search - Google
  Paid Search - Bing

  Paid Social - All
  Paid Social - <platform>

  Programmatic - All
  Programmatic - <platform>


Each channel_group value is an alternative reporting selection.

Examples:

  Paid Search - All
    already contains the qualifying Paid Search platforms.

  All Channels
    already contains the approved Programmatic + Paid Social + Paid Search total.

Do NOT sum channel total rows together with their platform-detail rows.


---------------------------------------------------------------------------------------------------
LOB HANDLING
---------------------------------------------------------------------------------------------------

MFC:

  Canonicalization is applied inside MfcBase:

    CONSUMER POSTPAID / POSTPAID
      -> POSTPAID

    HSI / BROADBAND
      -> BROADBAND

    TFB / TBG
      -> TFB


Platform:

  Expected current Wide reporting values:

    POSTPAID
    BROADBAND


Biddable:

  Canonicalized upstream in Biddable Silver:

    POSTPAID
    BROADBAND
    FIBER

  Plus synthetic reporting value:

    ALL = POSTPAID + BROADBAND + FIBER


---------------------------------------------------------------------------------------------------
CHANNEL SPINE
---------------------------------------------------------------------------------------------------

Adobe is NOT used as the sole final-row spine.

ChannelSpine is constructed from:

  Adobe
  MFC
  Platform
  Biddable
  UPV Forecast

This prevents a valid qgp_date x channel_group spend row from disappearing
simply because Adobe does not contain that same channel_group on that date.


---------------------------------------------------------------------------------------------------
QGP GRAIN NOTE
---------------------------------------------------------------------------------------------------

QGP metrics do not contain channel_group.

QGP values are joined using qgp_date only and are therefore repeated across
each channel row for that qgp_date.

Do NOT aggregate QGP metrics across channel_group rows or they will be multiplied.


---------------------------------------------------------------------------------------------------
CALENDAR / COMPLETENESS NOTE
---------------------------------------------------------------------------------------------------

  week_type
  qgp_quarter
  days_in_period
  is_complete_period

are sourced from:

  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar


is_complete_period indicates QGP/calendar completeness.

It does NOT guarantee raw-source settlement.

Source settlement/readiness is monitored separately.


---------------------------------------------------------------------------------------------------
GRAIN
---------------------------------------------------------------------------------------------------

  qgp_date x channel_group


---------------------------------------------------------------------------------------------------
ORDERING
---------------------------------------------------------------------------------------------------

  qgp_date DESC
  channel_group ASC

================================================================================================= */


CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide
AS

WITH


/* ===============================================================================================
   QGP CALENDAR METADATA
   =============================================================================================== */

CalendarMeta AS (

  SELECT
    qgp_date,
    week_type,
    quarter AS qgp_quarter,
    days_in_period,
    is_complete_period

  FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar
),


/* ===============================================================================================
   ADOBE
   =============================================================================================== */

Adobe AS (

  SELECT
    qgp_date,
    channel_group,

    /* -------------------------------------------------------------------------------------------
       UPV
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       CART START
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       ORDERS
       ------------------------------------------------------------------------------------------- */

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

    MAX(
      IF(
        metric_name = 'ordersTotal',
        metric_value,
        NULL
      )
    ) AS ordersTotal,


    /* -------------------------------------------------------------------------------------------
       ADOBE CVR
       ------------------------------------------------------------------------------------------- */

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

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly

  WHERE metric_type = 'ADOBE_VOLUME'

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   MFC BASE

   Canonicalize the LOB vocabulary needed by the Wide spend view.
   =============================================================================================== */

MfcBase AS (

  SELECT
    qgp_date,
    channel_group,
    metric_name,

    CASE
      WHEN UPPER(TRIM(lob_mfc)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID'
      )
        THEN 'POSTPAID'

      WHEN UPPER(TRIM(lob_mfc)) IN (
        'HSI',
        'BROADBAND'
      )
        THEN 'BROADBAND'

      WHEN UPPER(TRIM(lob_mfc)) IN (
        'TFB',
        'TBG'
      )
        THEN 'TFB'

      ELSE UPPER(TRIM(lob_mfc))
    END AS lob,

    metric_value

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly

  WHERE data_source = 'MFC_SPEND_CHANNEL'
),


/* ===============================================================================================
   MFC SPEND

   Current Total:
     POSTPAID + BROADBAND
   =============================================================================================== */

Mfc AS (

  SELECT
    qgp_date,
    channel_group,

    SUM(
      CASE
        WHEN lob = 'POSTPAID'
         AND metric_name = 'mfcSpendActual'
          THEN metric_value
      END
    ) AS mfcSpendActualPostpaid,

    SUM(
      CASE
        WHEN lob = 'BROADBAND'
         AND metric_name = 'mfcSpendActual'
          THEN metric_value
      END
    ) AS mfcSpendActualBroadband,

    SUM(
      CASE
        WHEN lob IN (
          'POSTPAID',
          'BROADBAND'
        )
         AND metric_name = 'mfcSpendActual'
          THEN metric_value
      END
    ) AS mfcSpendActualTotal,

    SUM(
      CASE
        WHEN lob = 'POSTPAID'
         AND metric_name = 'mfcSpendForecast'
          THEN metric_value
      END
    ) AS mfcSpendForecastPostpaid,

    SUM(
      CASE
        WHEN lob = 'BROADBAND'
         AND metric_name = 'mfcSpendForecast'
          THEN metric_value
      END
    ) AS mfcSpendForecastBroadband,

    SUM(
      CASE
        WHEN lob IN (
          'POSTPAID',
          'BROADBAND'
        )
         AND metric_name = 'mfcSpendForecast'
          THEN metric_value
      END
    ) AS mfcSpendForecastTotal

  FROM MfcBase

  WHERE lob IN (
    'POSTPAID',
    'BROADBAND'
  )

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   PLATFORM BASE
   =============================================================================================== */

PlatformBase AS (

  SELECT
    qgp_date,
    channel_group,
    metric_name,
    UPPER(TRIM(lob)) AS lob,
    metric_value

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly
),


/* ===============================================================================================
   PLATFORM SPEND

   Current Total:
     POSTPAID + BROADBAND
   =============================================================================================== */

Platform AS (

  SELECT
    qgp_date,
    channel_group,

    SUM(
      CASE
        WHEN lob = 'POSTPAID'
         AND metric_name = 'platformSpend'
          THEN metric_value
      END
    ) AS platformSpendPostpaid,

    SUM(
      CASE
        WHEN lob = 'BROADBAND'
         AND metric_name = 'platformSpend'
          THEN metric_value
      END
    ) AS platformSpendBroadband,

    SUM(
      CASE
        WHEN lob IN (
          'POSTPAID',
          'BROADBAND'
        )
         AND metric_name = 'platformSpend'
          THEN metric_value
      END
    ) AS platformSpendTotal

  FROM PlatformBase

  WHERE lob IN (
    'POSTPAID',
    'BROADBAND'
  )

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   BIDDABLE BASE

   Biddable Silver already owns LOB canonicalization and reporting rollups.

   Expected Silver LOB values:

     ALL
     POSTPAID
     BROADBAND
     FIBER

   ALL already equals:

     POSTPAID + BROADBAND + FIBER

   Therefore ALL must NOT be summed together with its component LOBs.
   =============================================================================================== */

BiddableBase AS (

  SELECT
    qgp_date,
    channel_group,
    metric_name,
    UPPER(TRIM(lob)) AS lob,
    metric_value

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly

  WHERE data_source = 'BIDDABLE_SPEND_CHANNEL'
    AND lob IN (
      'ALL',
      'POSTPAID',
      'BROADBAND',
      'FIBER'
    )
),


/* ===============================================================================================
   BIDDABLE SPEND

   Reporting columns:

     biddableSpendPostpaid
       -> Silver lob = POSTPAID

     biddableSpendBroadband
       -> Silver lob = BROADBAND

     biddableSpendFiber
       -> Silver lob = FIBER

     biddableSpendTotal
       -> Silver lob = ALL
       -> POSTPAID + BROADBAND + FIBER

   IMPORTANT:

     Total is sourced directly from the Silver ALL row.

     It is NOT recalculated by summing the three component rows here.
   =============================================================================================== */

Biddable AS (

  SELECT
    qgp_date,
    channel_group,

    SUM(
      CASE
        WHEN lob = 'POSTPAID'
         AND metric_name = 'biddableSpend'
          THEN metric_value
      END
    ) AS biddableSpendPostpaid,

    SUM(
      CASE
        WHEN lob = 'BROADBAND'
         AND metric_name = 'biddableSpend'
          THEN metric_value
      END
    ) AS biddableSpendBroadband,

    SUM(
      CASE
        WHEN lob = 'FIBER'
         AND metric_name = 'biddableSpend'
          THEN metric_value
      END
    ) AS biddableSpendFiber,

    SUM(
      CASE
        WHEN lob = 'ALL'
         AND metric_name = 'biddableSpend'
          THEN metric_value
      END
    ) AS biddableSpendTotal

  FROM BiddableBase

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   UPV FORECAST
   =============================================================================================== */

UpvForecast AS (

  SELECT
    qgp_date,
    channel_group,

    MAX(
      IF(
        metric_name = 'upvForecast',
        metric_value,
        NULL
      )
    ) AS upvForecast,

    MAX(
      IF(
        metric_name = 'upvWebAppForecast',
        metric_value,
        NULL
      )
    ) AS upvWebAppForecast

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   QGP SCORECARD — DATE GRAIN
   =============================================================================================== */

Qgp AS (

  SELECT
    qgp_date,

    MAX(
      IF(
        metric_name = 'activationsBopis'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpActivationsBopisActual,

    MAX(
      IF(
        metric_name = 'activationsBopis'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpActivationsBopisTarget,

    MAX(
      IF(
        metric_name = 'activationsNewAalNoAssistance'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpActivationsNewAalNoAssistanceActual,

    MAX(
      IF(
        metric_name = 'activationsNewAalNoAssistance'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpActivationsNewAalNoAssistanceTarget,

    MAX(
      IF(
        metric_name = 'storeTraffic'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpStoreTrafficActual,

    MAX(
      IF(
        metric_name = 'storeTraffic'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpStoreTrafficTarget,

    MAX(
      IF(
        metric_name = 'vrCalls'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpVrCallsActual,

    MAX(
      IF(
        metric_name = 'vrCalls'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpVrCallsTarget,

    MAX(
      IF(
        metric_name = 'vrChats'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpVrChatsActual,

    MAX(
      IF(
        metric_name = 'vrChats'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpVrChatsTarget,

    MAX(
      IF(
        metric_name = 'vrPostpaidActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpVrPostpaidActivationsActual,

    MAX(
      IF(
        metric_name = 'vrPostpaidActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpVrPostpaidActivationsTarget,

    MAX(
      IF(
        metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctPhoneNewActsNoAssistPlusAssistActual,

    MAX(
      IF(
        metric_name = 'digitalPctPhoneNewActsNoAssistPlusAssist'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctPhoneNewActsNoAssistPlusAssistTarget,

    MAX(
      IF(
        metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual,

    MAX(
      IF(
        metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget,

    MAX(
      IF(
        metric_name = 'digitalPctNoAssistanceActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctNoAssistanceActivationsActual,

    MAX(
      IF(
        metric_name = 'digitalPctNoAssistanceActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctNoAssistanceActivationsTarget,

    MAX(
      IF(
        metric_name = 'digitalPctAssistanceActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctAssistanceActivationsActual,

    MAX(
      IF(
        metric_name = 'digitalPctAssistanceActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpDigitalPctAssistanceActivationsTarget

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly

  GROUP BY
    qgp_date
),


/* ===============================================================================================
   CHANNEL SPINE

   One qgp_date x channel_group key-space across every channel-grain source.

   UNION intentionally removes duplicate keys.

   Biddable may introduce platform-level channel_group values such as:
     Paid Search - Google
     Paid Social - Facebook
     Programmatic - DV360

   Those are intentionally retained for Wide sense-checking.
   =============================================================================================== */

ChannelSpine AS (

  SELECT
    qgp_date,
    channel_group
  FROM Adobe
  WHERE channel_group IS NOT NULL

  UNION

  SELECT
    qgp_date,
    channel_group
  FROM Mfc
  WHERE channel_group IS NOT NULL

  UNION

  SELECT
    qgp_date,
    channel_group
  FROM Platform
  WHERE channel_group IS NOT NULL

  UNION

  SELECT
    qgp_date,
    channel_group
  FROM Biddable
  WHERE channel_group IS NOT NULL

  UNION

  SELECT
    qgp_date,
    channel_group
  FROM UpvForecast
  WHERE channel_group IS NOT NULL
)


/* ===============================================================================================
   FINAL WIDE DATASET
   =============================================================================================== */

SELECT

  /* ---------------------------------------------------------------------------------------------
     DIMENSIONS / PERIOD METADATA
     --------------------------------------------------------------------------------------------- */

  spine.qgp_date,
  cal.week_type,
  cal.qgp_quarter,
  cal.days_in_period,
  cal.is_complete_period,
  spine.channel_group,


  /* ---------------------------------------------------------------------------------------------
     ADOBE UPV
     --------------------------------------------------------------------------------------------- */

  a.upvPostpaid,
  a.upvHsi,
  a.upvByod,
  a.upvFlowTotal,
  a.upvTotalAdobe,


  /* ---------------------------------------------------------------------------------------------
     ADOBE CART START
     --------------------------------------------------------------------------------------------- */

  a.cartstartPostpaid,
  a.cartstartHsi,
  a.cartstartByod,
  a.cartstartTotal,


  /* ---------------------------------------------------------------------------------------------
     ADOBE ORDERS
     --------------------------------------------------------------------------------------------- */

  a.ordersUnassistedPostpaid,
  a.ordersUnassistedHsi,
  a.ordersUnassistedByod,
  a.ordersUnassistedTotal,

  a.ordersAssistedPostpaid,
  a.ordersAssistedHsi,
  a.ordersAssistedByod,
  a.ordersAssistedTotal,

  a.ordersTotal,


  /* ---------------------------------------------------------------------------------------------
     MFC ACTUAL SPEND

     Total = POSTPAID + BROADBAND
     --------------------------------------------------------------------------------------------- */

  m.mfcSpendActualPostpaid,
  m.mfcSpendActualBroadband,
  m.mfcSpendActualTotal,


  /* ---------------------------------------------------------------------------------------------
     MFC FORECAST SPEND

     Total = POSTPAID + BROADBAND
     --------------------------------------------------------------------------------------------- */

  m.mfcSpendForecastPostpaid,
  m.mfcSpendForecastBroadband,
  m.mfcSpendForecastTotal,


  /* ---------------------------------------------------------------------------------------------
     PLATFORM SPEND

     Total = POSTPAID + BROADBAND
     --------------------------------------------------------------------------------------------- */

  p.platformSpendPostpaid,
  p.platformSpendBroadband,
  p.platformSpendTotal,


  /* ---------------------------------------------------------------------------------------------
     BIDDABLE SPEND

     Total = ALL
           = POSTPAID + BROADBAND + FIBER

     Total is read directly from the Silver ALL reporting row.
     --------------------------------------------------------------------------------------------- */

  b.biddableSpendPostpaid,
  b.biddableSpendBroadband,
  b.biddableSpendFiber,
  b.biddableSpendTotal,


  /* ---------------------------------------------------------------------------------------------
     UPV FORECAST
     --------------------------------------------------------------------------------------------- */

  uf.upvForecast,
  uf.upvWebAppForecast,


  /* ---------------------------------------------------------------------------------------------
     QGP SCORECARD

     Date-level values repeated across channel_group rows.
     Do not aggregate these across channel_group.
     --------------------------------------------------------------------------------------------- */

  q.qgpActivationsBopisActual,
  q.qgpActivationsBopisTarget,

  q.qgpActivationsNewAalNoAssistanceActual,
  q.qgpActivationsNewAalNoAssistanceTarget,

  q.qgpStoreTrafficActual,
  q.qgpStoreTrafficTarget,

  q.qgpVrCallsActual,
  q.qgpVrCallsTarget,

  q.qgpVrChatsActual,
  q.qgpVrChatsTarget,

  q.qgpVrPostpaidActivationsActual,
  q.qgpVrPostpaidActivationsTarget,

  q.qgpDigitalPctPhoneNewActsNoAssistPlusAssistActual,
  q.qgpDigitalPctPhoneNewActsNoAssistPlusAssistTarget,

  q.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual,
  q.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedTarget,

  q.qgpDigitalPctNoAssistanceActivationsActual,
  q.qgpDigitalPctNoAssistanceActivationsTarget,

  q.qgpDigitalPctAssistanceActivationsActual,
  q.qgpDigitalPctAssistanceActivationsTarget,


  /* ---------------------------------------------------------------------------------------------
     ADOBE CVR
     --------------------------------------------------------------------------------------------- */

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


FROM ChannelSpine spine


/* ===============================================================================================
   CALENDAR
   =============================================================================================== */

LEFT JOIN CalendarMeta cal
  ON cal.qgp_date = spine.qgp_date


/* ===============================================================================================
   ADOBE
   =============================================================================================== */

LEFT JOIN Adobe a
  ON  a.qgp_date      = spine.qgp_date
  AND a.channel_group = spine.channel_group


/* ===============================================================================================
   MFC
   =============================================================================================== */

LEFT JOIN Mfc m
  ON  m.qgp_date      = spine.qgp_date
  AND m.channel_group = spine.channel_group


/* ===============================================================================================
   PLATFORM
   =============================================================================================== */

LEFT JOIN Platform p
  ON  p.qgp_date      = spine.qgp_date
  AND p.channel_group = spine.channel_group


/* ===============================================================================================
   BIDDABLE
   =============================================================================================== */

LEFT JOIN Biddable b
  ON  b.qgp_date      = spine.qgp_date
  AND b.channel_group = spine.channel_group


/* ===============================================================================================
   UPV FORECAST
   =============================================================================================== */

LEFT JOIN UpvForecast uf
  ON  uf.qgp_date      = spine.qgp_date
  AND uf.channel_group = spine.channel_group


/* ===============================================================================================
   QGP SCORECARD

   Date-only join because QGP has no channel_group dimension.
   =============================================================================================== */

LEFT JOIN Qgp q
  ON q.qgp_date = spine.qgp_date


ORDER BY
  spine.qgp_date DESC,
  spine.channel_group ASC
;