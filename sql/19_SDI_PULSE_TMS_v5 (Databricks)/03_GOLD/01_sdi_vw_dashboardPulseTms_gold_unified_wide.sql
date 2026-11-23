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
    - C2C / Chat Engagement / Eligibility Check (EC)
    - Orders
    - Pre-computed Adobe CVRs
    - Metro / TFB mPulse metrics (18 volumes + 16 CVRs = metric / the same LOB's own UPV)

  MFC_SPEND_CHANNEL
    - mfcSpendActualPostpaid
    - mfcSpendActualBroadband
    - mfcSpendActualTfb
    - mfcSpendActualPrepaid
    - mfcSpendActualTotal
    - mfcSpendForecastPostpaid
    - mfcSpendForecastBroadband
    - mfcSpendForecastTfb
    - mfcSpendForecastPrepaid
    - mfcSpendForecastTotal

  PLATFORM_SPEND_CHANNEL
    - platformSpendPostpaid
    - platformSpendBroadband
    - platformSpendTotal

  BIDDABLE_SPEND_CHANNEL
    - biddableSpendPostpaid
    - biddableSpendBroadband
    - biddableSpendTotal   (= Postpaid + Broadband + Prepaid + Tfb)
    - biddableSpendPrepaid
    - biddableSpendTfb

  UPV_FORECAST
    - upvForecast
    - upvWebAppForecast


DATE-LEVEL SOURCE:

  QGP_SCORECARD
    - 13 named QGP metrics (the 12 original + vrChatsPctOfChatEngagement)
    - 12 Metro/TFB QGP metrics (11 added 2026-11-10, metroMediaSpend added 2026-11-12)
    - Actual/Target representation
    - no channel_group dimension
    - joined by qgp_date and repeated across each channel row

  Phone activation metrics include:

    activationsBopis
      Existing combined Phone BOPIS + Non-BOPIS metric

    activationsBopisOnly
      BOPIS-only component

    activationsNonBopisOnly
      Non-BOPIS-only component


NOT INCLUDED:

  MFC_SPEND_GRANULAR

  Use:
    sdi_vw_dashboardPulseTms_gold_unified_long

  for granular MFC analysis.


---------------------------------------------------------------------------------------------------
ADOBE C2C / CHAT ENGAGEMENT / ELIGIBILITY CHECK (EC)
---------------------------------------------------------------------------------------------------

Four Adobe metrics sit between Cart Start and Orders:

  c2cVisitors
    UPVs with a click to call (visitors)

  chatEngagementVisitors
    Chat engagement, catch-all VR + Care (visitors)

  ecCompletedPageEvents
    Broadband Eligibility Check completed (page EVENT count)

  ecSuccessfulVisits
    Broadband Eligibility Check successful (VISIT count)

The two EC metrics are different units. Do NOT divide one by the other.

Each of the four has an Adobe CVR column in the ADOBE CVR block of this view:

  cvrC2c, cvrChatEngagement, cvrEcCompleted, cvrEcSuccessful = metric / upvTotalAdobe


---------------------------------------------------------------------------------------------------
METRO / TFB (added 2026-11-10)
---------------------------------------------------------------------------------------------------

ADOBE (channel grain, same columns as the Silver metric_name; CVR columns are cvr<MetricName> = metric / the same
LOB's own UPV, Metro: metroProspectVisitors, TFB: tfbExclSinglePageVisitors; none for those two UPV metrics):

  Metro
    metroProspectVisitors, metroHomepageEntryVisitors, metroDealsEntryVisitors,
    metroProductPlansEntryVisitors, metroOtherEntryVisitors, metroActivationIntentVisitors,
    metroCartStartVisitors, metroProspectClicktocallVisitors      (visitors)
    metroOrderConfirmationOrders                                  (orders)

  TFB
    tfbExclSinglePageVisitors, tfbCartStartsVisitors, tfbClicktocallExistsVisitors,
    tfbChatEngagementVisitors                                     (visitors)
    tfbLeadgenFormCompleteEvent104, tfbCreditCheckEvent104        (event 104)
    tfbAllClicktocallEvent95                                      (event 95)
    tfbAllChatEngagementEvent21                                   (event 21)
    tfbEcommOrderHitOrders                                        (orders)

  Units differ by metric: do not divide one by another. Unique-visitor metrics are not additive
  across channel_group.

QGP (date grain, repeated across channel rows, Actual / Target):

  qgpMetroActivations*, qgpMetroNets*, qgpMetroDigitalActivationsPhoneAndOther*, qgpMetroNewAccounts*,
  qgpMetroPspvs* (Target is NULL in the feed), qgpMetroMediaSpend* (Actual corrected in the QGP Silver),
  qgpTfbTotalActivations*, qgpTfbPhoneActivations*,
  qgpTfbHsiActivations*, qgpTfbDigitalChannelActivations*, qgpTfbEcommerceActivations*,
  qgpTfbSalesCredit*   (each as ...Actual and ...Target)

Metro media spend is qgpMetroMediaSpendActual / qgpMetroMediaSpendTarget. The Actual is corrected in the QGP Silver
(stub-week placeholder ignored, whole-week actual split by days, newest placeholder week blanked) and can be NULL for
the newest week.


---------------------------------------------------------------------------------------------------
SPEND LOB SCOPE — IMPORTANT
---------------------------------------------------------------------------------------------------

Spend Total definitions are source-specific.

MFC:

  mfcSpendActualTotal
    = POSTPAID + BROADBAND + TFB + CONSUMER PREPAID

  mfcSpendForecastTotal
    = POSTPAID + BROADBAND + TFB + CONSUMER PREPAID

  Changed 2026-11-10: these MFC Totals previously covered POSTPAID + BROADBAND only. TFB and
  CONSUMER PREPAID are separate LOBs, so the four components do not overlap, and each has its own
  column (mfcSpend{Actual|Forecast}{Postpaid|Broadband|Tfb|Prepaid}). The Platform and Biddable
  Totals below are unchanged.


PLATFORM:

  platformSpendTotal
    = POSTPAID + BROADBAND


BIDDABLE (four LOBs, changed 2026-11-15):

  biddableSpendTotal
    = ALL
    = POSTPAID + BROADBAND + PREPAID + TFB

Biddable Silver already creates:

  lob = ALL

Therefore Wide uses the existing ALL row directly.

Do NOT calculate:

  ALL
    +
  POSTPAID
    +
  BROADBAND
    +
  PREPAID
    +
  TFB

because ALL already contains the component LOBs. Fiber is part of BROADBAND, so there is no
Fiber column any more.


Other source LOBs such as:

  TMONEY
  HOME INTERNET
  etc.

remain outside the current approved spend totals unless explicitly added.
(TFB and CONSUMER PREPAID are inside the MFC Totals; the Platform Total still covers
POSTPAID + BROADBAND only.)


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
    POSTPAID / HSI / BROADBAND / PREPAID (includes Metro) / TFB

  Paid Social:
    Paid Social
    + Agency = InHouse or In-House
    + POSTPAID / HSI / BROADBAND / PREPAID (includes Metro) / TFB

  Paid Search:
    Google / Bing
    + POSTPAID / HSI / BROADBAND / FIBER / METRO / TFB

Paid Social does NOT use an individual platform whitelist.

Biddable Silver canonicalizes:

  POSTPAID / CONSUMER POSTPAID
    -> POSTPAID

  HSI / BROADBAND / FIBER
    -> BROADBAND

  PREPAID / METRO
    -> PREPAID

  TFB
    -> TFB

and creates:

  ALL
    = POSTPAID + BROADBAND + PREPAID + TFB

Therefore Wide expects:

  ALL
  POSTPAID
  BROADBAND
  PREPAID
  TFB


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

Do NOT sum channel totals together with their platform-detail rows.

In Wide (changed 2026-11-10): only the channel TOTAL rows are shown, mapped onto the main groups:

  All Channels       -> All Channels
  Paid Search - All  -> Paid Search
  Paid Social - All  -> Paid Social
  Programmatic - All -> Programmatic

The platform-detail rows are not in Wide (they remain in the long view).


---------------------------------------------------------------------------------------------------
QGP METRIC HANDLING
---------------------------------------------------------------------------------------------------

All QGP business metric construction occurs in Silver.

Gold Wide only pivots Silver metric_name / metric_type rows.

Phone activation fields:

  qgpActivationsBopisActual
  qgpActivationsBopisTarget

  qgpActivationsBopisOnlyActual
  qgpActivationsBopisOnlyTarget

  qgpActivationsNonBopisOnlyActual
  qgpActivationsNonBopisOnlyTarget

The existing qgpActivationsBopis fields remain unchanged.

No BOPIS / Non-BOPIS derivation occurs in this Wide view.

vrChatsPctOfChatEngagement (VR Chats as a share of Adobe Chat Engagement, All Channels) is built in the
QGP Silver and is only pivoted here as qgpVrChatsPctOfChatEngagementActual / ...Target (Target is NULL).


---------------------------------------------------------------------------------------------------
CHANNEL SPINE
---------------------------------------------------------------------------------------------------

ChannelSpine is constructed from:

  Adobe
  MFC
  Platform
  Biddable
  UPV Forecast

This prevents valid spend/channel rows from disappearing when Adobe lacks the same key.

MAIN CHANNEL GROUPS ONLY (changed 2026-11-10):

  All Channels | Paid Search | Paid Social | Organic Search | Direct | Programmatic | Other

Every channel-grain source is restricted to that list (CTE MainChannelGroups), so Wide has no other
channel_group values. Rows for groups outside the list, such as Platform's iSpot National TV,
iSpot OTT and Affiliate, are not shown in Wide; they stay in the long view, and the All Channels row
of each source still contains their spend.


---------------------------------------------------------------------------------------------------
QGP GRAIN NOTE
---------------------------------------------------------------------------------------------------

QGP metrics do not contain channel_group.

QGP values are joined using qgp_date only and are repeated across each channel row.

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
   MAIN CHANNEL GROUPS

   The only channel_group values Wide shows. One place to change if the vocabulary changes.
   Every channel-grain source below is restricted to this list, so rows for any other group
   (Platform iSpot / Affiliate, Biddable platform-level groups, ...) never reach the spine.
   =============================================================================================== */

MainChannelGroups AS (

  SELECT channel_group
  FROM VALUES
    ('All Channels'),
    ('Paid Search'),
    ('Paid Social'),
    ('Organic Search'),
    ('Direct'),
    ('Programmatic'),
    ('Other')
  AS t(channel_group)
),


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

    MAX(IF(metric_name = 'upvPostpaid', metric_value, NULL)) AS upvPostpaid,
    MAX(IF(metric_name = 'upvHsi', metric_value, NULL)) AS upvHsi,
    MAX(IF(metric_name = 'upvByod', metric_value, NULL)) AS upvByod,
    MAX(IF(metric_name = 'upvFlowTotal', metric_value, NULL)) AS upvFlowTotal,
    MAX(IF(metric_name = 'upvTotalAdobe', metric_value, NULL)) AS upvTotalAdobe,

    MAX(IF(metric_name = 'cartstartPostpaid', metric_value, NULL)) AS cartstartPostpaid,
    MAX(IF(metric_name = 'cartstartHsi', metric_value, NULL)) AS cartstartHsi,
    MAX(IF(metric_name = 'cartstartByod', metric_value, NULL)) AS cartstartByod,
    MAX(IF(metric_name = 'cartstartTotal', metric_value, NULL)) AS cartstartTotal,

    MAX(IF(metric_name = 'c2cVisitors', metric_value, NULL)) AS c2cVisitors,
    MAX(IF(metric_name = 'chatEngagementVisitors', metric_value, NULL)) AS chatEngagementVisitors,
    MAX(IF(metric_name = 'ecCompletedPageEvents', metric_value, NULL)) AS ecCompletedPageEvents,
    MAX(IF(metric_name = 'ecSuccessfulVisits', metric_value, NULL)) AS ecSuccessfulVisits,

    MAX(IF(metric_name = 'ordersUnassistedPostpaid', metric_value, NULL)) AS ordersUnassistedPostpaid,
    MAX(IF(metric_name = 'ordersUnassistedHsi', metric_value, NULL)) AS ordersUnassistedHsi,
    MAX(IF(metric_name = 'ordersUnassistedByod', metric_value, NULL)) AS ordersUnassistedByod,
    MAX(IF(metric_name = 'ordersUnassistedTotal', metric_value, NULL)) AS ordersUnassistedTotal,

    MAX(IF(metric_name = 'ordersAssistedPostpaid', metric_value, NULL)) AS ordersAssistedPostpaid,
    MAX(IF(metric_name = 'ordersAssistedHsi', metric_value, NULL)) AS ordersAssistedHsi,
    MAX(IF(metric_name = 'ordersAssistedByod', metric_value, NULL)) AS ordersAssistedByod,
    MAX(IF(metric_name = 'ordersAssistedTotal', metric_value, NULL)) AS ordersAssistedTotal,

    MAX(IF(metric_name = 'ordersTotal', metric_value, NULL)) AS ordersTotal,

    /* ---- METRO / TFB mPULSE volumes ---- */
    MAX(IF(metric_name = 'metroProspectVisitors', metric_value, NULL)) AS metroProspectVisitors,
    MAX(IF(metric_name = 'metroHomepageEntryVisitors', metric_value, NULL)) AS metroHomepageEntryVisitors,
    MAX(IF(metric_name = 'metroDealsEntryVisitors', metric_value, NULL)) AS metroDealsEntryVisitors,
    MAX(IF(metric_name = 'metroProductPlansEntryVisitors', metric_value, NULL)) AS metroProductPlansEntryVisitors,
    MAX(IF(metric_name = 'metroOtherEntryVisitors', metric_value, NULL)) AS metroOtherEntryVisitors,
    MAX(IF(metric_name = 'metroActivationIntentVisitors', metric_value, NULL)) AS metroActivationIntentVisitors,
    MAX(IF(metric_name = 'metroCartStartVisitors', metric_value, NULL)) AS metroCartStartVisitors,
    MAX(IF(metric_name = 'metroProspectClicktocallVisitors', metric_value, NULL)) AS metroProspectClicktocallVisitors,
    MAX(IF(metric_name = 'metroOrderConfirmationOrders', metric_value, NULL)) AS metroOrderConfirmationOrders,

    MAX(IF(metric_name = 'tfbExclSinglePageVisitors', metric_value, NULL)) AS tfbExclSinglePageVisitors,
    MAX(IF(metric_name = 'tfbLeadgenFormCompleteEvent104', metric_value, NULL)) AS tfbLeadgenFormCompleteEvent104,
    MAX(IF(metric_name = 'tfbCreditCheckEvent104', metric_value, NULL)) AS tfbCreditCheckEvent104,
    MAX(IF(metric_name = 'tfbAllClicktocallEvent95', metric_value, NULL)) AS tfbAllClicktocallEvent95,
    MAX(IF(metric_name = 'tfbAllChatEngagementEvent21', metric_value, NULL)) AS tfbAllChatEngagementEvent21,
    MAX(IF(metric_name = 'tfbCartStartsVisitors', metric_value, NULL)) AS tfbCartStartsVisitors,
    MAX(IF(metric_name = 'tfbEcommOrderHitOrders', metric_value, NULL)) AS tfbEcommOrderHitOrders,
    MAX(IF(metric_name = 'tfbClicktocallExistsVisitors', metric_value, NULL)) AS tfbClicktocallExistsVisitors,
    MAX(IF(metric_name = 'tfbChatEngagementVisitors', metric_value, NULL)) AS tfbChatEngagementVisitors,

    /* ---- METRO / TFB mPULSE CVR: metric / the same LOB's own UPV (none for the two UPV metrics) ---- */
    MAX(IF(metric_name = 'metroHomepageEntryVisitors', adobe_cvr_value, NULL)) AS cvrMetroHomepageEntryVisitors,
    MAX(IF(metric_name = 'metroDealsEntryVisitors', adobe_cvr_value, NULL)) AS cvrMetroDealsEntryVisitors,
    MAX(IF(metric_name = 'metroProductPlansEntryVisitors', adobe_cvr_value, NULL)) AS cvrMetroProductPlansEntryVisitors,
    MAX(IF(metric_name = 'metroOtherEntryVisitors', adobe_cvr_value, NULL)) AS cvrMetroOtherEntryVisitors,
    MAX(IF(metric_name = 'metroActivationIntentVisitors', adobe_cvr_value, NULL)) AS cvrMetroActivationIntentVisitors,
    MAX(IF(metric_name = 'metroCartStartVisitors', adobe_cvr_value, NULL)) AS cvrMetroCartStartVisitors,
    MAX(IF(metric_name = 'metroProspectClicktocallVisitors', adobe_cvr_value, NULL)) AS cvrMetroProspectClicktocallVisitors,
    MAX(IF(metric_name = 'metroOrderConfirmationOrders', adobe_cvr_value, NULL)) AS cvrMetroOrderConfirmationOrders,
    MAX(IF(metric_name = 'tfbLeadgenFormCompleteEvent104', adobe_cvr_value, NULL)) AS cvrTfbLeadgenFormCompleteEvent104,
    MAX(IF(metric_name = 'tfbCreditCheckEvent104', adobe_cvr_value, NULL)) AS cvrTfbCreditCheckEvent104,
    MAX(IF(metric_name = 'tfbAllClicktocallEvent95', adobe_cvr_value, NULL)) AS cvrTfbAllClicktocallEvent95,
    MAX(IF(metric_name = 'tfbAllChatEngagementEvent21', adobe_cvr_value, NULL)) AS cvrTfbAllChatEngagementEvent21,
    MAX(IF(metric_name = 'tfbCartStartsVisitors', adobe_cvr_value, NULL)) AS cvrTfbCartStartsVisitors,
    MAX(IF(metric_name = 'tfbEcommOrderHitOrders', adobe_cvr_value, NULL)) AS cvrTfbEcommOrderHitOrders,
    MAX(IF(metric_name = 'tfbClicktocallExistsVisitors', adobe_cvr_value, NULL)) AS cvrTfbClicktocallExistsVisitors,
    MAX(IF(metric_name = 'tfbChatEngagementVisitors', adobe_cvr_value, NULL)) AS cvrTfbChatEngagementVisitors,

    MAX(IF(metric_name = 'upvFlowTotal', adobe_cvr_value, NULL)) AS cvrUpvFlow,
    MAX(IF(metric_name = 'upvPostpaid', adobe_cvr_value, NULL)) AS cvrUpvPostpaid,
    MAX(IF(metric_name = 'upvHsi', adobe_cvr_value, NULL)) AS cvrUpvHsi,
    MAX(IF(metric_name = 'upvByod', adobe_cvr_value, NULL)) AS cvrUpvByod,

    MAX(IF(metric_name = 'cartstartTotal', adobe_cvr_value, NULL)) AS cvrCartstartTotal,
    MAX(IF(metric_name = 'cartstartPostpaid', adobe_cvr_value, NULL)) AS cvrCartstartPostpaid,
    MAX(IF(metric_name = 'cartstartHsi', adobe_cvr_value, NULL)) AS cvrCartstartHsi,
    MAX(IF(metric_name = 'cartstartByod', adobe_cvr_value, NULL)) AS cvrCartstartByod,

    MAX(IF(metric_name = 'c2cVisitors', adobe_cvr_value, NULL)) AS cvrC2c,
    MAX(IF(metric_name = 'chatEngagementVisitors', adobe_cvr_value, NULL)) AS cvrChatEngagement,
    MAX(IF(metric_name = 'ecCompletedPageEvents', adobe_cvr_value, NULL)) AS cvrEcCompleted,
    MAX(IF(metric_name = 'ecSuccessfulVisits', adobe_cvr_value, NULL)) AS cvrEcSuccessful,

    MAX(IF(metric_name = 'ordersTotal', adobe_cvr_value, NULL)) AS cvrOrdersTotal,
    MAX(IF(metric_name = 'ordersUnassistedTotal', adobe_cvr_value, NULL)) AS cvrOrdersUnassistedTotal,
    MAX(IF(metric_name = 'ordersAssistedTotal', adobe_cvr_value, NULL)) AS cvrOrdersAssistedTotal,
    MAX(IF(metric_name = 'ordersUnassistedPostpaid', adobe_cvr_value, NULL)) AS cvrOrdersUnassistedPostpaid,
    MAX(IF(metric_name = 'ordersAssistedPostpaid', adobe_cvr_value, NULL)) AS cvrOrdersAssistedPostpaid,
    MAX(IF(metric_name = 'ordersUnassistedHsi', adobe_cvr_value, NULL)) AS cvrOrdersUnassistedHsi,
    MAX(IF(metric_name = 'ordersAssistedHsi', adobe_cvr_value, NULL)) AS cvrOrdersAssistedHsi,
    MAX(IF(metric_name = 'ordersUnassistedByod', adobe_cvr_value, NULL)) AS cvrOrdersUnassistedByod,
    MAX(IF(metric_name = 'ordersAssistedByod', adobe_cvr_value, NULL)) AS cvrOrdersAssistedByod

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly
  WHERE metric_type = 'ADOBE_VOLUME'
    AND channel_group IN (SELECT channel_group FROM MainChannelGroups)

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   MFC BASE
   =============================================================================================== */

MfcBase AS (

  SELECT
    qgp_date,
    channel_group,
    metric_name,

    CASE
      WHEN UPPER(TRIM(lob_mfc)) IN ('POSTPAID', 'CONSUMER POSTPAID') THEN 'POSTPAID'
      WHEN UPPER(TRIM(lob_mfc)) IN ('HSI', 'BROADBAND') THEN 'BROADBAND'
      WHEN UPPER(TRIM(lob_mfc)) IN ('TFB', 'TBG') THEN 'TFB'
      ELSE UPPER(TRIM(lob_mfc))
    END AS lob,

    metric_value

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly
  WHERE data_source = 'MFC_SPEND_CHANNEL'
    AND channel_group IN (SELECT channel_group FROM MainChannelGroups)
),


/* ===============================================================================================
   MFC SPEND

   Total = POSTPAID + BROADBAND + TFB + CONSUMER PREPAID
   =============================================================================================== */

Mfc AS (

  SELECT
    qgp_date,
    channel_group,

    SUM(CASE
          WHEN lob = 'POSTPAID'
           AND metric_name = 'mfcSpendActual'
          THEN metric_value
        END) AS mfcSpendActualPostpaid,

    SUM(CASE
          WHEN lob = 'BROADBAND'
           AND metric_name = 'mfcSpendActual'
          THEN metric_value
        END) AS mfcSpendActualBroadband,

    SUM(CASE
          WHEN lob = 'TFB'
           AND metric_name = 'mfcSpendActual'
          THEN metric_value
        END) AS mfcSpendActualTfb,

    SUM(CASE
          WHEN lob = 'CONSUMER PREPAID'
           AND metric_name = 'mfcSpendActual'
          THEN metric_value
        END) AS mfcSpendActualPrepaid,

    SUM(CASE
          WHEN lob IN ('POSTPAID', 'BROADBAND', 'TFB', 'CONSUMER PREPAID')
           AND metric_name = 'mfcSpendActual'
          THEN metric_value
        END) AS mfcSpendActualTotal,

    SUM(CASE
          WHEN lob = 'POSTPAID'
           AND metric_name = 'mfcSpendForecast'
          THEN metric_value
        END) AS mfcSpendForecastPostpaid,

    SUM(CASE
          WHEN lob = 'BROADBAND'
           AND metric_name = 'mfcSpendForecast'
          THEN metric_value
        END) AS mfcSpendForecastBroadband,

    SUM(CASE
          WHEN lob = 'TFB'
           AND metric_name = 'mfcSpendForecast'
          THEN metric_value
        END) AS mfcSpendForecastTfb,

    SUM(CASE
          WHEN lob = 'CONSUMER PREPAID'
           AND metric_name = 'mfcSpendForecast'
          THEN metric_value
        END) AS mfcSpendForecastPrepaid,

    SUM(CASE
          WHEN lob IN ('POSTPAID', 'BROADBAND', 'TFB', 'CONSUMER PREPAID')
           AND metric_name = 'mfcSpendForecast'
          THEN metric_value
        END) AS mfcSpendForecastTotal

  FROM MfcBase

  WHERE lob IN ('POSTPAID', 'BROADBAND', 'TFB', 'CONSUMER PREPAID')

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

  WHERE channel_group IN (SELECT channel_group FROM MainChannelGroups)
),


/* ===============================================================================================
   PLATFORM SPEND

   Total = POSTPAID + BROADBAND
   =============================================================================================== */

Platform AS (

  SELECT
    qgp_date,
    channel_group,

    SUM(CASE
          WHEN lob = 'POSTPAID'
           AND metric_name = 'platformSpend'
          THEN metric_value
        END) AS platformSpendPostpaid,

    SUM(CASE
          WHEN lob = 'BROADBAND'
           AND metric_name = 'platformSpend'
          THEN metric_value
        END) AS platformSpendBroadband,

    SUM(CASE
          WHEN lob IN ('POSTPAID', 'BROADBAND')
           AND metric_name = 'platformSpend'
          THEN metric_value
        END) AS platformSpendTotal

  FROM PlatformBase

  WHERE lob IN ('POSTPAID', 'BROADBAND')

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   BIDDABLE BASE

   Silver LOB values:
     ALL
     POSTPAID
     BROADBAND
     PREPAID
     TFB

   ALL = POSTPAID + BROADBAND + PREPAID + TFB
   =============================================================================================== */

BiddableBase AS (

  SELECT
    qgp_date,

    /* Channel TOTAL rows map onto the main groups. Platform-level rows (Paid Search - Google, ...)
       are alternative views of those same totals, so they are excluded: summing them would
       double count. */
    CASE channel_group
      WHEN 'All Channels'       THEN 'All Channels'
      WHEN 'Paid Search - All'  THEN 'Paid Search'
      WHEN 'Paid Social - All'  THEN 'Paid Social'
      WHEN 'Programmatic - All' THEN 'Programmatic'
    END AS channel_group,

    metric_name,
    UPPER(TRIM(lob)) AS lob,
    metric_value

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly

  WHERE data_source = 'BIDDABLE_SPEND_CHANNEL'
    AND lob IN ('ALL', 'POSTPAID', 'BROADBAND', 'PREPAID', 'TFB')
    AND channel_group IN ('All Channels', 'Paid Search - All', 'Paid Social - All', 'Programmatic - All')
),


/* ===============================================================================================
   BIDDABLE SPEND

   biddableSpendTotal is read directly from Silver lob = ALL.
   =============================================================================================== */

Biddable AS (

  SELECT
    qgp_date,
    channel_group,

    SUM(CASE
          WHEN lob = 'POSTPAID'
           AND metric_name = 'biddableSpend'
          THEN metric_value
        END) AS biddableSpendPostpaid,

    SUM(CASE
          WHEN lob = 'BROADBAND'
           AND metric_name = 'biddableSpend'
          THEN metric_value
        END) AS biddableSpendBroadband,

    SUM(CASE
          WHEN lob = 'ALL'
           AND metric_name = 'biddableSpend'
          THEN metric_value
        END) AS biddableSpendTotal,

    SUM(CASE
          WHEN lob = 'PREPAID'
           AND metric_name = 'biddableSpend'
          THEN metric_value
        END) AS biddableSpendPrepaid,

    SUM(CASE
          WHEN lob = 'TFB'
           AND metric_name = 'biddableSpend'
          THEN metric_value
        END) AS biddableSpendTfb

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

    MAX(IF(metric_name = 'upvForecast', metric_value, NULL)) AS upvForecast,
    MAX(IF(metric_name = 'upvWebAppForecast', metric_value, NULL)) AS upvWebAppForecast

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly

  WHERE channel_group IN (SELECT channel_group FROM MainChannelGroups)

  GROUP BY
    qgp_date,
    channel_group
),


/* ===============================================================================================
   QGP SCORECARD — DATE GRAIN

   Silver metric rows are pivoted into Wide columns.

   Existing combined Phone metric:
     activationsBopis

   New component metrics:
     activationsBopisOnly
     activationsNonBopisOnly

   Each is exposed independently for:
     QGP_ACTUAL
     QGP_TARGET

   The 12 Metro/TFB QGP metrics are pivoted the same way (qgpMetro... / qgpTfb...).
   =============================================================================================== */

Qgp AS (

  SELECT
    qgp_date,

    /* -------------------------------------------------------------------------------------------
       PHONE ACTIVATIONS — EXISTING COMBINED BOPIS + NON-BOPIS
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       PHONE ACTIVATIONS — BOPIS ONLY
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'activationsBopisOnly'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpActivationsBopisOnlyActual,

    MAX(
      IF(
        metric_name = 'activationsBopisOnly'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpActivationsBopisOnlyTarget,


    /* -------------------------------------------------------------------------------------------
       PHONE ACTIVATIONS — NON-BOPIS ONLY
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'activationsNonBopisOnly'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpActivationsNonBopisOnlyActual,

    MAX(
      IF(
        metric_name = 'activationsNonBopisOnly'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpActivationsNonBopisOnlyTarget,


    /* -------------------------------------------------------------------------------------------
       NEW + AAL NO ASSISTANCE
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       STORE TRAFFIC
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       VR CALLS
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       VR CHATS
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       VR CHATS AS % OF CHAT ENGAGEMENT
       Built in the QGP Silver: VR Chats / Adobe chatEngagementVisitors (All Channels). Target is NULL.
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'vrChatsPctOfChatEngagement'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpVrChatsPctOfChatEngagementActual,

    MAX(
      IF(
        metric_name = 'vrChatsPctOfChatEngagement'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpVrChatsPctOfChatEngagementTarget,


    /* -------------------------------------------------------------------------------------------
       VR POSTPAID ACTIVATIONS
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       DIGITAL % PHONE NEW ACTS
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       DIGITAL % CONSUMER POSTPAID ACTIVATIONS
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       DIGITAL % NO ASSISTANCE
       ------------------------------------------------------------------------------------------- */

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


    /* -------------------------------------------------------------------------------------------
       DIGITAL % ASSISTANCE
       ------------------------------------------------------------------------------------------- */

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
    ,

    /* -------------------------------------------------------------------------------------------
       METROACTIVATIONS (METRO)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'metroActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpMetroActivationsActual,

    MAX(
      IF(
        metric_name = 'metroActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpMetroActivationsTarget,

    /* -------------------------------------------------------------------------------------------
       METRONETS (METRO)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'metroNets'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpMetroNetsActual,

    MAX(
      IF(
        metric_name = 'metroNets'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpMetroNetsTarget,

    /* -------------------------------------------------------------------------------------------
       METRODIGITALACTIVATIONSPHONEANDOTHER (METRO)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'metroDigitalActivationsPhoneAndOther'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpMetroDigitalActivationsPhoneAndOtherActual,

    MAX(
      IF(
        metric_name = 'metroDigitalActivationsPhoneAndOther'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpMetroDigitalActivationsPhoneAndOtherTarget,

    /* -------------------------------------------------------------------------------------------
       METRONEWACCOUNTS (METRO)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'metroNewAccounts'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpMetroNewAccountsActual,

    MAX(
      IF(
        metric_name = 'metroNewAccounts'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpMetroNewAccountsTarget,

    /* -------------------------------------------------------------------------------------------
       METROMEDIASPEND (METRO)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'metroMediaSpend'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpMetroMediaSpendActual,

    MAX(
      IF(
        metric_name = 'metroMediaSpend'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpMetroMediaSpendTarget,

    /* -------------------------------------------------------------------------------------------
       METROPSPVS (METRO)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'metroPspvs'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpMetroPspvsActual,

    MAX(
      IF(
        metric_name = 'metroPspvs'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpMetroPspvsTarget,

    /* -------------------------------------------------------------------------------------------
       TFBTOTALACTIVATIONS (TFB)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'tfbTotalActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpTfbTotalActivationsActual,

    MAX(
      IF(
        metric_name = 'tfbTotalActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpTfbTotalActivationsTarget,

    /* -------------------------------------------------------------------------------------------
       TFBPHONEACTIVATIONS (TFB)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'tfbPhoneActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpTfbPhoneActivationsActual,

    MAX(
      IF(
        metric_name = 'tfbPhoneActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpTfbPhoneActivationsTarget,

    /* -------------------------------------------------------------------------------------------
       TFBHSIACTIVATIONS (TFB)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'tfbHsiActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpTfbHsiActivationsActual,

    MAX(
      IF(
        metric_name = 'tfbHsiActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpTfbHsiActivationsTarget,

    /* -------------------------------------------------------------------------------------------
       TFBDIGITALCHANNELACTIVATIONS (TFB)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'tfbDigitalChannelActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpTfbDigitalChannelActivationsActual,

    MAX(
      IF(
        metric_name = 'tfbDigitalChannelActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpTfbDigitalChannelActivationsTarget,

    /* -------------------------------------------------------------------------------------------
       TFBECOMMERCEACTIVATIONS (TFB)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'tfbEcommerceActivations'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpTfbEcommerceActivationsActual,

    MAX(
      IF(
        metric_name = 'tfbEcommerceActivations'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpTfbEcommerceActivationsTarget,

    /* -------------------------------------------------------------------------------------------
       TFBSALESCREDIT (TFB)
       ------------------------------------------------------------------------------------------- */

    MAX(
      IF(
        metric_name = 'tfbSalesCredit'
        AND metric_type = 'QGP_ACTUAL',
        metric_value,
        NULL
      )
    ) AS qgpTfbSalesCreditActual,

    MAX(
      IF(
        metric_name = 'tfbSalesCredit'
        AND metric_type = 'QGP_TARGET',
        metric_value,
        NULL
      )
    ) AS qgpTfbSalesCreditTarget

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly

  GROUP BY
    qgp_date
),


/* ===============================================================================================
   CHANNEL SPINE

   One qgp_date x channel_group key-space across every channel-grain source.

   UNION intentionally removes duplicate keys.

   Every source is already restricted to MainChannelGroups, so the spine contains only:
     All Channels, Paid Search, Paid Social, Organic Search, Direct, Programmatic, Other

   Biddable's channel totals are mapped onto those groups; its platform-level groups are excluded.
   =============================================================================================== */

ChannelSpine AS (

  SELECT qgp_date, channel_group
  FROM Adobe
  WHERE channel_group IS NOT NULL

  UNION

  SELECT qgp_date, channel_group
  FROM Mfc
  WHERE channel_group IS NOT NULL

  UNION

  SELECT qgp_date, channel_group
  FROM Platform
  WHERE channel_group IS NOT NULL

  UNION

  SELECT qgp_date, channel_group
  FROM Biddable
  WHERE channel_group IS NOT NULL

  UNION

  SELECT qgp_date, channel_group
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
     ADOBE C2C / CHAT ENGAGEMENT / ELIGIBILITY CHECK (EC)

     c2cVisitors, chatEngagementVisitors : visitors
     ecCompletedPageEvents               : page EVENT count
     ecSuccessfulVisits                  : VISIT count
     The two EC metrics are different units; do not divide one by the other.
     --------------------------------------------------------------------------------------------- */

  a.c2cVisitors,
  a.chatEngagementVisitors,
  a.ecCompletedPageEvents,
  a.ecSuccessfulVisits,


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
     ADOBE METRO / TFB mPULSE (added 2026-11-10; CVRs added 2026-11-12)

     Volumes, then CVR = metric / the same LOB's own UPV (Metro: metroProspectVisitors, TFB:
     tfbExclSinglePageVisitors). Units differ by metric (visitors / orders / events); event-count
     CVRs can exceed 100%. Unique-visitor metrics are not additive across channel_group.
     --------------------------------------------------------------------------------------------- */

  a.metroProspectVisitors,
  a.metroHomepageEntryVisitors,
  a.metroDealsEntryVisitors,
  a.metroProductPlansEntryVisitors,
  a.metroOtherEntryVisitors,
  a.metroActivationIntentVisitors,
  a.metroCartStartVisitors,
  a.metroProspectClicktocallVisitors,
  a.metroOrderConfirmationOrders,

  a.tfbExclSinglePageVisitors,
  a.tfbLeadgenFormCompleteEvent104,
  a.tfbCreditCheckEvent104,
  a.tfbAllClicktocallEvent95,
  a.tfbAllChatEngagementEvent21,
  a.tfbCartStartsVisitors,
  a.tfbEcommOrderHitOrders,
  a.tfbClicktocallExistsVisitors,
  a.tfbChatEngagementVisitors,

  a.cvrMetroHomepageEntryVisitors,
  a.cvrMetroDealsEntryVisitors,
  a.cvrMetroProductPlansEntryVisitors,
  a.cvrMetroOtherEntryVisitors,
  a.cvrMetroActivationIntentVisitors,
  a.cvrMetroCartStartVisitors,
  a.cvrMetroProspectClicktocallVisitors,
  a.cvrMetroOrderConfirmationOrders,

  a.cvrTfbLeadgenFormCompleteEvent104,
  a.cvrTfbCreditCheckEvent104,
  a.cvrTfbAllClicktocallEvent95,
  a.cvrTfbAllChatEngagementEvent21,
  a.cvrTfbCartStartsVisitors,
  a.cvrTfbEcommOrderHitOrders,
  a.cvrTfbClicktocallExistsVisitors,
  a.cvrTfbChatEngagementVisitors,


  /* ---------------------------------------------------------------------------------------------
     MFC ACTUAL SPEND
     Total = POSTPAID + BROADBAND + TFB + CONSUMER PREPAID
     --------------------------------------------------------------------------------------------- */

  m.mfcSpendActualPostpaid,
  m.mfcSpendActualBroadband,
  m.mfcSpendActualTfb,
  m.mfcSpendActualPrepaid,
  m.mfcSpendActualTotal,


  /* ---------------------------------------------------------------------------------------------
     MFC FORECAST SPEND
     Total = POSTPAID + BROADBAND + TFB + CONSUMER PREPAID
     --------------------------------------------------------------------------------------------- */

  m.mfcSpendForecastPostpaid,
  m.mfcSpendForecastBroadband,
  m.mfcSpendForecastTfb,
  m.mfcSpendForecastPrepaid,
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
           = POSTPAID + BROADBAND + PREPAID + TFB
     --------------------------------------------------------------------------------------------- */

  b.biddableSpendPostpaid,
  b.biddableSpendBroadband,
  b.biddableSpendTotal,
  b.biddableSpendPrepaid,
  b.biddableSpendTfb,


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

  q.qgpActivationsBopisOnlyActual,
  q.qgpActivationsBopisOnlyTarget,

  q.qgpActivationsNonBopisOnlyActual,
  q.qgpActivationsNonBopisOnlyTarget,

  q.qgpActivationsNewAalNoAssistanceActual,
  q.qgpActivationsNewAalNoAssistanceTarget,

  q.qgpStoreTrafficActual,
  q.qgpStoreTrafficTarget,

  q.qgpVrCallsActual,
  q.qgpVrCallsTarget,

  q.qgpVrChatsActual,
  q.qgpVrChatsTarget,

  q.qgpVrChatsPctOfChatEngagementActual,
  q.qgpVrChatsPctOfChatEngagementTarget,

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
     QGP SCORECARD - METRO / TFB (added 2026-11-10)

     Date-level values repeated across channel_group rows. Do not aggregate across channel_group.
     qgpMetroPspvsTarget is NULL in the feed.
     --------------------------------------------------------------------------------------------- */

  q.qgpMetroActivationsActual,
  q.qgpMetroActivationsTarget,

  q.qgpMetroNetsActual,
  q.qgpMetroNetsTarget,

  q.qgpMetroDigitalActivationsPhoneAndOtherActual,
  q.qgpMetroDigitalActivationsPhoneAndOtherTarget,

  q.qgpMetroNewAccountsActual,
  q.qgpMetroNewAccountsTarget,

  q.qgpMetroMediaSpendActual,
  q.qgpMetroMediaSpendTarget,

  q.qgpMetroPspvsActual,
  q.qgpMetroPspvsTarget,

  q.qgpTfbTotalActivationsActual,
  q.qgpTfbTotalActivationsTarget,

  q.qgpTfbPhoneActivationsActual,
  q.qgpTfbPhoneActivationsTarget,

  q.qgpTfbHsiActivationsActual,
  q.qgpTfbHsiActivationsTarget,

  q.qgpTfbDigitalChannelActivationsActual,
  q.qgpTfbDigitalChannelActivationsTarget,

  q.qgpTfbEcommerceActivationsActual,
  q.qgpTfbEcommerceActivationsTarget,

  q.qgpTfbSalesCreditActual,
  q.qgpTfbSalesCreditTarget,


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

  a.cvrC2c,
  a.cvrChatEngagement,
  a.cvrEcCompleted,
  a.cvrEcSuccessful,

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

LEFT JOIN CalendarMeta cal
  ON cal.qgp_date = spine.qgp_date

LEFT JOIN Adobe a
  ON  a.qgp_date = spine.qgp_date
  AND a.channel_group = spine.channel_group

LEFT JOIN Mfc m
  ON  m.qgp_date = spine.qgp_date
  AND m.channel_group = spine.channel_group

LEFT JOIN Platform p
  ON  p.qgp_date = spine.qgp_date
  AND p.channel_group = spine.channel_group

LEFT JOIN Biddable b
  ON  b.qgp_date = spine.qgp_date
  AND b.channel_group = spine.channel_group

LEFT JOIN UpvForecast uf
  ON  uf.qgp_date = spine.qgp_date
  AND uf.channel_group = spine.channel_group

LEFT JOIN Qgp q
  ON q.qgp_date = spine.qgp_date

ORDER BY
  spine.qgp_date DESC,
  spine.channel_group ASC
;