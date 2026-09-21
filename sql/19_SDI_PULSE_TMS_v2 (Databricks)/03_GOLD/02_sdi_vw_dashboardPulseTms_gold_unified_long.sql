/* =================================================================================================
FILE:         03_sdi_vw_dashboardPulseTms_gold_unified_long.sql
PLATFORM:     Databricks
LAYER:        Gold View
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_unified_long

PURPOSE:
  Final unified long-format Gold view for the PulseTMS pipeline.

  This view is the production Tableau-facing unified dataset.

  It assembles supported Silver outputs into one common schema using UNION ALL.

  Source-specific calculations such as:
    - QGP period construction
    - Quarter-boundary proration
    - WoW
    - YoY
    - Adobe CVR
    - UPV forecast channel allocation
    - Spend aggregation
    - QGP business metric construction
    - Biddable LOB reporting rollups
    - Biddable channel/platform reporting selections

  are completed upstream.

  Gold performs only the conformance required to assemble those outputs.

STRUCTURE:

  CTE 1 — AdobeVolume
    data_source = 'ADOBE'

  CTE 2 — MfcChannel
    data_source = 'MFC_SPEND_CHANNEL'

  CTE 3 — MfcGranular
    data_source = 'MFC_SPEND_GRANULAR'

  CTE 4 — PlatformSpend
    data_source = 'PLATFORM_SPEND_CHANNEL'

  CTE 5 — UpvForecast
    data_source = 'UPV_FORECAST'

  CTE 6 — QgpScorecard
    data_source = 'QGP_SCORECARD'

  CTE 7 — BiddableSpend
    data_source = 'BIDDABLE_SPEND_CHANNEL'

  UnifiedPulseTms
    UNION ALL of the seven conformed CTEs.


===================================================================================================
BIDDABLE SOURCE ARCHITECTURE
===================================================================================================

BIDDABLE_SPEND_CHANNEL represents:

  Programmatic
  Paid Social
  Paid Search

Upstream primary sources are:

  Programmatic
    prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr

  Paid Social
    prdrzranalytics.lab42.media_analytics_integrated_snapshot

  Paid Search
    prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily

Raw-source logic is intentionally NOT repeated in Gold.

Bronze handles:
  - Source selection
  - Source LOB selection
  - Platform normalization
  - Atomic weekly spend

Silver handles:
  - LOB canonicalization
  - Biddable LOB = ALL rollup
  - Channel-total reporting selections
  - Platform-level reporting selections
  - All Channels rollup
  - QGP alignment
  - Quarter-boundary proration
  - WoW
  - YoY

Gold only conforms the Silver output into the common unified schema.


---------------------------------------------------------------------------------------------------
BIDDABLE SILVER LOB
---------------------------------------------------------------------------------------------------

Silver canonical atomic LOB values:

  POSTPAID
  BROADBAND
  FIBER

Silver also creates:

  ALL
    = POSTPAID + BROADBAND + FIBER

Therefore Silver Biddable LOB values are:

  ALL
  POSTPAID
  BROADBAND
  FIBER

Fiber currently originates from Paid Search only.

Programmatic currently contributes:
  POSTPAID
  BROADBAND

Paid Social currently contributes:
  POSTPAID
  BROADBAND

Paid Search currently contributes:
  POSTPAID
  BROADBAND
  FIBER


---------------------------------------------------------------------------------------------------
BIDDABLE GOLD LOB / TRUE_LOB
---------------------------------------------------------------------------------------------------

For BIDDABLE_SPEND_CHANNEL:

  lob
    = 'Biddable'

  true_lob
    = Silver Biddable reporting LOB

Therefore:

  lob        true_lob
  ---------- ----------
  Biddable   ALL
  Biddable   POSTPAID
  Biddable   BROADBAND
  Biddable   FIBER

Meaning:

  ALL
    = POSTPAID + BROADBAND + FIBER

  POSTPAID
    = Postpaid Biddable spend only

  BROADBAND
    = Broadband / HSI Biddable spend only

  FIBER
    = Fiber Biddable spend only

IMPORTANT:

  For Biddable, true_lob is a reporting-selection dimension.

  ALL is synthetic and differs from the ordinary literal/canonical true_lob
  behavior used by several other data sources.

  ALL, POSTPAID, BROADBAND, and FIBER are alternative reporting selections.

  Do NOT aggregate:

    ALL
      +
    POSTPAID
      +
    BROADBAND
      +
    FIBER


---------------------------------------------------------------------------------------------------
BIDDABLE CHANNEL_GROUP
---------------------------------------------------------------------------------------------------

BIDDABLE_SPEND_CHANNEL may contain:

  All Channels

  Paid Search - All
  Paid Search - Google
  Paid Search - Bing

  Paid Social - All
  Paid Social - <platform>

  Programmatic - All
  Programmatic - <platform>

Platform-specific values are driven by the approved upstream source universe.

IMPORTANT:

  Channel totals and platform selections are alternative reporting views.

  Paid Search - All already contains its qualifying Paid Search platforms.

  Do NOT add:

    Paid Search - All
      +
    Paid Search - Google
      +
    Paid Search - Bing

Likewise:

  All Channels

already contains the approved Programmatic + Paid Social + Paid Search spend
for the selected Biddable true_lob.


===================================================================================================
QGP_SCORECARD
===================================================================================================

QGP_SCORECARD contains no channel_group dimension.

channel_group is therefore NULL.

QGP business metric construction occurs in Silver.

The Gold QGP view now includes the two additional Phone activation components:

  activationsBopisOnly
  activationsNonBopisOnly

The existing:

  activationsBopis

remains unchanged and continues to represent the existing combined Phone
BOPIS + Non-BOPIS metric.

Silver carries both:

  QGP_ACTUAL
  QGP_TARGET

for all three metric_names.

Gold performs no BOPIS / Non-BOPIS calculation.

Display:
  lob = 'Postpaid + Broadband'

true_lob:
  derived by metric_name only when literal LOB classification is confirmed.


===================================================================================================
DATA_SOURCE VALUES
===================================================================================================

  ADOBE
  MFC_SPEND_CHANNEL
  MFC_SPEND_GRANULAR
  PLATFORM_SPEND_CHANNEL
  BIDDABLE_SPEND_CHANNEL
  UPV_FORECAST
  QGP_SCORECARD


===================================================================================================
IMPORTANT MFC NOTE
===================================================================================================

MFC is represented twice:

  MFC_SPEND_CHANNEL
  MFC_SPEND_GRANULAR

They represent the same spend at different grains.

Do NOT aggregate both together unless intentionally analyzing both grain representations.


===================================================================================================
CHANNEL_GROUP VALUES
===================================================================================================

Common channel groups may include:

  All Channels
  Paid Search
  Paid Social
  Organic Search
  Direct
  Programmatic
  Other

PLATFORM_SPEND_CHANNEL may additionally include:

  iSpot National TV
  iSpot OTT
  Affiliate

BIDDABLE_SPEND_CHANNEL may include:

  All Channels

  Paid Search - All
  Paid Search - <platform>

  Paid Social - All
  Paid Social - <platform>

  Programmatic - All
  Programmatic - <platform>

QGP_SCORECARD:
  channel_group = NULL


===================================================================================================
LOB / TRUE_LOB
===================================================================================================

lob:
  Broad business-facing LOB / metric-family grouping used by Gold.

true_lob:
  Source-specific canonical or reporting LOB where supported.

  For most sources:
    literal or canonical row-level LOB.

  For Biddable:
    reporting LOB selection:
      ALL
      POSTPAID
      BROADBAND
      FIBER

true_lob is NULL when:
  - no literal or supported reporting LOB exists
  - a metric does not have a valid LOB concept
  - mapping is not confirmed


===================================================================================================
CANONICAL / REPORTING LOB VALUES
===================================================================================================

POSTPAID

  MFC:
    CONSUMER POSTPAID
    POSTPAID

  Platform:
    POSTPAID

  Biddable Silver:
    POSTPAID

  Biddable Gold:
    lob      = Biddable
    true_lob = POSTPAID

  QGP:
    supported Postpaid-specific metrics map to true_lob = POSTPAID


BROADBAND

  MFC:
    HSI
    BROADBAND

  Platform:
    BROADBAND

  Biddable Silver:
    HSI / BROADBAND
      -> BROADBAND

  Biddable Gold:
    lob      = Biddable
    true_lob = BROADBAND


FIBER

  Biddable Silver:
    FIBER

  Biddable Gold:
    lob      = Biddable
    true_lob = FIBER

  Fiber currently originates from Paid Search only.


ALL

  Biddable only.

  Synthetic reporting value:

    POSTPAID + BROADBAND + FIBER

  Biddable Gold:
    lob      = Biddable
    true_lob = ALL


TFB

  MFC:
    TFB
    TBG


Postpaid + Broadband

  Display-only broad value used for:
    ADOBE
    UPV_FORECAST
    QGP_SCORECARD


===================================================================================================
ADOBE LOB
===================================================================================================

lob      = 'Postpaid + Broadband'
true_lob = NULL

Adobe source-level metrics retain their detailed Postpaid / HSI / BYOD distinctions
through metric_name.


===================================================================================================
UPV FORECAST LOB
===================================================================================================

lob      = 'Postpaid + Broadband'
true_lob = NULL


===================================================================================================
MFC / PLATFORM LOB
===================================================================================================

MFC and Platform Spend contain actual row-level LOB values.

Therefore:

  true_lob = canonical row-level LOB


===================================================================================================
BIDDABLE LOB
===================================================================================================

Biddable uses:

  lob = 'Biddable'

and:

  true_lob =
    ALL
    POSTPAID
    BROADBAND
    FIBER


===================================================================================================
QGP TRUE_LOB
===================================================================================================

POSTPAID:

  activationsBopis
  activationsBopisOnly
  activationsNonBopisOnly
  activationsNewAalNoAssistance
  vrPostpaidActivations
  digitalPctPhoneNewActsNoAssistPlusAssist
  digitalPctConsumerPostpaidActivationsTotalInclAssisted
  digitalPctNoAssistanceActivations
  digitalPctAssistanceActivations

NULL:

  storeTraffic
  vrCalls
  vrChats
  unrecognized metrics


===================================================================================================
METRIC_TYPE VALUES
===================================================================================================

  ADOBE_VOLUME
  MFC_SPEND_ACTUAL
  MFC_SPEND_FORECAST
  PLATFORM_SPEND
  BIDDABLE_SPEND
  UPV_FORECAST
  QGP_ACTUAL
  QGP_TARGET


===================================================================================================
COMMON OUTPUT SCHEMA
===================================================================================================

Every CTE returns exactly 28 columns, in this order:

  1.  data_source
  2.  qgp_date
  3.  week_type
  4.  qgp_quarter
  5.  days_in_period
  6.  is_complete_period
  7.  lob
  8.  true_lob
  9.  channel_group
  10. metric_name
  11. metric_type
  12. metric_value
  13. metric_value_ly
  14. wow_numerator
  15. wow_denominator
  16. wow_pct
  17. yoy_numerator
  18. yoy_denominator
  19. yoy_pct
  20. max_date
  21. adobe_cvr_value
  22. adobe_cvr_numerator
  23. adobe_cvr_denominator
  24. mfc_channel
  25. mfc_tactic
  26. mfc_message_type
  27. mfc_agency
  28. allocation_ratio


===================================================================================================
IMPORTANT READINESS NOTE
===================================================================================================

is_complete_period is a calendar / QGP-period completeness flag.

It must NOT be interpreted as confirmation that every raw media source is fully settled.

Source settlement/readiness is monitored separately.


===================================================================================================
DOWNSTREAM
===================================================================================================

Tableau:
  Directly consumes this view.

sdi_vw_dashboardPulseTms_gold_metricAnnotated_long:
  consumes this view and joins the appendix/metric bridge.


===================================================================================================
DEPLOYMENT NOTE
===================================================================================================

QGP rows should be identified by:

  data_source = 'QGP_SCORECARD'

and not through ISNULL(lob), because QGP uses:

  lob = 'Postpaid + Broadband'


Biddable rows should be identified by:

  data_source = 'BIDDABLE_SPEND_CHANNEL'

with:

  lob = 'Biddable'

Biddable LOB selection should use:

  true_lob

================================================================================================= */


CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
AS

WITH

/* ===============================================================================================
   CTE 1: ADOBE VOLUME
   =============================================================================================== */

AdobeVolume AS (

  SELECT
    'ADOBE' AS data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Postpaid + Broadband' AS lob,
    CAST(NULL AS STRING) AS true_lob,
    s.channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    s.adobe_cvr_value,
    s.adobe_cvr_numerator,
    s.adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly s
  WHERE s.metric_type = 'ADOBE_VOLUME'
),


/* ===============================================================================================
   CTE 2: MFC SPEND — CHANNEL GRAIN
   =============================================================================================== */

MfcChannel AS (

  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,

    CASE
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('CONSUMER POSTPAID', 'POSTPAID') THEN 'POSTPAID'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('HSI', 'BROADBAND') THEN 'BROADBAND'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('TBG', 'TFB') THEN 'TFB'
      ELSE UPPER(TRIM(s.lob_mfc))
    END AS lob,

    CASE
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('CONSUMER POSTPAID', 'POSTPAID') THEN 'POSTPAID'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('HSI', 'BROADBAND') THEN 'BROADBAND'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('TBG', 'TFB') THEN 'TFB'
      ELSE UPPER(TRIM(s.lob_mfc))
    END AS true_lob,

    s.channel_group,
    s.metric_name,

    CASE s.metric_name
      WHEN 'mfcSpendActual' THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
      ELSE CAST(NULL AS STRING)
    END AS metric_type,

    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s
  WHERE s.data_source = 'MFC_SPEND_CHANNEL'
),


/* ===============================================================================================
   CTE 3: MFC SPEND — GRANULAR
   =============================================================================================== */

MfcGranular AS (

  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,

    CASE
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('CONSUMER POSTPAID', 'POSTPAID') THEN 'POSTPAID'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('HSI', 'BROADBAND') THEN 'BROADBAND'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('TBG', 'TFB') THEN 'TFB'
      ELSE UPPER(TRIM(s.lob_mfc))
    END AS lob,

    CASE
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('CONSUMER POSTPAID', 'POSTPAID') THEN 'POSTPAID'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('HSI', 'BROADBAND') THEN 'BROADBAND'
      WHEN UPPER(TRIM(s.lob_mfc)) IN ('TBG', 'TFB') THEN 'TFB'
      ELSE UPPER(TRIM(s.lob_mfc))
    END AS true_lob,

    s.channel_group,
    s.metric_name,

    CASE s.metric_name
      WHEN 'mfcSpendActual' THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
      ELSE CAST(NULL AS STRING)
    END AS metric_type,

    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    s.channel AS mfc_channel,
    s.tactic AS mfc_tactic,
    s.message_type AS mfc_message_type,
    s.agency AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s
  WHERE s.data_source = 'MFC_SPEND_GRANULAR'
),


/* ===============================================================================================
   CTE 4: PLATFORM SPEND
   =============================================================================================== */

PlatformSpend AS (

  SELECT
    'PLATFORM_SPEND_CHANNEL' AS data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    s.lob,
    s.lob AS true_lob,
    s.channel_group,
    s.metric_name,
    'PLATFORM_SPEND' AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly s
),


/* ===============================================================================================
   CTE 5: UPV FORECAST
   =============================================================================================== */

UpvForecast AS (

  SELECT
    'UPV_FORECAST' AS data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Postpaid + Broadband' AS lob,
    CAST(NULL AS STRING) AS true_lob,
    s.channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    s.allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly s
),


/* ===============================================================================================
   CTE 6: QGP SCORECARD

   Silver owns all QGP metric construction.

   New Phone activation component metrics:
     activationsBopisOnly
     activationsNonBopisOnly

   Both flow through with:
     QGP_ACTUAL
     QGP_TARGET

   Existing activationsBopis remains unchanged.
   Gold performs no calculation between the three metrics.
   =============================================================================================== */

QgpScorecard AS (

  SELECT
    'QGP_SCORECARD' AS data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Postpaid + Broadband' AS lob,

    CASE s.metric_name
      WHEN 'activationsBopis' THEN 'POSTPAID'
      WHEN 'activationsBopisOnly' THEN 'POSTPAID'
      WHEN 'activationsNonBopisOnly' THEN 'POSTPAID'
      WHEN 'activationsNewAalNoAssistance' THEN 'POSTPAID'
      WHEN 'vrPostpaidActivations' THEN 'POSTPAID'
      WHEN 'digitalPctPhoneNewActsNoAssistPlusAssist' THEN 'POSTPAID'
      WHEN 'digitalPctConsumerPostpaidActivationsTotalInclAssisted' THEN 'POSTPAID'
      WHEN 'digitalPctNoAssistanceActivations' THEN 'POSTPAID'
      WHEN 'digitalPctAssistanceActivations' THEN 'POSTPAID'
      WHEN 'storeTraffic' THEN CAST(NULL AS STRING)
      WHEN 'vrCalls' THEN CAST(NULL AS STRING)
      WHEN 'vrChats' THEN CAST(NULL AS STRING)
      ELSE CAST(NULL AS STRING)
    END AS true_lob,

    CAST(NULL AS STRING) AS channel_group,
    s.metric_name,
    s.metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly s
),


/* ===============================================================================================
   CTE 7: BIDDABLE SPEND

   Silver already owns the Biddable reporting logic.

   Silver LOB values:
     ALL        = POSTPAID + BROADBAND + FIBER
     POSTPAID
     BROADBAND
     FIBER

   Gold mapping:
     lob      = Biddable
     true_lob = Silver reporting LOB

   Silver channel_group values may include:
     All Channels
     Paid Search - All
     Paid Search - <platform>
     Paid Social - All
     Paid Social - <platform>
     Programmatic - All
     Programmatic - <platform>

   Gold performs no Biddable aggregation.
   =============================================================================================== */

BiddableSpend AS (

  SELECT
    'BIDDABLE_SPEND_CHANNEL' AS data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Biddable' AS lob,
    s.lob AS true_lob,
    s.channel_group,
    s.metric_name,
    'BIDDABLE_SPEND' AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly s

  WHERE s.data_source = 'BIDDABLE_SPEND_CHANNEL'
    AND s.lob IN ('ALL', 'POSTPAID', 'BROADBAND', 'FIBER')
),


/* ===============================================================================================
   FINAL UNIFIED DATASET

   Every CTE returns exactly 28 columns in the same positional order.

   MFC:
     MFC_SPEND_CHANNEL and MFC_SPEND_GRANULAR represent the same spend at
     different grains and should not be blindly aggregated together.

   Biddable:
     true_lob and channel_group contain alternative reporting selections.

   QGP:
     QGP metrics have channel_group = NULL.
     QGP metric definitions come directly from Silver.
   =============================================================================================== */

UnifiedPulseTms AS (

  SELECT * FROM AdobeVolume
  UNION ALL
  SELECT * FROM MfcChannel
  UNION ALL
  SELECT * FROM MfcGranular
  UNION ALL
  SELECT * FROM PlatformSpend
  UNION ALL
  SELECT * FROM UpvForecast
  UNION ALL
  SELECT * FROM QgpScorecard
  UNION ALL
  SELECT * FROM BiddableSpend
)

SELECT *
FROM UnifiedPulseTms
;


/* ===============================================================================================
FUTURE SOURCE TEMPLATE

A new source must return the exact same 28 columns, in the same order.

lob:
  - broad business-facing grouping appropriate for the source

true_lob:
  - canonical row-level LOB when genuinely available
  - confirmed metric-level mapping when applicable
  - supported source-specific reporting LOB when intentionally modeled
  - NULL when no valid LOB concept or confirmed mapping exists

If true_lob contains a synthetic reporting value such as ALL, that behavior
must be explicitly documented for that source.

=================================================================================================

NewSource AS (

  SELECT
    '<SOURCE_NAME>' AS data_source,
    CAST(s.qgp_date AS DATE) AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    <lob_expression> AS lob,
    <true_lob_expression> AS true_lob,
    s.channel_group,
    s.metric_name,
    '<METRIC_TYPE>' AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE) AS max_date,
    CAST(NULL AS DOUBLE) AS adobe_cvr_value,
    CAST(NULL AS DOUBLE) AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE) AS adobe_cvr_denominator,
    CAST(NULL AS STRING) AS mfc_channel,
    CAST(NULL AS STRING) AS mfc_tactic,
    CAST(NULL AS STRING) AS mfc_message_type,
    CAST(NULL AS STRING) AS mfc_agency,
    CAST(NULL AS DOUBLE) AS allocation_ratio

  FROM prdrzranalytics.lab42.<silver_table> s
)

Then append to UnifiedPulseTms:

  UNION ALL
  SELECT * FROM NewSource

================================================================================================= */