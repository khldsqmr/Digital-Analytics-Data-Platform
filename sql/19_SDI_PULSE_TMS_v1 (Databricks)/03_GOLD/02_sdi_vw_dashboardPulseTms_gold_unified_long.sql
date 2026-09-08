/* =================================================================================================
FILE:         03_sdi_vw_dashboardPulseTms_gold_unified_long.sql
PLATFORM:     Databricks
LAYER:        Gold View
VIEW NAME:    sdi_vw_dashboardPulseTms_gold_unified_long

PURPOSE:
  Final unified long-format Gold view for the PulseTMS pipeline.

  This view provides a single Tableau data source for PulseTMS reporting by assembling the
  supported Silver outputs into a common schema and stacking them with UNION ALL.

  This is a lightweight conformance and assembly layer. Metric-level processing such as
  proration, WoW/YoY calculations, CVR calculations, channel allocation, and source-specific
  aggregations is performed upstream in the Silver layer.

  This view performs only the conformance logic required to combine those outputs, including:
    - Standardizing source identifiers.
    - Canonicalizing selected LOB values.
    - Assigning standardized metric_type values where required.
    - Deriving the true_lob classification.
    - Adding typed NULL placeholders for source-specific columns.
    - Stacking all sources into one positional schema.

STRUCTURE:
  CTE 1 - AdobeVolume:
          Adobe funnel volume metrics and pre-computed CVR fields.
          data_source = 'ADOBE'

  CTE 2 - MfcChannel:
          MFC actual and forecast spend at LOB x channel_group grain.
          data_source = 'MFC_SPEND_CHANNEL'

  CTE 3 - MfcGranular:
          MFC actual and forecast spend at the finest available MFC grain.
          data_source = 'MFC_SPEND_GRANULAR'

  CTE 4 - PlatformSpend:
          Platform spend at LOB x channel_group grain.
          data_source = 'PLATFORM_SPEND_CHANNEL'

  CTE 5 - UpvForecast:
          Channel-allocated UPV forecast.
          data_source = 'UPV_FORECAST'

  CTE 6 - QgpScorecard:
          QGP scorecard Actual and Target rows.
          data_source = 'QGP_SCORECARD'

  CTE 7 - BiddableSpend:
          Combined Programmatic, Paid Social, and Paid Search spend at
          LOB x channel_group grain.
          data_source = 'BIDDABLE_SPEND_CHANNEL'

  Final SELECT:
          UNION ALL of the seven CTEs above.

QGP_SCORECARD NOTE:
  QGP_SCORECARD does not contain a channel_group dimension. Therefore, channel_group is NULL
  for every QGP_SCORECARD row.

  This behavior is specific to QGP_SCORECARD. Adobe and UPV Forecast contain channel-level
  rows and populate channel_group.

  QGP_SCORECARD contains the following 10 business metrics:
    - Activations BOPIS
    - Activations New/AAL No Assistance
    - Store Traffic
    - VR Calls
    - VR Chats
    - VR Postpaid Activations
    - Digital % Phone New Acts No Assist Plus Assist
    - Digital % Consumer Postpaid Activations Total Including Assisted
    - Digital % No Assistance Activations
    - Digital % Assistance Activations

  The QGP Silver source provides Actual and Target records through metric_type values of
  'QGP_ACTUAL' and 'QGP_TARGET'.

  The display lob value is 'Postpaid + Broadband' for all QGP_SCORECARD rows. The true_lob
  value is derived separately by metric_name and may be NULL where a literal LOB scope does
  not exist or has not been confirmed.

DATA_SOURCE VALUES:
  'ADOBE'
      Adobe volume and CVR rows.

  'MFC_SPEND_CHANNEL'
      MFC spend at LOB x channel_group grain, including the All Channels rollup.

  'MFC_SPEND_GRANULAR'
      MFC spend at the finest available grain. The mfc_* columns are populated.

  'PLATFORM_SPEND_CHANNEL'
      Platform spend at LOB x channel_group grain.

  'BIDDABLE_SPEND_CHANNEL'
      Combined Programmatic, Paid Social, and Paid Search spend at
      LOB x channel_group grain.

      This source coexists with PLATFORM_SPEND_CHANNEL and does not replace it.

  'UPV_FORECAST'
      Channel-allocated UPV forecast.

  'QGP_SCORECARD'
      QGP scorecard Actual and Target rows. channel_group is NULL.

IMPORTANT:
  MFC contributes two separate row sets:
    - MFC_SPEND_CHANNEL
    - MFC_SPEND_GRANULAR

  Always filter data_source to the intended MFC grain before aggregating spend. Summing both
  MFC data sources together will double-count the same business spend at different grains.

CHANNEL_GROUP VALUES:
  Common channel vocabulary used where applicable:

    'All Channels'
    'Paid Search'
    'Paid Social'
    'Organic Search'
    'Direct'
    'Programmatic'
    'Other'

  PLATFORM_SPEND_CHANNEL may additionally contain:

    'iSpot National TV'
    'iSpot OTT'
    'Affiliate'

  These are paid-media channels that do not have an equivalent Adobe on-site action
  attribution category.

  BIDDABLE_SPEND_CHANNEL is limited to the scope of its three component sources and is
  expected to populate only:

    'All Channels'
    'Paid Search'
    'Paid Social'
    'Programmatic'

  Organic Search and Direct are expected in ADOBE and UPV_FORECAST, but not in spend sources.
  These channels represent unpaid traffic. UPV_FORECAST inherits Adobe's channel vocabulary
  because its allocation is based on the prior-year Adobe channel mix.

  QGP_SCORECARD always has channel_group = NULL because QGP does not contain a channel
  dimension.

  Differences in channel availability by data_source are expected and should not be treated
  as missing joins or incomplete data.

LOB / TRUE_LOB SPLIT:
  Two LOB columns are provided because they answer different reporting questions.

  lob:
    Business-facing display value intended for dashboard filters, labels, and presentation.

    For sources without a literal row-level LOB dimension, lob may contain a broadcast display
    label. Therefore, lob alone must not be used to determine whether a row is genuinely scoped
    to one independently filterable LOB.

  true_lob:
    Literal or confirmed LOB classification for the row.

    Use true_lob when logic needs to determine whether a row genuinely belongs to a specific
    LOB. true_lob is NULL when:
      - The source does not contain a literal LOB dimension.
      - The metric has no applicable LOB concept.
      - The metric-to-LOB mapping has not yet been confirmed.

CANONICAL LOB VALUES:
  The following canonical values are used in the lob column:

  'POSTPAID'
      MFC raw values:
        - CONSUMER POSTPAID
        - POSTPAID

      Platform:
        - POSTPAID

      Biddable raw value:
        - POSTPAID

  'BROADBAND'
      MFC raw values:
        - HSI
        - BROADBAND

      Platform:
        - BROADBAND

      Biddable raw value:
        - HSI

  'TFB'
      MFC raw values:
        - TFB
        - TBG

      TBG is treated as a legacy representation of TFB.

  'Postpaid + Broadband'
      Used as the display lob for:
        - ADOBE
        - UPV_FORECAST
        - QGP_SCORECARD

      The mixed-case format is intentional and should remain unchanged unless the dashboard
      display standard is formally updated.

ADOBE LOB BEHAVIOR:
  Every ADOBE row receives:

    lob      = 'Postpaid + Broadband'
    true_lob = NULL

  The lob value is a static reporting label. It is not derived from metric_name and does not
  imply that Adobe values can be summed across Postpaid and Broadband.

  Adobe upv, cartstart, and orders metrics represent visitor or event-based measures that may
  overlap across classifications. metric_name continues to preserve the detailed Postpaid,
  HSI, BYOD, and Total distinctions provided by the Silver source.

  Although at least one component mapping has been identified, the complete metric_name to
  true_lob mapping is not yet finalized. Therefore, this view deliberately avoids assigning
  partial or assumed Adobe true_lob values.

UPV_FORECAST LOB BEHAVIOR:
  Every UPV_FORECAST row receives:

    lob      = 'Postpaid + Broadband'
    true_lob = NULL

  UPV_FORECAST forecasts upvTotalAdobe and follows the current Adobe display treatment.
  Its true_lob should remain NULL until the corresponding Adobe LOB treatment is finalized.

MFC / PLATFORM / BIDDABLE LOB BEHAVIOR:
  The following sources contain a real row-level LOB dimension:

    - MFC_SPEND_CHANNEL
    - MFC_SPEND_GRANULAR
    - PLATFORM_SPEND_CHANNEL
    - BIDDABLE_SPEND_CHANNEL

  For these sources:

    true_lob = lob

  The same canonicalization is applied to both columns where source values require
  standardization.

QGP_SCORECARD TRUE_LOB MAPPING:
  QGP_SCORECARD uses a common display value:

    lob = 'Postpaid + Broadband'

  true_lob is assigned by metric_name as follows:

    activationsBopis
      -> 'POSTPAID'

    activationsNewAalNoAssistance
      -> 'POSTPAID'

    vrPostpaidActivations
      -> 'POSTPAID'

    digitalPctPhoneNewActsNoAssistPlusAssist
      -> 'POSTPAID'

    digitalPctConsumerPostpaidActivationsTotalInclAssisted
      -> 'POSTPAID'

    digitalPctNoAssistanceActivations
      -> 'POSTPAID'

    digitalPctAssistanceActivations
      -> 'POSTPAID'

    storeTraffic
      -> NULL
         Store Traffic is retail-wide and does not have a literal LOB classification.

    vrCalls
      -> NULL
         The available scope description is ambiguous and does not support a confirmed
         literal LOB classification.

    vrChats
      -> NULL
         The available scope description is ambiguous and does not support a confirmed
         literal LOB classification.

  Any unrecognized QGP metric_name also receives true_lob = NULL by default.

NULL HANDLING:
  A NULL true_lob value is valid and expected for multiple sources and metrics. It must not
  automatically be treated as a data-quality issue.

  QGP_SCORECARD no longer has a NULL lob value. Its display lob is now
  'Postpaid + Broadband'. However, true_lob remains NULL for QGP metrics without a confirmed
  or applicable literal LOB classification.

METRIC_TYPE VALUES:
  'ADOBE_VOLUME'
      Raw Adobe funnel metrics, including upv*, cartstart*, and orders*.

  'MFC_SPEND_ACTUAL'
      MFC actual spend.

  'MFC_SPEND_FORECAST'
      MFC forecast spend.

  'PLATFORM_SPEND'
      Platform actual spend. This source does not include a forecast metric.

  'BIDDABLE_SPEND'
      Biddable actual spend. This source does not include a forecast metric.

  'UPV_FORECAST'
      UPV forecast rows, including upvForecast and upvWebAppForecast where supplied by Silver.
      allocation_ratio identifies the channel allocation share.

  'QGP_ACTUAL'
      QGP scorecard actual value.

  'QGP_TARGET'
      QGP scorecard target or plan value.

COLUMN SCHEMA:
  data_source
      Source and grain identifier.

  qgp_date
      QGP reporting period-end date. Typically Saturday, or a quarter-boundary date for
      boundary periods.

  week_type
      Expected values:
        - 'NORMAL'
        - 'BOUNDARY_STUB'
        - 'BOUNDARY_FIRST'

  qgp_quarter
      Display quarter, such as '2026 Q1'.

  days_in_period
      Number of days represented by the row:
        - 7 for NORMAL
        - Less than 7 for BOUNDARY_STUB
        - Remaining days for BOUNDARY_FIRST

  is_complete_period
      Indicates whether the reporting period is complete according to the upstream Silver
      processing.

  lob
      Business-facing display LOB. See LOB / TRUE_LOB SPLIT.

  true_lob
      Literal or confirmed row-level LOB classification. NULL where no literal classification
      exists or where the mapping has not been confirmed.

  channel_group
      Standardized channel group where applicable. NULL for QGP_SCORECARD.

  metric_name
      Camel-case metric identifier.

  metric_type
      Standardized metric category.

  metric_value
      Current-period volume, spend, forecast, actual, or target value.

  metric_value_ly
      Prior-year value where available.

  wow_numerator
      Week-over-week numerator. NULL for BOUNDARY_STUB rows.

  wow_denominator
      Week-over-week denominator. NULL for BOUNDARY_STUB rows.

  wow_pct
      Week-over-week percentage. NULL for BOUNDARY_STUB rows or when the denominator is zero.

  yoy_numerator
      Year-over-year numerator. NULL for BOUNDARY_STUB rows.

  yoy_denominator
      Year-over-year denominator. NULL for BOUNDARY_STUB rows.

  yoy_pct
      Year-over-year percentage. NULL for BOUNDARY_STUB rows or when the denominator is zero.

  max_date
      Most recent qgp_date with a non-NULL metric_value, as calculated by the Silver source.

  adobe_cvr_value
      Pre-computed Adobe CVR. Populated for ADOBE rows only.

  adobe_cvr_numerator
      Adobe CVR numerator. Populated for ADOBE rows only.

  adobe_cvr_denominator
      Adobe CVR denominator. Populated for ADOBE rows only.

  mfc_channel
      Detailed MFC channel. Populated for MFC_SPEND_GRANULAR rows only.

  mfc_tactic
      Detailed MFC tactic. Populated for MFC_SPEND_GRANULAR rows only.

  mfc_message_type
      Detailed MFC message type. Populated for MFC_SPEND_GRANULAR rows only.

  mfc_agency
      Detailed MFC agency. Populated for MFC_SPEND_GRANULAR rows only.

  allocation_ratio
      UPV forecast channel-allocation ratio based on the prior-year same-quarter Adobe channel
      mix. Populated for UPV_FORECAST rows only.

DOWNSTREAM:
  Tableau:
    Direct connection to this unified view.

  sdi_vw_dashboardPulseTms_gold_metricAnnotated_long:
    Consumes this view and joins it to the appendix bridge for Genie and agentic-SQL use.

    The bridge join uses the display lob column rather than true_lob. No structural join change
    is required for the addition of true_lob. However, downstream results may reflect the
    updated QGP_SCORECARD lob value because this unified view now supplies
    'Postpaid + Broadband' instead of NULL for those rows.

FUTURE SOURCES:
  To add another source:
    1. Add a named CTE before the final SELECT.
    2. Return the same columns in the same order and with compatible data types.
    3. Add one UNION ALL line to the final SELECT.
    4. Set true_lob equal to canonical lob only when the new source has a real row-level LOB
       dimension.
    5. Otherwise, set true_lob to a typed NULL until a valid mapping is available.

PORTING NOTES:
  BigQuery to Databricks type conversion used in this view:

    FLOAT64 -> DOUBLE

  The remaining logic uses Databricks-compatible CAST, CASE, CTE, and UNION ALL syntax.

IMPLEMENTATION HISTORY:
  - Added UPV_FORECAST as a unified source.
  - Added allocation_ratio to the common schema.
  - Added QGP_SCORECARD Actual and Target rows.
  - Enabled PLATFORM_SPEND_CHANNEL and UPV_FORECAST after their Silver processes became
    available.
  - Added BIDDABLE_SPEND_CHANNEL as a separate source that coexists with Platform spend.
  - Added true_lob to distinguish display LOB values from literal row-level LOB scope.
  - Added canonical LOB handling for MFC and Biddable source values.
  - Updated QGP_SCORECARD lob from NULL to 'Postpaid + Broadband'.
  - Added metric-level QGP true_lob classification.
  - Retained NULL true_lob for Adobe, UPV Forecast, and unresolved or non-LOB QGP metrics.

DEPLOYMENT CONSIDERATION:
  The QGP_SCORECARD lob change is a value-level behavioral change:

    Previous value: NULL
    Current value:  'Postpaid + Broadband'

  Before deployment, review downstream Tableau calculations and filters for logic that uses
  ISNULL([lob]) to identify QGP rows. QGP rows should be identified using:

    data_source = 'QGP_SCORECARD'

  rather than relying on a NULL lob value.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
AS

WITH

-- =============================================================================
-- CTE 1: ADOBE VOLUME
--
-- Grain:
--   qgp_date x channel_group x metric_name
--
-- LOB treatment:
--   lob      = 'Postpaid + Broadband'
--   true_lob = NULL until the complete Adobe metric-to-LOB mapping is finalized
--
-- metric_type:
--   'ADOBE_VOLUME'
-- =============================================================================
AdobeVolume AS (
  SELECT
    'ADOBE'                                      AS data_source,
    CAST(s.qgp_date AS DATE)                     AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Postpaid + Broadband'                       AS lob,
    CAST(NULL AS STRING)                         AS true_lob,
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
    CAST(s.max_date AS DATE)                     AS max_date,
    s.adobe_cvr_value,
    s.adobe_cvr_numerator,
    s.adobe_cvr_denominator,
    CAST(NULL AS STRING)                         AS mfc_channel,
    CAST(NULL AS STRING)                         AS mfc_tactic,
    CAST(NULL AS STRING)                         AS mfc_message_type,
    CAST(NULL AS STRING)                         AS mfc_agency,
    CAST(NULL AS DOUBLE)                         AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly s
  WHERE s.metric_type = 'ADOBE_VOLUME'
),

-- =============================================================================
-- CTE 2: MFC SPEND, CHANNEL GRAIN
--
-- Contains MFC actual and forecast spend at LOB x channel_group grain,
-- including the All Channels rollup.
--
-- Both lob and true_lob use the same canonicalized row-level MFC LOB.
--
-- metric_type:
--   'MFC_SPEND_ACTUAL'
--   'MFC_SPEND_FORECAST'
-- =============================================================================
MfcChannel AS (
  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE)                     AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,

    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                           AS lob,

    -- Duplicated because a SELECT-list alias cannot be reused by another
    -- expression in the same SELECT list.
    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                           AS true_lob,

    s.channel_group,
    s.metric_name,

    CASE s.metric_name
      WHEN 'mfcSpendActual'   THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
    END                                           AS metric_type,

    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                     AS max_date,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                         AS mfc_channel,
    CAST(NULL AS STRING)                         AS mfc_tactic,
    CAST(NULL AS STRING)                         AS mfc_message_type,
    CAST(NULL AS STRING)                         AS mfc_agency,
    CAST(NULL AS DOUBLE)                         AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s
  WHERE s.data_source = 'MFC_SPEND_CHANNEL'
),

-- =============================================================================
-- CTE 3: MFC SPEND, GRANULAR GRAIN
--
-- Contains MFC actual and forecast spend at the finest available MFC grain.
-- The mfc_* dimension columns are populated.
--
-- Both lob and true_lob use the same canonicalized row-level MFC LOB.
--
-- metric_type:
--   'MFC_SPEND_ACTUAL'
--   'MFC_SPEND_FORECAST'
-- =============================================================================
MfcGranular AS (
  SELECT
    s.data_source,
    CAST(s.qgp_date AS DATE)                     AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,

    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                           AS lob,

    -- Duplicated because a SELECT-list alias cannot be reused by another
    -- expression in the same SELECT list.
    CASE s.lob_mfc
      WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
      WHEN 'POSTPAID'          THEN 'POSTPAID'
      WHEN 'HSI'               THEN 'BROADBAND'
      WHEN 'BROADBAND'         THEN 'BROADBAND'
      WHEN 'TBG'               THEN 'TFB'
      WHEN 'TFB'               THEN 'TFB'
      ELSE s.lob_mfc
    END                                           AS true_lob,

    s.channel_group,
    s.metric_name,

    CASE s.metric_name
      WHEN 'mfcSpendActual'   THEN 'MFC_SPEND_ACTUAL'
      WHEN 'mfcSpendForecast' THEN 'MFC_SPEND_FORECAST'
    END                                           AS metric_type,

    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                     AS max_date,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_denominator,
    s.channel                                    AS mfc_channel,
    s.tactic                                     AS mfc_tactic,
    s.message_type                               AS mfc_message_type,
    s.agency                                     AS mfc_agency,
    CAST(NULL AS DOUBLE)                         AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s
  WHERE s.data_source = 'MFC_SPEND_GRANULAR'
),

-- =============================================================================
-- CTE 4: PLATFORM SPEND
--
-- Platform paid-media spend at LOB x channel_group grain.
-- Both POSTPAID and BROADBAND are retained in this long-format view.
--
-- No additional LOB filter is applied here.
--
-- true_lob mirrors lob because Platform contains a real row-level LOB dimension.
--
-- metric_type:
--   'PLATFORM_SPEND'
-- =============================================================================
PlatformSpend AS (
  SELECT
    'PLATFORM_SPEND_CHANNEL'                     AS data_source,
    CAST(s.qgp_date AS DATE)                     AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    s.lob,
    s.lob                                        AS true_lob,
    s.channel_group,
    s.metric_name,
    'PLATFORM_SPEND'                             AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                     AS max_date,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                         AS mfc_channel,
    CAST(NULL AS STRING)                         AS mfc_tactic,
    CAST(NULL AS STRING)                         AS mfc_message_type,
    CAST(NULL AS STRING)                         AS mfc_agency,
    CAST(NULL AS DOUBLE)                         AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly s
),

-- =============================================================================
-- CTE 5: UPV FORECAST
--
-- UPV forecast allocated by channel using the prior-year same-quarter Adobe
-- channel mix.
--
-- LOB treatment:
--   lob      = 'Postpaid + Broadband'
--   true_lob = NULL
--
-- true_lob remains NULL because this source forecasts upvTotalAdobe and follows
-- Adobe's unresolved literal LOB treatment.
--
-- metric_type:
--   Passed through from Silver. Expected value is 'UPV_FORECAST'.
-- =============================================================================
UpvForecast AS (
  SELECT
    'UPV_FORECAST'                               AS data_source,
    CAST(s.qgp_date AS DATE)                     AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Postpaid + Broadband'                       AS lob,
    CAST(NULL AS STRING)                         AS true_lob,
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
    CAST(s.max_date AS DATE)                     AS max_date,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                         AS mfc_channel,
    CAST(NULL AS STRING)                         AS mfc_tactic,
    CAST(NULL AS STRING)                         AS mfc_message_type,
    CAST(NULL AS STRING)                         AS mfc_agency,
    s.allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly s
),

-- =============================================================================
-- CTE 6: QGP SCORECARD
--
-- Contains 10 QGP business metrics with Actual and Target records where supplied
-- by the Silver source.
--
-- Grain:
--   qgp_date x metric_name x metric_type
--
-- channel_group:
--   NULL because QGP does not contain a channel dimension.
--
-- LOB treatment:
--   lob      = 'Postpaid + Broadband'
--   true_lob = Derived by metric_name.
--
-- true_lob remains NULL for retail-wide, ambiguous, or unrecognized metrics.
--
-- metric_type:
--   'QGP_ACTUAL'
--   'QGP_TARGET'
-- =============================================================================
QgpScorecard AS (
  SELECT
    'QGP_SCORECARD'                              AS data_source,
    CAST(s.qgp_date AS DATE)                     AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,
    'Postpaid + Broadband'                       AS lob,

    CASE s.metric_name
      WHEN 'activationsBopis'
        THEN 'POSTPAID'

      WHEN 'activationsNewAalNoAssistance'
        THEN 'POSTPAID'

      WHEN 'vrPostpaidActivations'
        THEN 'POSTPAID'

      WHEN 'digitalPctPhoneNewActsNoAssistPlusAssist'
        THEN 'POSTPAID'

      WHEN 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'
        THEN 'POSTPAID'

      WHEN 'digitalPctNoAssistanceActivations'
        THEN 'POSTPAID'

      WHEN 'digitalPctAssistanceActivations'
        THEN 'POSTPAID'

      -- Retail-wide metric with no literal LOB dimension.
      WHEN 'storeTraffic'
        THEN CAST(NULL AS STRING)

      -- Scope remains unconfirmed because the available definition is ambiguous.
      WHEN 'vrCalls'
        THEN CAST(NULL AS STRING)

      -- Scope remains unconfirmed because the available definition is ambiguous.
      WHEN 'vrChats'
        THEN CAST(NULL AS STRING)

      -- Prevent an unrecognized metric from being assigned an assumed LOB.
      ELSE CAST(NULL AS STRING)
    END                                           AS true_lob,

    CAST(NULL AS STRING)                         AS channel_group,
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
    CAST(s.max_date AS DATE)                     AS max_date,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                         AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                         AS mfc_channel,
    CAST(NULL AS STRING)                         AS mfc_tactic,
    CAST(NULL AS STRING)                         AS mfc_message_type,
    CAST(NULL AS STRING)                         AS mfc_agency,
    CAST(NULL AS DOUBLE)                         AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly s
),

-- =============================================================================
-- CTE 7: BIDDABLE SPEND
--
-- Combines Programmatic, Paid Social, and Paid Search spend at
-- LOB x channel_group grain.
--
-- This source coexists with PLATFORM_SPEND_CHANNEL and does not replace it.
--
-- The Silver source contains raw LOB values. This CTE retains only:
--   POSTPAID -> POSTPAID
--   HSI      -> BROADBAND
--
-- Both lob and true_lob use the same canonicalized row-level LOB.
--
-- metric_type:
--   'BIDDABLE_SPEND'
-- =============================================================================
BiddableSpend AS (
  SELECT
    'BIDDABLE_SPEND_CHANNEL'                    AS data_source,
    CAST(s.qgp_date AS DATE)                    AS qgp_date,
    s.week_type,
    s.qgp_quarter,
    s.days_in_period,
    s.is_complete_period,

    CASE s.lob
      WHEN 'POSTPAID' THEN 'POSTPAID'
      WHEN 'HSI'      THEN 'BROADBAND'
      ELSE s.lob
    END                                          AS lob,

    -- Duplicated because a SELECT-list alias cannot be reused by another
    -- expression in the same SELECT list.
    CASE s.lob
      WHEN 'POSTPAID' THEN 'POSTPAID'
      WHEN 'HSI'      THEN 'BROADBAND'
      ELSE s.lob
    END                                          AS true_lob,

    s.channel_group,
    s.metric_name,
    'BIDDABLE_SPEND'                            AS metric_type,
    s.metric_value,
    s.metric_value_ly,
    s.wow_numerator,
    s.wow_denominator,
    s.wow_pct,
    s.yoy_numerator,
    s.yoy_denominator,
    s.yoy_pct,
    CAST(s.max_date AS DATE)                    AS max_date,
    CAST(NULL AS DOUBLE)                        AS adobe_cvr_value,
    CAST(NULL AS DOUBLE)                        AS adobe_cvr_numerator,
    CAST(NULL AS DOUBLE)                        AS adobe_cvr_denominator,
    CAST(NULL AS STRING)                        AS mfc_channel,
    CAST(NULL AS STRING)                        AS mfc_tactic,
    CAST(NULL AS STRING)                        AS mfc_message_type,
    CAST(NULL AS STRING)                        AS mfc_agency,
    CAST(NULL AS DOUBLE)                        AS allocation_ratio
  FROM prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly s
  WHERE s.lob IN ('POSTPAID', 'HSI')
    -- Other raw LOB values are intentionally excluded from this PulseTMS view.
),

-- =============================================================================
-- FINAL UNIFIED DATASET
--
-- IMPORTANT:
--   UNION ALL aligns columns by position, not by alias.
--
--   Every CTE must return the same 29 columns in the same order and with
--   compatible data types.
--
--   true_lob must remain between lob and channel_group.
-- =============================================================================
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

/*
  =============================================================================
  FUTURE SOURCE TEMPLATE

  Add the new source as a CTE before UnifiedPulseTms, then add it to the
  UnifiedPulseTms UNION ALL block.

  The new CTE must return the same columns in the same order.

  true_lob guidance:
    - Use the canonicalized row-level LOB when the source contains a real,
      independently filterable LOB dimension.
    - Use a confirmed metric-level derivation when LOB depends on metric_name.
    - Use CAST(NULL AS STRING) when the source has no literal LOB dimension or
      the mapping has not been confirmed.
  =============================================================================

  NewSource AS (
    SELECT
      '<SOURCE_NAME>'                             AS data_source,
      CAST(s.qgp_date AS DATE)                    AS qgp_date,
      s.week_type,
      s.qgp_quarter,
      s.days_in_period,
      s.is_complete_period,
      <lob_expression>                            AS lob,
      <true_lob_expression>                       AS true_lob,
      s.channel_group,
      s.metric_name,
      '<METRIC_TYPE>'                             AS metric_type,
      s.metric_value,
      s.metric_value_ly,
      s.wow_numerator,
      s.wow_denominator,
      s.wow_pct,
      s.yoy_numerator,
      s.yoy_denominator,
      s.yoy_pct,
      CAST(s.max_date AS DATE)                    AS max_date,
      CAST(NULL AS DOUBLE)                        AS adobe_cvr_value,
      CAST(NULL AS DOUBLE)                        AS adobe_cvr_numerator,
      CAST(NULL AS DOUBLE)                        AS adobe_cvr_denominator,
      CAST(NULL AS STRING)                        AS mfc_channel,
      CAST(NULL AS STRING)                        AS mfc_tactic,
      CAST(NULL AS STRING)                        AS mfc_message_type,
      CAST(NULL AS STRING)                        AS mfc_agency,
      CAST(NULL AS DOUBLE)                        AS allocation_ratio
    FROM prdrzranalytics.lab42.<silver_table> s
  )

  Then add:

    UNION ALL
    SELECT * FROM NewSource

  to the UnifiedPulseTms CTE.
  =============================================================================
*/
