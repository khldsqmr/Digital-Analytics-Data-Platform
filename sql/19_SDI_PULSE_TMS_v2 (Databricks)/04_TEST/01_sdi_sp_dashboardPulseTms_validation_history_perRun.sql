/* =================================================================================================
FILE:           sdi_sp_dashboardPulseTms_validation_history_perRun.sql
PLATFORM:       Databricks
LAYER:          Validation / Monitoring
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_dashboardPulseTms_validation_history_perRun
PURPOSE:
  Performs post-run reconciliation for Dashboard Pulse TMS.

  Every invocation:
    1. Creates the persistent history table if it does not yet exist.
    2. Identifies the latest completed weekly reporting Saturday.
    3. Reconciles selected reporting metrics:
           Source -> Bronze -> Bronze Comparable -> Silver -> Gold
    4. Normalizes values to TWO decimal places BEFORE comparison.
    5. Calculates absolute and percentage variance.
    6. Identifies the layer where a mismatch occurred.
    7. Assigns:
           Healthy
           Warning
           Failed
    8. Generates human-readable Notes and Next Step.
    9. Appends a permanent validation snapshot.

WHY "PER RUN":
  The underlying PulseTMS data is weekly-grain, but the orchestration can execute daily or
  multiple times in one day. Validation therefore represents an execution/run, not a cadence.

COMMON VALIDATION DATE:
  Uses the latest completed Saturday from:
    sdi_vw_dashboardPulseTms_dim_qgp_calendar

  Multiple executions during the same week can therefore validate the same data_as_of_date,
  while receiving different validation_run_id values.

APPLE-TO-APPLE COMPARISON:
  Source -> Bronze:
    Natural source value versus Bronze natural value.

  Bronze -> Silver:
    Uses bronze_comparable_value.

    Adobe / Platform / Biddable:
      Recreates the same quarter-boundary proration used by Silver:
        BOUNDARY_FIRST = Bronze natural week * days_in_period / 7
        NORMAL         = Bronze natural week

    MFC:
      Direct comparison because Bronze qgp_week is already aligned to QGP date.

    UPV Forecast:
      Direct comparison because Bronze is already boundary-aware/prorated upstream.

    QGP:
      Business metrics are constructed in Silver.
      Therefore:
        Source -> Bronze = source coverage reconciliation
        Silver -> Gold   = named business metric reconciliation

  Silver -> Gold:
    Direct comparison because Gold primarily conforms the Silver output.

PRECISION:
  Every comparable value is ROUND(..., 2) before calculating variance.

STATUS:
  Healthy:
    All applicable comparisons reconcile to two decimals.

  Warning:
    Any unexpected two-decimal mismatch.
    Or expected data is unavailable in both applicable layers.

  Failed:
    Required downstream value disappears while its upstream value exists.
    OR absolute unexplained percentage variance >= 25%.

  IMPORTANT:
    A validation result with Status='Failed' is DATA QUALITY status.
    It does not deliberately throw a SQL exception.
    The orchestration job only fails when the validation procedure itself encounters
    a technical execution error.

QGP NULL TARGET EXCEPTIONS:
  The following QGP target metrics are permitted to be NULL in both Silver and Gold:

    activationsBopisOnly
    activationsNonBopisOnly
      -> source component target may legitimately not exist.

    digitalPctNoAssistanceActivations
    digitalPctAssistanceActivations
      -> confirmed source QGP target absent / Silver intentionally returns NULL.

================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_dashboardPulseTms_validation_history_perRun()

LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA

AS
BEGIN

  /* ===============================================================================================
     STEP 0 — CREATE PERSISTENT HISTORY TABLE IF NEEDED

     This keeps table DDL owned by the validation procedure.

     NOTE:
       IF NOT EXISTS will not evolve an already-created table.
       Future schema changes should use ALTER TABLE.
     =============================================================================================== */

  CREATE TABLE IF NOT EXISTS
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun
  (
    validation_run_id                  STRING,
    validation_run_ts                  TIMESTAMP,

    data_as_of_date                    DATE,
    week_type                          STRING,
    days_in_period                     INT,

    data_source                        STRING,
    metric_name                        STRING,
    metric_type                        STRING,

    comparison_scope                   STRING,
    comparison_method                  STRING,

    /* ---------------------------------------------------------------------------------------------
       VALUES
       ------------------------------------------------------------------------------------------- */

    source_value                       DECIMAL(38,2),

    /* Actual value physically present in Bronze. */
    bronze_value                       DECIMAL(38,2),

    /*
      Bronze value converted to the same reporting grain used by Silver.

      NORMAL:
        usually equals bronze_value.

      Adobe / Platform / Biddable BOUNDARY_FIRST:
        bronze_value * days_in_period / 7
    */
    bronze_comparable_value            DECIMAL(38,2),

    silver_value                       DECIMAL(38,2),
    gold_value                         DECIMAL(38,2),

    /* ---------------------------------------------------------------------------------------------
       SOURCE -> BRONZE
       ------------------------------------------------------------------------------------------- */

    source_bronze_variance             DECIMAL(38,2),
    source_bronze_variance_pct         DECIMAL(18,2),

    /* ---------------------------------------------------------------------------------------------
       BRONZE COMPARABLE -> SILVER
       ------------------------------------------------------------------------------------------- */

    bronze_silver_variance             DECIMAL(38,2),
    bronze_silver_variance_pct         DECIMAL(18,2),

    /* ---------------------------------------------------------------------------------------------
       SILVER -> GOLD
       ------------------------------------------------------------------------------------------- */

    silver_gold_variance               DECIMAL(38,2),
    silver_gold_variance_pct           DECIMAL(18,2),

    /*
      Warning threshold is intentionally 0.00.

      After:
        - recreating the expected transformation
        - normalizing both sides to two decimals

      any remaining mismatch is worth displaying as a Warning.
    */
    warning_threshold_pct              DECIMAL(18,2),

    /*
      Failed is reserved for a very large unexplained reconciliation difference.
    */
    critical_threshold_pct             DECIMAL(18,2),

    /*
      NONE
      SOURCE_TO_BRONZE
      BRONZE_TO_SILVER
      SILVER_TO_GOLD
      DATA_AVAILABILITY
      MULTIPLE
    */
    issue_layer                        STRING,

    /*
      Healthy
      Warning
      Failed
    */
    status                             STRING,

    notes                              STRING,
    next_step                          STRING,

    /* Debugging lineage */
    source_object                      STRING,
    bronze_object                      STRING,
    silver_object                      STRING,
    gold_object                        STRING,

    created_ts                         TIMESTAMP
  )

  USING DELTA

  CLUSTER BY (
    data_as_of_date,
    validation_run_ts,
    data_source
  )

  COMMENT
  'PulseTMS post-run validation history. One row per monitored metric per validation run. Stores Source, Bronze, Bronze comparable, Silver and Gold values normalized to two decimals, reconciliation variances, issue layer, status, notes and next step.';


  /* ===============================================================================================
     STEP 1 — APPEND CURRENT VALIDATION RUN
     =============================================================================================== */

  WITH

  /* ===============================================================================================
     RUN CONTEXT

     Since the orchestration can execute daily while processing weekly-grain data:

       validation_run_id
         = unique execution identifier

       data_as_of_date
         = latest completed Saturday being validated

     This allows:
       Monday run    -> Sep 19 data
       Tuesday rerun -> Sep 19 data
       Wednesday     -> Sep 19 data

     while each execution remains separately auditable.
     =============================================================================================== */

  RunContext AS (

    SELECT
      CONCAT(
        'PULSETMS_VAL_',
        DATE_FORMAT(
          CURRENT_TIMESTAMP(),
          'yyyyMMdd_HHmmss_SSS'
        )
      )                                                         AS validation_run_id,

      CURRENT_TIMESTAMP()                                       AS validation_run_ts,

      cal.qgp_date                                               AS data_as_of_date,
      cal.week_type                                              AS week_type,
      CAST(cal.days_in_period AS INT)                            AS days_in_period,

      CAST(0.00 AS DECIMAL(18,2))                               AS warning_threshold_pct,
      CAST(25.00 AS DECIMAL(18,2))                              AS critical_threshold_pct

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal

    WHERE
      cal.is_complete_period = TRUE

      AND cal.qgp_date <= CURRENT_DATE()

      /* Databricks DAYOFWEEK: Sunday=1 ... Saturday=7 */
      AND DAYOFWEEK(cal.qgp_date) = 7

    ORDER BY
      cal.qgp_date DESC

    LIMIT 1
  ),


  /* ################################################################################################
     ADOBE
     ################################################################################################ */


  /* ===============================================================================================
     ADOBE SOURCE — ALL CHANNELS

     These are the 14 directly sourced Adobe metrics.

     Four additional Adobe totals are derived from Bronze components:
       cartstartTotal
       ordersUnassistedTotal
       ordersAssistedTotal
       ordersTotal

     Those derived metrics therefore begin validation at Bronze rather than Source.
     =============================================================================================== */

  AdobeSourceUnion AS (

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS week_sun_sat,
      'upvPostpaid' AS metric_name,
      TRY_CAST(visitors AS DOUBLE) AS metric_value,
      'sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo' AS source_table,
      __insert_date AS insert_date,
      File_Load_datetime AS file_load_datetime,
      Filename AS filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_postpaid_flow_visitors_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'upvHsi',
      TRY_CAST(visitors AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'upvByod',
      TRY_CAST(visitors AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'upvFlowTotal',
      TRY_CAST(visitors AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'upvTotalAdobe',
      TRY_CAST(visitors AS DOUBLE),
      'sdi_raw_pp_pro_uvnb_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_pp_pro_uvnb_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'cartstartPostpaid',
      TRY_CAST(visits AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'cartstartHsi',
      TRY_CAST(visits AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'cartstartByod',
      TRY_CAST(visits AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'ordersUnassistedPostpaid',
      TRY_CAST(orders AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'ordersUnassistedHsi',
      TRY_CAST(orders AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'ordersUnassistedByod',
      TRY_CAST(orders AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'ordersAssistedPostpaid',
      TRY_CAST(orders AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'ordersAssistedHsi',
      TRY_CAST(orders AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo

    UNION ALL

    SELECT
      DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6),
      'ordersAssistedByod',
      TRY_CAST(orders AS DOUBLE),
      'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo',
      __insert_date,
      File_Load_datetime,
      Filename
    FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo
  ),


  /* Same latest-file dedup principle used by Adobe Bronze. */

  AdobeSourceDeduped AS (

    SELECT
      *

    FROM AdobeSourceUnion

    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          week_sun_sat,
          metric_name,
          source_table

        ORDER BY
          file_load_datetime DESC,
          filename DESC,
          insert_date DESC
      ) = 1
  ),


  AdobeSource AS (

    SELECT
      s.metric_name,
      MAX(s.metric_value) AS source_value

    FROM AdobeSourceDeduped s

    CROSS JOIN RunContext rc

    WHERE
      s.week_sun_sat = rc.data_as_of_date

    GROUP BY
      s.metric_name
  ),


  /* ===============================================================================================
     ADOBE BRONZE BASE
     One All Channels natural-week row.
     =============================================================================================== */

  AdobeBronzeBase AS (

    SELECT
      MAX(b.upvPostpaid)                AS upvPostpaid,
      MAX(b.upvHsi)                     AS upvHsi,
      MAX(b.upvByod)                    AS upvByod,
      MAX(b.upvFlowTotal)               AS upvFlowTotal,
      MAX(b.upvTotalAdobe)              AS upvTotalAdobe,

      MAX(b.cartstartPostpaid)          AS cartstartPostpaid,
      MAX(b.cartstartHsi)               AS cartstartHsi,
      MAX(b.cartstartByod)              AS cartstartByod,

      MAX(b.ordersUnassistedPostpaid)   AS ordersUnassistedPostpaid,
      MAX(b.ordersUnassistedHsi)        AS ordersUnassistedHsi,
      MAX(b.ordersUnassistedByod)       AS ordersUnassistedByod,

      MAX(b.ordersAssistedPostpaid)     AS ordersAssistedPostpaid,
      MAX(b.ordersAssistedHsi)          AS ordersAssistedHsi,
      MAX(b.ordersAssistedByod)         AS ordersAssistedByod

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_adobeFunnel_weekly b

    CROSS JOIN RunContext rc

    WHERE
      b.week_sun_sat = rc.data_as_of_date
      AND b.channel_group = 'All Channels'
  ),


  /* ===============================================================================================
     ADOBE BRONZE METRICS

     Includes:
       14 direct Bronze metrics
       4 derived totals built exactly from their Bronze components
     =============================================================================================== */

  AdobeBronzeMetrics AS (

    SELECT 'upvPostpaid' AS metric_name, upvPostpaid AS bronze_value
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'upvHsi', upvHsi
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'upvByod', upvByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'upvFlowTotal', upvFlowTotal
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'upvTotalAdobe', upvTotalAdobe
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'cartstartPostpaid', cartstartPostpaid
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'cartstartHsi', cartstartHsi
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'cartstartByod', cartstartByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT
      'cartstartTotal',
      cartstartPostpaid + cartstartHsi + cartstartByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'ordersUnassistedPostpaid', ordersUnassistedPostpaid
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'ordersUnassistedHsi', ordersUnassistedHsi
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'ordersUnassistedByod', ordersUnassistedByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT
      'ordersUnassistedTotal',
      ordersUnassistedPostpaid
        + ordersUnassistedHsi
        + ordersUnassistedByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'ordersAssistedPostpaid', ordersAssistedPostpaid
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'ordersAssistedHsi', ordersAssistedHsi
    FROM AdobeBronzeBase

    UNION ALL
    SELECT 'ordersAssistedByod', ordersAssistedByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT
      'ordersAssistedTotal',
      ordersAssistedPostpaid
        + ordersAssistedHsi
        + ordersAssistedByod
    FROM AdobeBronzeBase

    UNION ALL
    SELECT
      'ordersTotal',
      ordersUnassistedPostpaid
        + ordersUnassistedHsi
        + ordersUnassistedByod
        + ordersAssistedPostpaid
        + ordersAssistedHsi
        + ordersAssistedByod
    FROM AdobeBronzeBase
  ),


  /* ===============================================================================================
     ADOBE BRONZE COMPARABLE VALUE

     Recreates the relevant Silver transformation.

     Latest validation date is always a Saturday, so the possible cases are primarily:

       NORMAL
         comparable = full natural-week Bronze value

       BOUNDARY_FIRST
         comparable = Bronze * days_in_period / 7

     This prevents a false variance caused purely by comparing different grains.
     =============================================================================================== */

  AdobeBronzeComparable AS (

    SELECT
      b.metric_name,
      b.bronze_value,

      CASE
        WHEN rc.week_type = 'BOUNDARY_FIRST'
          THEN b.bronze_value * rc.days_in_period / 7.0

        ELSE b.bronze_value
      END AS bronze_comparable_value

    FROM AdobeBronzeMetrics b

    CROSS JOIN RunContext rc
  ),


  AdobeSilver AS (

    SELECT
      s.metric_name,
      MAX(s.metric_value) AS silver_value

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly s

    CROSS JOIN RunContext rc

    WHERE
      s.qgp_date = rc.data_as_of_date
      AND s.channel_group = 'All Channels'
      AND s.metric_type = 'ADOBE_VOLUME'

    GROUP BY
      s.metric_name
  ),


  AdobeGold AS (

    SELECT
      g.metric_name,
      MAX(g.metric_value) AS gold_value

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

    CROSS JOIN RunContext rc

    WHERE
      g.qgp_date = rc.data_as_of_date
      AND g.data_source = 'ADOBE'
      AND g.channel_group = 'All Channels'
      AND g.metric_type = 'ADOBE_VOLUME'

    GROUP BY
      g.metric_name
  ),


  AdobeValidation AS (

    SELECT
      'ADOBE'                                             AS data_source,

      b.metric_name,
      'ADOBE_VOLUME'                                      AS metric_type,

      CASE
        WHEN b.metric_name IN (
          'cartstartTotal',
          'ordersUnassistedTotal',
          'ordersAssistedTotal',
          'ordersTotal'
        )
          THEN 'BRONZE_TO_GOLD'

        ELSE 'SOURCE_TO_GOLD'
      END                                                 AS comparison_scope,

      CASE
        WHEN rc.week_type = 'BOUNDARY_FIRST'
          THEN 'QGP_PRORATION'
        ELSE 'DIRECT'
      END                                                 AS comparison_method,

      src.source_value,
      b.bronze_value,
      b.bronze_comparable_value,
      s.silver_value,
      g.gold_value,

      CASE
        WHEN b.metric_name IN (
          'cartstartTotal',
          'ordersUnassistedTotal',
          'ordersAssistedTotal',
          'ordersTotal'
        )
          THEN FALSE
        ELSE TRUE
      END                                                 AS check_source_bronze,

      TRUE                                                AS check_bronze_silver,
      TRUE                                                AS check_silver_gold,

      FALSE                                               AS allow_all_null,
      FALSE                                               AS require_nonzero,

      'Adobe Improvado weekly source tables'              AS source_object,
      'sdi_tbl_dashboardPulseTms_bronze_adobeFunnel_weekly'
                                                          AS bronze_object,
      'sdi_tbl_dashboardPulseTms_silver_adobeFunnel_weekly'
                                                          AS silver_object,
      'sdi_vw_dashboardPulseTms_gold_unified_long'        AS gold_object

    FROM AdobeBronzeComparable b

    LEFT JOIN AdobeSource src
      ON src.metric_name = b.metric_name

    LEFT JOIN AdobeSilver s
      ON s.metric_name = b.metric_name

    LEFT JOIN AdobeGold g
      ON g.metric_name = b.metric_name

    CROSS JOIN RunContext rc
  ),


  /* ################################################################################################
     MFC SPEND
     ################################################################################################ */


  /* ===============================================================================================
     MFC SOURCE

     Reproduce Bronze source filtering and the week-level all-or-nothing forecast fallback.
     =============================================================================================== */

  MfcSourceTyped AS (

    SELECT
      TRY_CAST(raw.QGP_Week AS DATE) AS qgp_week,

      CASE UPPER(TRIM(raw.LOB_Supported))
        WHEN 'CONSUMER POSTPAID' THEN 'POSTPAID'
        WHEN 'POSTPAID'          THEN 'POSTPAID'
        WHEN 'HSI'               THEN 'BROADBAND'
        WHEN 'BROADBAND'         THEN 'BROADBAND'
        WHEN 'TBG'               THEN 'TFB'
        WHEN 'TFB'               THEN 'TFB'
        ELSE UPPER(TRIM(raw.LOB_Supported))
      END AS lob,

      TRY_CAST(raw.spend_actual AS DOUBLE) AS spend_actual,
      TRY_CAST(raw.spend_forecast AS DOUBLE) AS spend_forecast,

      MAX(
        CASE
          WHEN TRY_CAST(raw.spend_forecast AS DOUBLE) IS NOT NULL
           AND TRY_CAST(raw.spend_forecast AS DOUBLE) != 0
            THEN 1
          ELSE 0
        END
      ) OVER (
        PARTITION BY TRY_CAST(raw.QGP_Week AS DATE)
      ) AS week_has_forecast

    FROM
      prdrzranalytics.lab42.sdi_vw_mfc_gold_spendGranular_weekly raw

    CROSS JOIN RunContext rc

    WHERE
      TRY_CAST(raw.QGP_Week AS DATE) = rc.data_as_of_date

      AND raw.Channel IS NOT NULL

      AND UPPER(TRIM(raw.Channel)) NOT IN (
        'OTHER (DO NOT USE)',
        'NON-WORKING',
        'BUDGET HELD'
      )

      AND (
        (raw.spend_actual IS NOT NULL AND raw.spend_actual != 0)
        OR
        (raw.spend_forecast IS NOT NULL AND raw.spend_forecast != 0)
      )
  ),


  MfcSourcePrepared AS (

    SELECT
      lob,
      spend_actual,

      CASE
        WHEN week_has_forecast = 1
          THEN spend_forecast

        ELSE spend_actual
      END AS spend_forecast

    FROM MfcSourceTyped
  ),


  MfcSourceAgg AS (

    SELECT
      SUM(CASE
            WHEN lob = 'POSTPAID'
            THEN spend_actual
          END) AS actual_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
            THEN spend_actual
          END) AS actual_broadband,

      SUM(CASE
            WHEN lob IN ('POSTPAID','BROADBAND')
            THEN spend_actual
          END) AS actual_total,

      SUM(CASE
            WHEN lob = 'POSTPAID'
            THEN spend_forecast
          END) AS forecast_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
            THEN spend_forecast
          END) AS forecast_broadband,

      SUM(CASE
            WHEN lob IN ('POSTPAID','BROADBAND')
            THEN spend_forecast
          END) AS forecast_total

    FROM MfcSourcePrepared
  ),


  MfcBronzeAgg AS (

    SELECT
      SUM(CASE
            WHEN UPPER(TRIM(b.lob)) = 'POSTPAID'
            THEN b.spend_actual
          END) AS actual_postpaid,

      SUM(CASE
            WHEN UPPER(TRIM(b.lob)) = 'BROADBAND'
            THEN b.spend_actual
          END) AS actual_broadband,

      SUM(CASE
            WHEN UPPER(TRIM(b.lob)) IN ('POSTPAID','BROADBAND')
            THEN b.spend_actual
          END) AS actual_total,

      SUM(CASE
            WHEN UPPER(TRIM(b.lob)) = 'POSTPAID'
            THEN b.spend_forecast
          END) AS forecast_postpaid,

      SUM(CASE
            WHEN UPPER(TRIM(b.lob)) = 'BROADBAND'
            THEN b.spend_forecast
          END) AS forecast_broadband,

      SUM(CASE
            WHEN UPPER(TRIM(b.lob)) IN ('POSTPAID','BROADBAND')
            THEN b.spend_forecast
          END) AS forecast_total

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly b

    CROSS JOIN RunContext rc

    WHERE
      b.qgp_week = rc.data_as_of_date
  ),


  MfcSilverBase AS (

    SELECT
      s.metric_name,

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

        WHEN UPPER(TRIM(s.lob_mfc)) IN (
          'TBG',
          'TFB'
        )
          THEN 'TFB'

        ELSE UPPER(TRIM(s.lob_mfc))
      END AS lob,

      s.metric_value

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly s

    CROSS JOIN RunContext rc

    WHERE
      s.qgp_date = rc.data_as_of_date
      AND s.data_source = 'MFC_SPEND_CHANNEL'
      AND s.channel_group = 'All Channels'
  ),


  MfcSilverAgg AS (

    SELECT
      SUM(CASE
            WHEN lob = 'POSTPAID'
             AND metric_name = 'mfcSpendActual'
            THEN metric_value
          END) AS actual_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
             AND metric_name = 'mfcSpendActual'
            THEN metric_value
          END) AS actual_broadband,

      SUM(CASE
            WHEN lob IN ('POSTPAID','BROADBAND')
             AND metric_name = 'mfcSpendActual'
            THEN metric_value
          END) AS actual_total,

      SUM(CASE
            WHEN lob = 'POSTPAID'
             AND metric_name = 'mfcSpendForecast'
            THEN metric_value
          END) AS forecast_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
             AND metric_name = 'mfcSpendForecast'
            THEN metric_value
          END) AS forecast_broadband,

      SUM(CASE
            WHEN lob IN ('POSTPAID','BROADBAND')
             AND metric_name = 'mfcSpendForecast'
            THEN metric_value
          END) AS forecast_total

    FROM MfcSilverBase
  ),


  MfcGoldBase AS (

    SELECT
      g.metric_name,
      UPPER(TRIM(g.true_lob)) AS lob,
      g.metric_value

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

    CROSS JOIN RunContext rc

    WHERE
      g.qgp_date = rc.data_as_of_date
      AND g.data_source = 'MFC_SPEND_CHANNEL'
      AND g.channel_group = 'All Channels'
  ),


  MfcGoldAgg AS (

    SELECT
      SUM(CASE
            WHEN lob = 'POSTPAID'
             AND metric_name = 'mfcSpendActual'
            THEN metric_value
          END) AS actual_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
             AND metric_name = 'mfcSpendActual'
            THEN metric_value
          END) AS actual_broadband,

      SUM(CASE
            WHEN lob IN ('POSTPAID','BROADBAND')
             AND metric_name = 'mfcSpendActual'
            THEN metric_value
          END) AS actual_total,

      SUM(CASE
            WHEN lob = 'POSTPAID'
             AND metric_name = 'mfcSpendForecast'
            THEN metric_value
          END) AS forecast_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
             AND metric_name = 'mfcSpendForecast'
            THEN metric_value
          END) AS forecast_broadband,

      SUM(CASE
            WHEN lob IN ('POSTPAID','BROADBAND')
             AND metric_name = 'mfcSpendForecast'
            THEN metric_value
          END) AS forecast_total

    FROM MfcGoldBase
  ),


  MfcValidation AS (

    SELECT
      'MFC_SPEND'                                    AS data_source,

      x.metric_name,
      x.metric_type,

      'SOURCE_TO_GOLD'                               AS comparison_scope,
      'DIRECT_QGP_WEEK'                              AS comparison_method,

      x.source_value,
      x.bronze_value,

      /*
        MFC Bronze is already keyed by authoritative qgp_week.
        No validator-side natural-week proration is required.
      */
      x.bronze_value                                 AS bronze_comparable_value,

      x.silver_value,
      x.gold_value,

      TRUE                                           AS check_source_bronze,
      TRUE                                           AS check_bronze_silver,
      TRUE                                           AS check_silver_gold,

      FALSE                                          AS allow_all_null,
      FALSE                                          AS require_nonzero,

      'sdi_vw_mfc_gold_spendGranular_weekly'         AS source_object,
      'sdi_tbl_dashboardPulseTms_bronze_mfcSpend_weekly'
                                                      AS bronze_object,
      'sdi_tbl_dashboardPulseTms_silver_mfcSpend_weekly'
                                                      AS silver_object,
      'sdi_vw_dashboardPulseTms_gold_unified_long'   AS gold_object

    FROM (

      SELECT
        'mfcSpendActualPostpaid' AS metric_name,
        'MFC_SPEND_ACTUAL' AS metric_type,

        src.actual_postpaid AS source_value,
        br.actual_postpaid AS bronze_value,
        si.actual_postpaid AS silver_value,
        go.actual_postpaid AS gold_value

      FROM MfcSourceAgg src
      CROSS JOIN MfcBronzeAgg br
      CROSS JOIN MfcSilverAgg si
      CROSS JOIN MfcGoldAgg go


      UNION ALL


      SELECT
        'mfcSpendActualBroadband',
        'MFC_SPEND_ACTUAL',

        src.actual_broadband,
        br.actual_broadband,
        si.actual_broadband,
        go.actual_broadband

      FROM MfcSourceAgg src
      CROSS JOIN MfcBronzeAgg br
      CROSS JOIN MfcSilverAgg si
      CROSS JOIN MfcGoldAgg go


      UNION ALL


      SELECT
        'mfcSpendActualTotal',
        'MFC_SPEND_ACTUAL',

        src.actual_total,
        br.actual_total,
        si.actual_total,
        go.actual_total

      FROM MfcSourceAgg src
      CROSS JOIN MfcBronzeAgg br
      CROSS JOIN MfcSilverAgg si
      CROSS JOIN MfcGoldAgg go


      UNION ALL


      SELECT
        'mfcSpendForecastPostpaid',
        'MFC_SPEND_FORECAST',

        src.forecast_postpaid,
        br.forecast_postpaid,
        si.forecast_postpaid,
        go.forecast_postpaid

      FROM MfcSourceAgg src
      CROSS JOIN MfcBronzeAgg br
      CROSS JOIN MfcSilverAgg si
      CROSS JOIN MfcGoldAgg go


      UNION ALL


      SELECT
        'mfcSpendForecastBroadband',
        'MFC_SPEND_FORECAST',

        src.forecast_broadband,
        br.forecast_broadband,
        si.forecast_broadband,
        go.forecast_broadband

      FROM MfcSourceAgg src
      CROSS JOIN MfcBronzeAgg br
      CROSS JOIN MfcSilverAgg si
      CROSS JOIN MfcGoldAgg go


      UNION ALL


      SELECT
        'mfcSpendForecastTotal',
        'MFC_SPEND_FORECAST',

        src.forecast_total,
        br.forecast_total,
        si.forecast_total,
        go.forecast_total

      FROM MfcSourceAgg src
      CROSS JOIN MfcBronzeAgg br
      CROSS JOIN MfcSilverAgg si
      CROSS JOIN MfcGoldAgg go

    ) x
  ),


  /* ################################################################################################
     PLATFORM SPEND
     ################################################################################################ */


  PlatformSourceAgg AS (

    SELECT
      SUM(
        CASE
          WHEN UPPER(TRIM(raw.LOB)) = 'POSTPAID'
            THEN TRY_CAST(raw.Spend AS DOUBLE)
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN UPPER(TRIM(raw.LOB)) = 'BROADBAND'
            THEN TRY_CAST(raw.Spend AS DOUBLE)
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN UPPER(TRIM(raw.LOB)) IN (
            'POSTPAID',
            'BROADBAND'
          )
            THEN TRY_CAST(raw.Spend AS DOUBLE)
        END
      ) AS spend_total

    FROM
      prdrzranalytics.lab42.media_analytics_integrated_snapshot raw

    CROSS JOIN RunContext rc

    WHERE
      DATE_ADD(
        CAST(raw.Date AS DATE),
        7 - DAYOFWEEK(CAST(raw.Date AS DATE))
      ) = rc.data_as_of_date

      AND UPPER(TRIM(raw.LOB)) IN (
        'POSTPAID',
        'BROADBAND'
      )

      AND raw.Channel_Group_Name IS NOT NULL
  ),


  PlatformBronzeAgg AS (

    SELECT
      SUM(
        CASE
          WHEN UPPER(TRIM(b.lob)) = 'POSTPAID'
            THEN b.spend
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.lob)) = 'BROADBAND'
            THEN b.spend
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN UPPER(TRIM(b.lob)) IN (
            'POSTPAID',
            'BROADBAND'
          )
            THEN b.spend
        END
      ) AS spend_total

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly b

    CROSS JOIN RunContext rc

    WHERE
      b.week_sun_sat = rc.data_as_of_date
  ),


  PlatformSilverAgg AS (

    SELECT
      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) = 'POSTPAID'
            THEN s.metric_value
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) = 'BROADBAND'
            THEN s.metric_value
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) IN (
            'POSTPAID',
            'BROADBAND'
          )
            THEN s.metric_value
        END
      ) AS spend_total

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly s

    CROSS JOIN RunContext rc

    WHERE
      s.qgp_date = rc.data_as_of_date
      AND s.channel_group = 'All Channels'
      AND s.metric_name = 'platformSpend'
  ),


  PlatformGoldAgg AS (

    SELECT
      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) = 'POSTPAID'
            THEN g.metric_value
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) = 'BROADBAND'
            THEN g.metric_value
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) IN (
            'POSTPAID',
            'BROADBAND'
          )
            THEN g.metric_value
        END
      ) AS spend_total

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

    CROSS JOIN RunContext rc

    WHERE
      g.qgp_date = rc.data_as_of_date
      AND g.data_source = 'PLATFORM_SPEND_CHANNEL'
      AND g.channel_group = 'All Channels'
      AND g.metric_name = 'platformSpend'
  ),


  PlatformValidation AS (

    SELECT
      'PLATFORM_SPEND'                                 AS data_source,

      x.metric_name,
      'PLATFORM_SPEND'                                 AS metric_type,

      'SOURCE_TO_GOLD'                                 AS comparison_scope,

      CASE
        WHEN rc.week_type = 'BOUNDARY_FIRST'
          THEN 'QGP_PRORATION'
        ELSE 'DIRECT'
      END                                              AS comparison_method,

      x.source_value,
      x.bronze_value,

      CASE
        WHEN rc.week_type = 'BOUNDARY_FIRST'
          THEN x.bronze_value * rc.days_in_period / 7.0

        ELSE x.bronze_value
      END                                              AS bronze_comparable_value,

      x.silver_value,
      x.gold_value,

      TRUE                                             AS check_source_bronze,
      TRUE                                             AS check_bronze_silver,
      TRUE                                             AS check_silver_gold,

      FALSE                                            AS allow_all_null,
      FALSE                                            AS require_nonzero,

      'media_analytics_integrated_snapshot'            AS source_object,
      'sdi_tbl_dashboardPulseTms_bronze_platformSpend_weekly'
                                                        AS bronze_object,
      'sdi_tbl_dashboardPulseTms_silver_platformSpend_weekly'
                                                        AS silver_object,
      'sdi_vw_dashboardPulseTms_gold_unified_long'     AS gold_object

    FROM (

      SELECT
        'platformSpendPostpaid' AS metric_name,

        src.spend_postpaid AS source_value,
        br.spend_postpaid AS bronze_value,
        si.spend_postpaid AS silver_value,
        go.spend_postpaid AS gold_value

      FROM PlatformSourceAgg src
      CROSS JOIN PlatformBronzeAgg br
      CROSS JOIN PlatformSilverAgg si
      CROSS JOIN PlatformGoldAgg go


      UNION ALL


      SELECT
        'platformSpendBroadband',

        src.spend_broadband,
        br.spend_broadband,
        si.spend_broadband,
        go.spend_broadband

      FROM PlatformSourceAgg src
      CROSS JOIN PlatformBronzeAgg br
      CROSS JOIN PlatformSilverAgg si
      CROSS JOIN PlatformGoldAgg go


      UNION ALL


      SELECT
        'platformSpendTotal',

        src.spend_total,
        br.spend_total,
        si.spend_total,
        go.spend_total

      FROM PlatformSourceAgg src
      CROSS JOIN PlatformBronzeAgg br
      CROSS JOIN PlatformSilverAgg si
      CROSS JOIN PlatformGoldAgg go

    ) x

    CROSS JOIN RunContext rc
  ),


  /* ################################################################################################
     BIDDABLE SPEND

     Source components:
       Programmatic
       Paid Social
       Paid Search

     Silver LOB ALL is already the synthetic:
       POSTPAID + BROADBAND + FIBER

     Therefore:
       Source/Bronze Total = sum of atomic LOBs
       Silver/Gold Total    = lob/true_lob = ALL

     We never add ALL back to its components.
     ################################################################################################ */


  BiddableSourceAtomic AS (

    /* ---------------------------------------------------------------------------------------------
       PROGRAMMATIC
       ------------------------------------------------------------------------------------------- */

    SELECT
      CASE
        WHEN UPPER(TRIM(raw.lob)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'

        WHEN UPPER(TRIM(raw.lob)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'
      END AS lob,

      TRY_CAST(raw.spend AS DOUBLE) AS spend

    FROM
      prd_dbi_analytics.improvado.pbi_programmatic_browsers_currentyr raw

    CROSS JOIN RunContext rc

    WHERE
      DATE_ADD(
        CAST(raw.date AS DATE),
        7 - DAYOFWEEK(CAST(raw.date AS DATE))
      ) = rc.data_as_of_date

      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )


    UNION ALL


    /* ---------------------------------------------------------------------------------------------
       PAID SOCIAL
       ------------------------------------------------------------------------------------------- */

    SELECT
      CASE
        WHEN UPPER(TRIM(raw.LOB)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'

        WHEN UPPER(TRIM(raw.LOB)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'
      END AS lob,

      TRY_CAST(raw.Spend AS DOUBLE) AS spend

    FROM
      prdrzranalytics.lab42.media_analytics_integrated_snapshot raw

    CROSS JOIN RunContext rc

    WHERE
      DATE_ADD(
        CAST(raw.Date AS DATE),
        7 - DAYOFWEEK(CAST(raw.Date AS DATE))
      ) = rc.data_as_of_date

      AND UPPER(TRIM(raw.Channel_Group_Name)) = 'PAID SOCIAL'
      AND UPPER(TRIM(raw.Agency)) = 'INHOUSE'

      AND UPPER(TRIM(raw.LOB)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND'
      )


    UNION ALL


    /* ---------------------------------------------------------------------------------------------
       PAID SEARCH
       ------------------------------------------------------------------------------------------- */

    SELECT
      CASE
        WHEN UPPER(TRIM(raw.lob)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'

        WHEN UPPER(TRIM(raw.lob)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'

        WHEN UPPER(TRIM(raw.lob)) = 'FIBER'
          THEN 'FIBER'
      END AS lob,

      TRY_CAST(raw.cost AS DOUBLE) AS spend

    FROM
      prdrzranalytics.lab42.sdi_tbl_sa360_gold_campaign_daily raw

    CROSS JOIN RunContext rc

    WHERE
      DATE_ADD(
        CAST(raw.date AS DATE),
        7 - DAYOFWEEK(CAST(raw.date AS DATE))
      ) = rc.data_as_of_date

      AND UPPER(TRIM(raw.lob)) IN (
        'POSTPAID',
        'CONSUMER POSTPAID',
        'HSI',
        'BROADBAND',
        'FIBER'
      )

      AND UPPER(TRIM(raw.ad_platform)) IN (
        'GOOGLE',
        'GOOGLE ADS',
        'GOOGLE_ADS',
        'BING',
        'MICROSOFT',
        'MICROSOFT ADS',
        'MICROSOFT_ADS'
      )
  ),


  BiddableSourceAgg AS (

    SELECT
      SUM(
        CASE
          WHEN lob = 'POSTPAID'
          THEN spend
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN lob = 'BROADBAND'
          THEN spend
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN lob = 'FIBER'
          THEN spend
        END
      ) AS spend_fiber,

      SUM(
        CASE
          WHEN lob IN (
            'POSTPAID',
            'BROADBAND',
            'FIBER'
          )
          THEN spend
        END
      ) AS spend_total

    FROM BiddableSourceAtomic
  ),


  BiddableBronzeBase AS (

    SELECT
      CASE
        WHEN UPPER(TRIM(b.lob)) IN (
          'POSTPAID',
          'CONSUMER POSTPAID'
        )
          THEN 'POSTPAID'

        WHEN UPPER(TRIM(b.lob)) IN (
          'HSI',
          'BROADBAND'
        )
          THEN 'BROADBAND'

        WHEN UPPER(TRIM(b.lob)) = 'FIBER'
          THEN 'FIBER'
      END AS lob,

      b.spend

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly b

    CROSS JOIN RunContext rc

    WHERE
      b.week_sun_sat = rc.data_as_of_date
  ),


  BiddableBronzeAgg AS (

    SELECT
      SUM(CASE
            WHEN lob = 'POSTPAID'
            THEN spend
          END) AS spend_postpaid,

      SUM(CASE
            WHEN lob = 'BROADBAND'
            THEN spend
          END) AS spend_broadband,

      SUM(CASE
            WHEN lob = 'FIBER'
            THEN spend
          END) AS spend_fiber,

      SUM(CASE
            WHEN lob IN (
              'POSTPAID',
              'BROADBAND',
              'FIBER'
            )
            THEN spend
          END) AS spend_total

    FROM BiddableBronzeBase
  ),


  BiddableSilverAgg AS (

    SELECT
      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) = 'POSTPAID'
           AND s.metric_name = 'biddableSpend'
          THEN s.metric_value
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) = 'BROADBAND'
           AND s.metric_name = 'biddableSpend'
          THEN s.metric_value
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) = 'FIBER'
           AND s.metric_name = 'biddableSpend'
          THEN s.metric_value
        END
      ) AS spend_fiber,

      /*
        ALL is already the synthetic total.
      */
      SUM(
        CASE
          WHEN UPPER(TRIM(s.lob)) = 'ALL'
           AND s.metric_name = 'biddableSpend'
          THEN s.metric_value
        END
      ) AS spend_total

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly s

    CROSS JOIN RunContext rc

    WHERE
      s.qgp_date = rc.data_as_of_date
      AND s.data_source = 'BIDDABLE_SPEND_CHANNEL'
      AND s.channel_group = 'All Channels'
  ),


  BiddableGoldAgg AS (

    SELECT
      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) = 'POSTPAID'
          THEN g.metric_value
        END
      ) AS spend_postpaid,

      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) = 'BROADBAND'
          THEN g.metric_value
        END
      ) AS spend_broadband,

      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) = 'FIBER'
          THEN g.metric_value
        END
      ) AS spend_fiber,

      SUM(
        CASE
          WHEN UPPER(TRIM(g.true_lob)) = 'ALL'
          THEN g.metric_value
        END
      ) AS spend_total

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

    CROSS JOIN RunContext rc

    WHERE
      g.qgp_date = rc.data_as_of_date
      AND g.data_source = 'BIDDABLE_SPEND_CHANNEL'
      AND g.channel_group = 'All Channels'
      AND g.metric_name = 'biddableSpend'
  ),


  BiddableValidation AS (

    SELECT
      'BIDDABLE_SPEND'                                    AS data_source,

      x.metric_name,
      'BIDDABLE_SPEND'                                    AS metric_type,

      'SOURCE_TO_GOLD'                                    AS comparison_scope,

      CASE
        WHEN rc.week_type = 'BOUNDARY_FIRST'
          THEN 'QGP_PRORATION'
        ELSE 'DIRECT'
      END                                                 AS comparison_method,

      x.source_value,
      x.bronze_value,

      CASE
        WHEN rc.week_type = 'BOUNDARY_FIRST'
          THEN x.bronze_value * rc.days_in_period / 7.0

        ELSE x.bronze_value
      END                                                 AS bronze_comparable_value,

      x.silver_value,
      x.gold_value,

      TRUE                                                AS check_source_bronze,
      TRUE                                                AS check_bronze_silver,
      TRUE                                                AS check_silver_gold,

      FALSE                                               AS allow_all_null,
      FALSE                                               AS require_nonzero,

      'Programmatic + Paid Social + SA360'                AS source_object,
      'sdi_tbl_dashboardPulseTms_bronze_biddableSpend_weekly'
                                                           AS bronze_object,
      'sdi_tbl_dashboardPulseTms_silver_biddableSpend_weekly'
                                                           AS silver_object,
      'sdi_vw_dashboardPulseTms_gold_unified_long'        AS gold_object

    FROM (

      SELECT
        'biddableSpendPostpaid' AS metric_name,

        src.spend_postpaid AS source_value,
        br.spend_postpaid AS bronze_value,
        si.spend_postpaid AS silver_value,
        go.spend_postpaid AS gold_value

      FROM BiddableSourceAgg src
      CROSS JOIN BiddableBronzeAgg br
      CROSS JOIN BiddableSilverAgg si
      CROSS JOIN BiddableGoldAgg go


      UNION ALL


      SELECT
        'biddableSpendBroadband',

        src.spend_broadband,
        br.spend_broadband,
        si.spend_broadband,
        go.spend_broadband

      FROM BiddableSourceAgg src
      CROSS JOIN BiddableBronzeAgg br
      CROSS JOIN BiddableSilverAgg si
      CROSS JOIN BiddableGoldAgg go


      UNION ALL


      SELECT
        'biddableSpendFiber',

        src.spend_fiber,
        br.spend_fiber,
        si.spend_fiber,
        go.spend_fiber

      FROM BiddableSourceAgg src
      CROSS JOIN BiddableBronzeAgg br
      CROSS JOIN BiddableSilverAgg si
      CROSS JOIN BiddableGoldAgg go


      UNION ALL


      SELECT
        'biddableSpendTotal',

        src.spend_total,
        br.spend_total,
        si.spend_total,
        go.spend_total

      FROM BiddableSourceAgg src
      CROSS JOIN BiddableBronzeAgg br
      CROSS JOIN BiddableSilverAgg si
      CROSS JOIN BiddableGoldAgg go

    ) x

    CROSS JOIN RunContext rc
  ),


  /* ################################################################################################
     QGP SOURCE -> BRONZE COVERAGE

     QGP named business metrics are constructed in Silver.

     We deliberately avoid duplicating the entire Silver business-metric definition here.

     Check 1:
       Scoped source rows -> Bronze scoped rows

     Check 2:
       Named Silver metric -> Gold metric
     ################################################################################################ */


  QgpSourceScoped AS (

    SELECT
      TRY_CAST(raw.PublishKey AS DATE)            AS publish_key,
      TRY_CAST(raw.WeekEnding AS DATE)            AS week_ending,

      TRIM(raw.MetricID)                          AS metric_id,
      TRIM(raw.DateContext)                       AS date_context,
      TRIM(raw.MetricType)                        AS metric_type,
      TRIM(raw.Page)                              AS page,

      TRY_CAST(raw.InsertDateTime AS TIMESTAMP)   AS insert_datetime

    FROM
      prdrzranalytics.lab42.sdi_tbl_qgpArchive_bronze_retained_weekly raw

    CROSS JOIN RunContext rc

    WHERE
      TRY_CAST(raw.WeekEnding AS DATE) = rc.data_as_of_date

      AND (

        UPPER(TRIM(raw.MetricID)) IN (

          UPPER('ConsumerPostpaidNewPhoneBANBOPISUnassistedActivationsTM1MappedDigital'),

          UPPER('ConsumerPostpaidNewPhoneBANNonBOPISUnassistedActivationsTM1MappedDigital'),

          UPPER('ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital'),

          UPPER('ConsumerPostpaidBTSBOPISUnassistedActivationsTM1MappedDigital'),

          UPPER('ConsumerPostpaidBTSNonBOPISUnassistedActivationsTM1MappedDigital'),

          UPPER('TotalDigitalOtherActivationsTMOandSprintGlanceTM1Mapped'),

          UPPER('VRInboundCallsinclHSIAutomatedManual'),

          UPPER('VRChatsinclHSIAutomatedManual'),

          UPPER('VRPostpaidActivationsinclVirtualBusinessTM1Mapped'),

          UPPER('ConPostpaidDigitalPctofNewBANPhoneUnassistedANDAssistedwoNRIndirect'),

          UPPER('DigitalPctofConsumerPostpaidActivationsExclFiberNRIndirectChannelBTSODA'),

          UPPER('DigitalPctofConsumerPostpaidActivationsPhoneNEWSamePagewoNRIndirect'),

          UPPER('NewPhoneBANAssistedActivationsDigitalPCTofConsumerPostpaidTM1MappedwoNRIndirect')
        )

        OR LOWER(TRIM(raw.MetricName))
          = 'store traffic (excl store-in-store)'
      )
  ),


  /* Apply same Bronze dedup grain. */

  QgpSourceDeduped AS (

    SELECT
      *

    FROM QgpSourceScoped

    QUALIFY
      ROW_NUMBER() OVER (
        PARTITION BY
          week_ending,
          metric_id,
          date_context,
          metric_type,
          page

        ORDER BY
          insert_datetime DESC,
          publish_key DESC
      ) = 1
  ),


  QgpSourceCoverage AS (

    SELECT
      CAST(COUNT(*) AS DOUBLE) AS source_value

    FROM QgpSourceDeduped
  ),


  QgpBronzeCoverage AS (

    SELECT
      CAST(COUNT(*) AS DOUBLE) AS bronze_value

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_qgp_weekly b

    CROSS JOIN RunContext rc

    WHERE
      b.week_ending = rc.data_as_of_date
  ),


  QgpCoverageValidation AS (

    SELECT
      'QGP_SCORECARD'                                   AS data_source,
      'qgpScopedRows'                                   AS metric_name,
      'ROW_COUNT'                                       AS metric_type,

      'SOURCE_TO_BRONZE'                                AS comparison_scope,
      'ROW_COUNT'                                       AS comparison_method,

      src.source_value,
      br.bronze_value,

      /* No Bronze -> Silver metric comparison for this coverage row. */
      CAST(NULL AS DOUBLE)                              AS bronze_comparable_value,

      CAST(NULL AS DOUBLE)                              AS silver_value,
      CAST(NULL AS DOUBLE)                              AS gold_value,

      TRUE                                              AS check_source_bronze,
      FALSE                                             AS check_bronze_silver,
      FALSE                                             AS check_silver_gold,

      FALSE                                             AS allow_all_null,

      /*
        A row-count check of 0 -> 0 does not prove health.
        It indicates that no scoped QGP source data was found.
      */
      TRUE                                              AS require_nonzero,

      'sdi_tbl_qgpArchive_bronze_retained_weekly'       AS source_object,

      'sdi_tbl_dashboardPulseTms_bronze_qgp_weekly'     AS bronze_object,

      CAST(NULL AS STRING)                              AS silver_object,
      CAST(NULL AS STRING)                              AS gold_object

    FROM QgpSourceCoverage src
    CROSS JOIN QgpBronzeCoverage br
  ),


  /* ===============================================================================================
     QGP BUSINESS METRIC UNIVERSE
     =============================================================================================== */

  QgpMetricNames AS (

    SELECT metric_name

    FROM VALUES
      ('activationsBopis'),
      ('activationsBopisOnly'),
      ('activationsNonBopisOnly'),
      ('activationsNewAalNoAssistance'),
      ('storeTraffic'),
      ('vrCalls'),
      ('vrChats'),
      ('vrPostpaidActivations'),
      ('digitalPctPhoneNewActsNoAssistPlusAssist'),
      ('digitalPctConsumerPostpaidActivationsTotalInclAssisted'),
      ('digitalPctNoAssistanceActivations'),
      ('digitalPctAssistanceActivations')

    AS q(metric_name)
  ),


  QgpMetricTypes AS (

    SELECT metric_type

    FROM VALUES
      ('QGP_ACTUAL'),
      ('QGP_TARGET')

    AS q(metric_type)
  ),


  QgpMetricList AS (

    SELECT
      n.metric_name,
      t.metric_type

    FROM QgpMetricNames n

    CROSS JOIN QgpMetricTypes t
  ),


  QgpSilverMetrics AS (

    SELECT
      s.metric_name,
      s.metric_type,

      MAX(s.metric_value) AS silver_value

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_qgp_weekly s

    CROSS JOIN RunContext rc

    WHERE
      s.qgp_date = rc.data_as_of_date

    GROUP BY
      s.metric_name,
      s.metric_type
  ),


  QgpGoldMetrics AS (

    SELECT
      g.metric_name,
      g.metric_type,

      MAX(g.metric_value) AS gold_value

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

    CROSS JOIN RunContext rc

    WHERE
      g.qgp_date = rc.data_as_of_date
      AND g.data_source = 'QGP_SCORECARD'

    GROUP BY
      g.metric_name,
      g.metric_type
  ),


  QgpMetricValidation AS (

    SELECT
      'QGP_SCORECARD'                                   AS data_source,

      m.metric_name,
      m.metric_type,

      'SILVER_TO_GOLD'                                  AS comparison_scope,
      'SILVER_TO_GOLD_ONLY'                             AS comparison_method,

      CAST(NULL AS DOUBLE)                              AS source_value,
      CAST(NULL AS DOUBLE)                              AS bronze_value,
      CAST(NULL AS DOUBLE)                              AS bronze_comparable_value,

      s.silver_value,
      g.gold_value,

      FALSE                                             AS check_source_bronze,
      FALSE                                             AS check_bronze_silver,
      TRUE                                              AS check_silver_gold,

      CASE

        /*
          Component QGP targets can naturally be unavailable.
        */
        WHEN m.metric_type = 'QGP_TARGET'
         AND m.metric_name IN (
           'activationsBopisOnly',
           'activationsNonBopisOnly'
         )
          THEN TRUE

        /*
          These two targets are intentionally hardcoded NULL in QGP Silver.
        */
        WHEN m.metric_type = 'QGP_TARGET'
         AND m.metric_name IN (
           'digitalPctNoAssistanceActivations',
           'digitalPctAssistanceActivations'
         )
          THEN TRUE

        ELSE FALSE

      END                                               AS allow_all_null,

      FALSE                                             AS require_nonzero,

      CAST(NULL AS STRING)                              AS source_object,
      CAST(NULL AS STRING)                              AS bronze_object,

      'sdi_tbl_dashboardPulseTms_silver_qgp_weekly'     AS silver_object,

      'sdi_vw_dashboardPulseTms_gold_unified_long'      AS gold_object

    FROM QgpMetricList m

    LEFT JOIN QgpSilverMetrics s
      ON s.metric_name = m.metric_name
     AND s.metric_type = m.metric_type

    LEFT JOIN QgpGoldMetrics g
      ON g.metric_name = m.metric_name
     AND g.metric_type = m.metric_type
  ),


  /* ################################################################################################
     UPV FORECAST

     Bronze is populated externally.

     The current Silver implementation states that:
       All Channels allocation_ratio = 1.0

     and Bronze has already handled boundary proration.

     Therefore:
       Bronze Comparable = Bronze
     ################################################################################################ */


  UpvForecastBronze AS (

    SELECT
      MAX(b.upv_forecast) AS upv_forecast,
      MAX(b.upv_webapp_forecast) AS upv_webapp_forecast

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly b

    CROSS JOIN RunContext rc

    WHERE
      b.week_sun_sat = rc.data_as_of_date
  ),


  UpvForecastSilver AS (

    SELECT
      MAX(
        CASE
          WHEN s.metric_name = 'upvForecast'
          THEN s.metric_value
        END
      ) AS upv_forecast,

      MAX(
        CASE
          WHEN s.metric_name = 'upvWebAppForecast'
          THEN s.metric_value
        END
      ) AS upv_webapp_forecast

    FROM
      prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly s

    CROSS JOIN RunContext rc

    WHERE
      s.qgp_date = rc.data_as_of_date
      AND s.channel_group = 'All Channels'
  ),


  UpvForecastGold AS (

    SELECT
      MAX(
        CASE
          WHEN g.metric_name = 'upvForecast'
          THEN g.metric_value
        END
      ) AS upv_forecast,

      MAX(
        CASE
          WHEN g.metric_name = 'upvWebAppForecast'
          THEN g.metric_value
        END
      ) AS upv_webapp_forecast

    FROM
      prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g

    CROSS JOIN RunContext rc

    WHERE
      g.qgp_date = rc.data_as_of_date
      AND g.data_source = 'UPV_FORECAST'
      AND g.channel_group = 'All Channels'
  ),


  UpvForecastValidation AS (

    SELECT
      'UPV_FORECAST'                                      AS data_source,

      x.metric_name,
      'UPV_FORECAST'                                      AS metric_type,

      'BRONZE_TO_GOLD'                                    AS comparison_scope,
      'PRE_PRORATED_BRONZE'                               AS comparison_method,

      CAST(NULL AS DOUBLE)                                AS source_value,

      x.bronze_value,
      x.bronze_value                                      AS bronze_comparable_value,

      x.silver_value,
      x.gold_value,

      FALSE                                               AS check_source_bronze,
      TRUE                                                AS check_bronze_silver,
      TRUE                                                AS check_silver_gold,

      FALSE                                               AS allow_all_null,
      FALSE                                               AS require_nonzero,

      'External UPV Forecast Bronze upload notebook'       AS source_object,

      'sdi_tbl_dashboardPulseTms_bronze_upvForecast_weekly'
                                                           AS bronze_object,

      'sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly'
                                                           AS silver_object,

      'sdi_vw_dashboardPulseTms_gold_unified_long'        AS gold_object

    FROM (

      SELECT
        'upvForecast' AS metric_name,

        br.upv_forecast AS bronze_value,
        si.upv_forecast AS silver_value,
        go.upv_forecast AS gold_value

      FROM UpvForecastBronze br
      CROSS JOIN UpvForecastSilver si
      CROSS JOIN UpvForecastGold go


      UNION ALL


      SELECT
        'upvWebAppForecast',

        br.upv_webapp_forecast,
        si.upv_webapp_forecast,
        go.upv_webapp_forecast

      FROM UpvForecastBronze br
      CROSS JOIN UpvForecastSilver si
      CROSS JOIN UpvForecastGold go

    ) x
  ),


  /* ===============================================================================================
     COMBINE ALL VALIDATION CHECKS
     =============================================================================================== */

  AllValidationRows AS (

    SELECT * FROM AdobeValidation

    UNION ALL

    SELECT * FROM MfcValidation

    UNION ALL

    SELECT * FROM PlatformValidation

    UNION ALL

    SELECT * FROM BiddableValidation

    UNION ALL

    SELECT * FROM QgpCoverageValidation

    UNION ALL

    SELECT * FROM QgpMetricValidation

    UNION ALL

    SELECT * FROM UpvForecastValidation
  ),


  /* ===============================================================================================
     NORMALIZE EVERYTHING TO TWO DECIMAL PLACES BEFORE COMPARISON
     =============================================================================================== */

  Normalized AS (

    SELECT
      rc.validation_run_id,
      rc.validation_run_ts,

      rc.data_as_of_date,
      rc.week_type,
      rc.days_in_period,

      v.data_source,
      v.metric_name,
      v.metric_type,

      v.comparison_scope,
      v.comparison_method,

      CAST(
        ROUND(v.source_value, 2)
        AS DECIMAL(38,2)
      ) AS source_value,

      CAST(
        ROUND(v.bronze_value, 2)
        AS DECIMAL(38,2)
      ) AS bronze_value,

      CAST(
        ROUND(v.bronze_comparable_value, 2)
        AS DECIMAL(38,2)
      ) AS bronze_comparable_value,

      CAST(
        ROUND(v.silver_value, 2)
        AS DECIMAL(38,2)
      ) AS silver_value,

      CAST(
        ROUND(v.gold_value, 2)
        AS DECIMAL(38,2)
      ) AS gold_value,

      v.check_source_bronze,
      v.check_bronze_silver,
      v.check_silver_gold,

      v.allow_all_null,
      v.require_nonzero,

      rc.warning_threshold_pct,
      rc.critical_threshold_pct,

      v.source_object,
      v.bronze_object,
      v.silver_object,
      v.gold_object

    FROM AllValidationRows v

    CROSS JOIN RunContext rc
  ),


  /* ===============================================================================================
     CALCULATE LAYER VARIANCES

     Percentage:
       (downstream - upstream) / ABS(upstream) * 100

     Bronze -> Silver uses:
       bronze_comparable_value

     not:
       bronze_value
     =============================================================================================== */

  Variances AS (

    SELECT
      *,

      /* SOURCE -> BRONZE */

      CASE
        WHEN check_source_bronze
         AND source_value IS NOT NULL
         AND bronze_value IS NOT NULL
          THEN CAST(
            bronze_value - source_value
            AS DECIMAL(38,2)
          )
      END AS source_bronze_variance,

      CASE
        WHEN check_source_bronze
         AND source_value IS NOT NULL
         AND source_value != 0
         AND bronze_value IS NOT NULL
          THEN CAST(
            ROUND(
              100.0 * TRY_DIVIDE(
                bronze_value - source_value,
                ABS(source_value)
              ),
              2
            )
            AS DECIMAL(18,2)
          )
      END AS source_bronze_variance_pct,


      /* BRONZE COMPARABLE -> SILVER */

      CASE
        WHEN check_bronze_silver
         AND bronze_comparable_value IS NOT NULL
         AND silver_value IS NOT NULL
          THEN CAST(
            silver_value - bronze_comparable_value
            AS DECIMAL(38,2)
          )
      END AS bronze_silver_variance,

      CASE
        WHEN check_bronze_silver
         AND bronze_comparable_value IS NOT NULL
         AND bronze_comparable_value != 0
         AND silver_value IS NOT NULL
          THEN CAST(
            ROUND(
              100.0 * TRY_DIVIDE(
                silver_value - bronze_comparable_value,
                ABS(bronze_comparable_value)
              ),
              2
            )
            AS DECIMAL(18,2)
          )
      END AS bronze_silver_variance_pct,


      /* SILVER -> GOLD */

      CASE
        WHEN check_silver_gold
         AND silver_value IS NOT NULL
         AND gold_value IS NOT NULL
          THEN CAST(
            gold_value - silver_value
            AS DECIMAL(38,2)
          )
      END AS silver_gold_variance,

      CASE
        WHEN check_silver_gold
         AND silver_value IS NOT NULL
         AND silver_value != 0
         AND gold_value IS NOT NULL
          THEN CAST(
            ROUND(
              100.0 * TRY_DIVIDE(
                gold_value - silver_value,
                ABS(silver_value)
              ),
              2
            )
            AS DECIMAL(18,2)
          )
      END AS silver_gold_variance_pct

    FROM Normalized
  ),


  /* ===============================================================================================
     DETECT RECONCILIATION ISSUES
     =============================================================================================== */

  IssueFlags AS (

    SELECT
      *,

      /* -------------------------------------------------------------------------------------------
         Pair mismatch flags.

         Both NULL is handled separately as data availability.
         ----------------------------------------------------------------------------------------- */

      CASE
        WHEN check_source_bronze = TRUE
         AND (
           (source_value IS NULL AND bronze_value IS NOT NULL)

           OR

           (source_value IS NOT NULL AND bronze_value IS NULL)

           OR

           (
             source_value IS NOT NULL
             AND bronze_value IS NOT NULL
             AND source_value != bronze_value
           )
         )
          THEN TRUE

        ELSE FALSE
      END AS source_bronze_issue,


      CASE
        WHEN check_bronze_silver = TRUE
         AND (
           (bronze_comparable_value IS NULL AND silver_value IS NOT NULL)

           OR

           (bronze_comparable_value IS NOT NULL AND silver_value IS NULL)

           OR

           (
             bronze_comparable_value IS NOT NULL
             AND silver_value IS NOT NULL
             AND bronze_comparable_value != silver_value
           )
         )
          THEN TRUE

        ELSE FALSE
      END AS bronze_silver_issue,


      CASE
        WHEN check_silver_gold = TRUE
         AND (
           (silver_value IS NULL AND gold_value IS NOT NULL)

           OR

           (silver_value IS NOT NULL AND gold_value IS NULL)

           OR

           (
             silver_value IS NOT NULL
             AND gold_value IS NOT NULL
             AND silver_value != gold_value
           )
         )
          THEN TRUE

        ELSE FALSE
      END AS silver_gold_issue,


      /* -------------------------------------------------------------------------------------------
         Data availability.

         Known permitted-all-NULL metric definitions are excluded.
         ----------------------------------------------------------------------------------------- */

      CASE

        WHEN allow_all_null = TRUE
          THEN FALSE


        WHEN require_nonzero = TRUE
         AND COALESCE(
               source_value,
               bronze_value,
               bronze_comparable_value,
               silver_value,
               gold_value,
               CAST(0 AS DECIMAL(38,2))
             ) = 0
          THEN TRUE


        WHEN check_source_bronze = TRUE
         AND source_value IS NULL
         AND bronze_value IS NULL
          THEN TRUE


        WHEN check_bronze_silver = TRUE
         AND bronze_comparable_value IS NULL
         AND silver_value IS NULL
          THEN TRUE


        WHEN check_silver_gold = TRUE
         AND silver_value IS NULL
         AND gold_value IS NULL
          THEN TRUE


        ELSE FALSE

      END AS data_availability_issue

    FROM Variances
  ),


  /* ===============================================================================================
     CLASSIFY STATUS + ISSUE LAYER
     =============================================================================================== */

  Classified AS (

    SELECT
      *,

      /* -------------------------------------------------------------------------------------------
         ISSUE LAYER
         ----------------------------------------------------------------------------------------- */

      CASE

        WHEN allow_all_null = TRUE
         AND silver_value IS NULL
         AND gold_value IS NULL
          THEN 'NONE'


        WHEN
          (
            CASE WHEN source_bronze_issue THEN 1 ELSE 0 END
            +
            CASE WHEN bronze_silver_issue THEN 1 ELSE 0 END
            +
            CASE WHEN silver_gold_issue THEN 1 ELSE 0 END
          ) > 1
          THEN 'MULTIPLE'


        WHEN source_bronze_issue
          THEN 'SOURCE_TO_BRONZE'


        WHEN bronze_silver_issue
          THEN 'BRONZE_TO_SILVER'


        WHEN silver_gold_issue
          THEN 'SILVER_TO_GOLD'


        WHEN data_availability_issue
          THEN 'DATA_AVAILABILITY'


        ELSE 'NONE'

      END AS issue_layer,


      /* -------------------------------------------------------------------------------------------
         STATUS

         FAILED:
           required downstream layer missing despite upstream value
           OR >=25% unexplained percentage variance

         WARNING:
           any other 2-decimal mismatch
           OR data availability issue

         HEALTHY:
           everything applicable reconciles
         ----------------------------------------------------------------------------------------- */

      CASE

        /* Known allowed NULL definition */
        WHEN allow_all_null = TRUE
         AND silver_value IS NULL
         AND gold_value IS NULL
          THEN 'Healthy'


        /* Required downstream Bronze disappeared */
        WHEN check_source_bronze = TRUE
         AND source_value IS NOT NULL
         AND bronze_value IS NULL
          THEN 'Failed'


        /* Required downstream Silver disappeared */
        WHEN check_bronze_silver = TRUE
         AND bronze_comparable_value IS NOT NULL
         AND silver_value IS NULL
          THEN 'Failed'


        /* Required downstream Gold disappeared */
        WHEN check_silver_gold = TRUE
         AND silver_value IS NOT NULL
         AND gold_value IS NULL
          THEN 'Failed'


        /* Critical Source -> Bronze break */
        WHEN ABS(
               COALESCE(
                 source_bronze_variance_pct,
                 CAST(0 AS DECIMAL(18,2))
               )
             ) >= critical_threshold_pct
          THEN 'Failed'


        /* Critical Bronze -> Silver break */
        WHEN ABS(
               COALESCE(
                 bronze_silver_variance_pct,
                 CAST(0 AS DECIMAL(18,2))
               )
             ) >= critical_threshold_pct
          THEN 'Failed'


        /* Critical Silver -> Gold break */
        WHEN ABS(
               COALESCE(
                 silver_gold_variance_pct,
                 CAST(0 AS DECIMAL(18,2))
               )
             ) >= critical_threshold_pct
          THEN 'Failed'


        /* Any remaining reconciliation mismatch */
        WHEN source_bronze_issue
          OR bronze_silver_issue
          OR silver_gold_issue
          THEN 'Warning'


        /* Both expected layers absent / no data */
        WHEN data_availability_issue
          THEN 'Warning'


        ELSE 'Healthy'

      END AS status

    FROM IssueFlags
  ),


  /* ===============================================================================================
     HUMAN-READABLE NOTES + NEXT STEP
     =============================================================================================== */

  FinalOutput AS (

    SELECT
      validation_run_id,
      validation_run_ts,

      data_as_of_date,
      week_type,
      days_in_period,

      data_source,
      metric_name,
      metric_type,

      comparison_scope,
      comparison_method,

      source_value,
      bronze_value,
      bronze_comparable_value,
      silver_value,
      gold_value,

      source_bronze_variance,
      source_bronze_variance_pct,

      bronze_silver_variance,
      bronze_silver_variance_pct,

      silver_gold_variance,
      silver_gold_variance_pct,

      warning_threshold_pct,
      critical_threshold_pct,

      issue_layer,
      status,


      /* -------------------------------------------------------------------------------------------
         NOTES
         ----------------------------------------------------------------------------------------- */

      CASE

        WHEN allow_all_null = TRUE
         AND silver_value IS NULL
         AND gold_value IS NULL
          THEN
            'Silver and Gold are both NULL as permitted by this metric definition.'


        WHEN check_source_bronze = TRUE
         AND source_value IS NOT NULL
         AND bronze_value IS NULL
          THEN CONCAT(
            'Critical: Source contains ',
            CAST(source_value AS STRING),
            ' but Bronze is missing.'
          )


        WHEN check_bronze_silver = TRUE
         AND bronze_comparable_value IS NOT NULL
         AND silver_value IS NULL
          THEN CONCAT(
            'Critical: Bronze comparable value is ',
            CAST(bronze_comparable_value AS STRING),
            ' but Silver is missing.'
          )


        WHEN check_silver_gold = TRUE
         AND silver_value IS NOT NULL
         AND gold_value IS NULL
          THEN CONCAT(
            'Critical: Silver contains ',
            CAST(silver_value AS STRING),
            ' but Gold is missing.'
          )


        WHEN issue_layer = 'MULTIPLE'
          THEN CONCAT(
            'Multiple reconciliation differences detected. ',
            'Source -> Bronze variance: ',
            COALESCE(CAST(source_bronze_variance AS STRING), 'N/A'),
            ' (',
            COALESCE(CAST(source_bronze_variance_pct AS STRING), 'N/A'),
            '%); Bronze Comparable -> Silver variance: ',
            COALESCE(CAST(bronze_silver_variance AS STRING), 'N/A'),
            ' (',
            COALESCE(CAST(bronze_silver_variance_pct AS STRING), 'N/A'),
            '%); Silver -> Gold variance: ',
            COALESCE(CAST(silver_gold_variance AS STRING), 'N/A'),
            ' (',
            COALESCE(CAST(silver_gold_variance_pct AS STRING), 'N/A'),
            '%).'
          )


        WHEN issue_layer = 'SOURCE_TO_BRONZE'
          THEN CONCAT(
            'Source -> Bronze does not reconcile at two-decimal precision. ',
            'Variance = ',
            COALESCE(CAST(source_bronze_variance AS STRING), 'N/A'),
            ' (',
            COALESCE(CAST(source_bronze_variance_pct AS STRING), 'N/A'),
            '%).'
          )


        WHEN issue_layer = 'BRONZE_TO_SILVER'
          THEN CONCAT(
            'Bronze Comparable -> Silver does not reconcile at two-decimal precision. ',
            'Bronze = ',
            COALESCE(CAST(bronze_value AS STRING), 'NULL'),
            '; Bronze Comparable = ',
            COALESCE(CAST(bronze_comparable_value AS STRING), 'NULL'),
            '; Silver = ',
            COALESCE(CAST(silver_value AS STRING), 'NULL'),
            '; variance = ',
            COALESCE(CAST(bronze_silver_variance AS STRING), 'N/A'),
            ' (',
            COALESCE(CAST(bronze_silver_variance_pct AS STRING), 'N/A'),
            '%).'
          )


        WHEN issue_layer = 'SILVER_TO_GOLD'
          THEN CONCAT(
            'Silver -> Gold does not reconcile at two-decimal precision. ',
            'Variance = ',
            COALESCE(CAST(silver_gold_variance AS STRING), 'N/A'),
            ' (',
            COALESCE(CAST(silver_gold_variance_pct AS STRING), 'N/A'),
            '%).'
          )


        WHEN issue_layer = 'DATA_AVAILABILITY'
          THEN
            'Expected validation data is unavailable for the selected reporting period.'


        WHEN comparison_method = 'QGP_PRORATION'
          THEN
            'Values reconcile after applying the same QGP boundary proration used by Silver.'


        ELSE
          'Values reconcile to two-decimal precision across all applicable layers.'

      END AS notes,


      /* -------------------------------------------------------------------------------------------
         NEXT STEP
         ----------------------------------------------------------------------------------------- */

      CASE

        WHEN status = 'Healthy'
          THEN 'No action required.'


        WHEN issue_layer = 'SOURCE_TO_BRONZE'
          THEN CONCAT(
            'Compare ',
            COALESCE(source_object, 'the upstream source'),
            ' with ',
            COALESCE(bronze_object, 'the Bronze layer'),
            '. Review date filters, source filters, mappings, deduplication and aggregation.'
          )


        WHEN issue_layer = 'BRONZE_TO_SILVER'
          THEN CONCAT(
            'Review ',
            COALESCE(silver_object, 'the Silver layer'),
            '. Confirm QGP/date alignment, the comparison transformation, LOB/channel mapping and aggregation logic.'
          )


        WHEN issue_layer = 'SILVER_TO_GOLD'
          THEN CONCAT(
            'Review ',
            COALESCE(gold_object, 'the Gold layer'),
            '. Gold should reconcile to the corresponding Silver reporting metric.'
          )


        WHEN issue_layer = 'MULTIPLE'
          THEN
            'Start with the earliest failing layer shown in the row, then validate downstream layers after the upstream difference is resolved.'


        WHEN issue_layer = 'DATA_AVAILABILITY'
          THEN CONCAT(
            'Confirm that the expected source/publication completed for ',
            CAST(data_as_of_date AS STRING),
            ' and then verify the corresponding Bronze/Silver refresh.'
          )


        ELSE
          'Review the metric lineage and corresponding validation values.'

      END AS next_step,

      source_object,
      bronze_object,
      silver_object,
      gold_object,

      CURRENT_TIMESTAMP() AS created_ts

    FROM Classified
  )


  /* ===============================================================================================
     APPEND SNAPSHOT
     =============================================================================================== */

  INSERT INTO
    prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_validation_history_perRun
  (
    validation_run_id,
    validation_run_ts,

    data_as_of_date,
    week_type,
    days_in_period,

    data_source,
    metric_name,
    metric_type,

    comparison_scope,
    comparison_method,

    source_value,
    bronze_value,
    bronze_comparable_value,
    silver_value,
    gold_value,

    source_bronze_variance,
    source_bronze_variance_pct,

    bronze_silver_variance,
    bronze_silver_variance_pct,

    silver_gold_variance,
    silver_gold_variance_pct,

    warning_threshold_pct,
    critical_threshold_pct,

    issue_layer,
    status,

    notes,
    next_step,

    source_object,
    bronze_object,
    silver_object,
    gold_object,

    created_ts
  )

  SELECT
    validation_run_id,
    validation_run_ts,

    data_as_of_date,
    week_type,
    days_in_period,

    data_source,
    metric_name,
    metric_type,

    comparison_scope,
    comparison_method,

    source_value,
    bronze_value,
    bronze_comparable_value,
    silver_value,
    gold_value,

    source_bronze_variance,
    source_bronze_variance_pct,

    bronze_silver_variance,
    bronze_silver_variance_pct,

    silver_gold_variance,
    silver_gold_variance_pct,

    warning_threshold_pct,
    critical_threshold_pct,

    issue_layer,
    status,

    notes,
    next_step,

    source_object,
    bronze_object,
    silver_object,
    gold_object,

    created_ts

  FROM FinalOutput
  ;

END;