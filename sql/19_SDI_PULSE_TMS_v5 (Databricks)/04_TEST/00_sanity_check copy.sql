/* =================================================================================================
FILE:           01_sdi_sp_improvadoMonitor_adobeWeekly_perRun.sql
PLATFORM:       Databricks
LAYER:          Source monitor (outside the PulseTMS pipeline)
CATALOG.SCHEMA: prdrzranalytics.lab42
PROCEDURE:      sdi_sp_improvadoMonitor_adobeWeekly_perRun

PURPOSE:
  Looks at the raw Adobe tables that Improvado delivers (prd_dbi_analytics.improvado.sdi_raw_adobe_*,
  251 tables, the same ones the PulseTMS Adobe Bronze reads) and says, for one week, which of them look
  wrong and what to ask next. It does NOT change anything in PulseTMS and PulseTMS does not read it.
  The PulseTMS validation answers "did the pipeline carry the source faithfully?". This monitor answers
  "is the source itself plausible?", which validation cannot see.

CHECKS (one row per raw table per run; problem_code is the first one that applies):
  NO_FILE_FOR_WEEK          the table has no file for the week.                                     HIGH
  LOADED_BEFORE_WEEK_ENDED  the latest file was loaded before the week ended (a partial count).     HIGH
  NULL_VALUE                the file exists but the value is empty.                                 HIGH
  ZERO_AFTER_NONZERO        value is 0 and the prior 4-week average is at least v_min_prior_avg.    HIGH
  RESTATED_DOWN             a later file for the week is below 80% of an earlier one.              MEDIUM
  DROP_VS_HISTORY           value is at most 50% of the prior 4-week average.                       MEDIUM
  JUMP_VS_HISTORY           value is at least 200% of the prior 4-week average.                     MEDIUM
  ZERO_VALUE                value is 0 on a low-volume metric.                                      INFO
  CHANNEL_SUM_MISMATCH      for additive metrics (orders, visits, page events, events), the All
                            Channels table is outside 90% to 110% of the sum of its channel tables.
                            Only compared when every channel file is present and complete.
                            HIGH when the ratio is above 1.5 or below 0.67, otherwise MEDIUM.
  Prior-week averages use the latest file of each of the 4 weeks before. Metrics whose prior average is
  below v_min_prior_avg are not judged on size (tiny numbers swing too much). Unique-visitor tables are
  left out of the channel-sum check because visitors are not additive across channels.

HOW TO RUN:
  CALL prdrzranalytics.lab42.sdi_sp_improvadoMonitor_adobeWeekly_perRun();              -- last completed week
  CALL prdrzranalytics.lab42.sdi_sp_improvadoMonitor_adobeWeekly_perRun(DATE'2026-11-08'); -- a given Sunday
  Schedule it after Improvado loads: Improvado writes finished weeks on Sunday about 02:00 UTC and some
  tables on Monday about 01:40 UTC, so Monday midday UTC is the first useful run, and Wednesday catches
  reloads. Every run is kept (history), the views show the latest.

HOW TO READ:
  SELECT * FROM prdrzranalytics.lab42.sdi_vw_improvadoMonitor_adobeWeekly_latestProblems;
  The suggested_next_step column says what to ask and of whom. Typical split: a missing, early or restated
  file is a question for Improvado; a drop, a jump or a channel mismatch with complete files is a question
  for the Adobe owner (a changed segment or event) with Improvado.
  A drop caused by a changed definition also makes re-pulled old weeks drop: ask for a re-pull of one
  earlier week as a test.

OBJECTS:
  prdrzranalytics.lab42.sdi_tbl_improvadoMonitor_adobeWeekly_history              all runs
  prdrzranalytics.lab42.sdi_vw_improvadoMonitor_adobeWeekly_latest                the latest run
  prdrzranalytics.lab42.sdi_vw_improvadoMonitor_adobeWeekly_latestProblems        the latest run, problems only, worst first

THRESHOLDS: the DECLARE lines at the top of the procedure. Tune them there.
EXTENDING: the list of raw tables (and each table's value column) is the VALUES list and the UNION ALL in
  the statement below. To watch more Improvado tables, add one branch and one VALUES row for each.
LIMITS: it only sees what Improvado landed. A file loaded after the week ended can still be incomplete if
  Adobe had not finished processing; the history checks catch that when it is far below normal.
================================================================================================= */

CREATE OR REPLACE PROCEDURE
  prdrzranalytics.lab42.sdi_sp_improvadoMonitor_adobeWeekly_perRun
(
  IN p_week_start DATE DEFAULT NULL
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Monitors the raw Improvado Adobe weekly tables for one week and suggests next steps. Separate from the PulseTMS pipeline.'
AS
BEGIN

  DECLARE v_run_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP();
  DECLARE v_run_id STRING;
  DECLARE v_week_start DATE;
  DECLARE v_week_end DATE;

  /* Thresholds */
  DECLARE v_min_prior_avg DOUBLE DEFAULT 20;
  DECLARE v_restate_ratio DOUBLE DEFAULT 0.80;
  DECLARE v_drop_ratio    DOUBLE DEFAULT 0.50;
  DECLARE v_jump_ratio    DOUBLE DEFAULT 2.00;
  DECLARE v_channel_lo    DOUBLE DEFAULT 0.90;
  DECLARE v_channel_hi    DOUBLE DEFAULT 1.10;

  SET v_run_id = CONCAT('IMPMON_', DATE_FORMAT(v_run_ts, 'yyyyMMdd_HHmmss_SSS'));

  /* Default: the last completed Sunday-Saturday week. DAYOFWEEK: Sunday = 1 ... Saturday = 7. */
  SET v_week_start = COALESCE(p_week_start, DATE_SUB(DATE_SUB(CURRENT_DATE(), DAYOFWEEK(CURRENT_DATE())), 6));

  IF DAYOFWEEK(v_week_start) <> 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'p_week_start must be a Sunday (the first day of a Sunday-Saturday week).';
  END IF;

  SET v_week_end = DATE_ADD(v_week_start, 6);

  IF v_week_end >= CURRENT_DATE() THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'That week has not ended yet. Monitor a completed week.';
  END IF;

  CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_improvadoMonitor_adobeWeekly_history (
    monitor_run_id        STRING    COMMENT 'One id per procedure run.',
    monitor_run_ts        TIMESTAMP COMMENT 'When the run started (UTC).',
    week_start            DATE      COMMENT 'Sunday that starts the monitored week (the date_yyyymmdd of the raw rows).',
    week_end              DATE      COMMENT 'Saturday that ends the monitored week.',
    check_name            STRING    COMMENT 'TABLE_WEEK = one raw table for the week; CHANNEL_SUM = All Channels table against the sum of its channel tables.',
    channel_group         STRING    COMMENT 'All Channels, Direct, Organic Search, Other, Paid Search, Paid Social, Programmatic; or "All Channels vs channels" for CHANNEL_SUM.',
    metric_name           STRING    COMMENT 'PulseTMS Adobe metric the raw table feeds.',
    source_table          STRING    COMMENT 'Raw Improvado table in prd_dbi_analytics.improvado.',
    value                 DOUBLE    COMMENT 'Value of the latest file for the week (CHANNEL_SUM: the All Channels value).',
    prior_4wk_avg         DOUBLE    COMMENT 'Average of the latest file of each of the 4 weeks before.',
    prior_weeks_count     BIGINT    COMMENT 'How many of those 4 weeks have a value.',
    ratio                 DOUBLE    COMMENT 'TABLE_WEEK: value / prior 4-week average. CHANNEL_SUM: sum of channels / All Channels.',
    load_count            BIGINT    COMMENT 'How many files the table holds for the week.',
    file_load_datetime    TIMESTAMP COMMENT 'When the latest file for the week was loaded.',
    hours_after_week_end  DOUBLE    COMMENT 'Hours between the end of the week (Sunday 00:00 UTC) and the latest file load. Negative = loaded before the week ended.',
    earlier_max_value     DOUBLE    COMMENT 'Highest value among earlier files for the same week (restatement check).',
    compare_value         DOUBLE    COMMENT 'CHANNEL_SUM: sum of the channel tables.',
    problem_code          STRING    COMMENT 'OK, NO_FILE_FOR_WEEK, LOADED_BEFORE_WEEK_ENDED, NULL_VALUE, ZERO_AFTER_NONZERO, RESTATED_DOWN, DROP_VS_HISTORY, JUMP_VS_HISTORY, ZERO_VALUE, CHANNEL_SUM_MISMATCH.',
    severity              STRING    COMMENT 'HIGH, MEDIUM, INFO or OK.',
    suggested_next_step   STRING    COMMENT 'What to ask, and of whom.',
    created_ts            TIMESTAMP COMMENT 'Row insert time.'
  )
  USING DELTA
  CLUSTER BY (week_start, monitor_run_ts)
  COMMENT 'Improvado Adobe weekly source monitor, outside the PulseTMS pipeline. One row per raw table per run, plus one row per additive metric for the channel-sum check.';

  WITH
  loads AS (
    SELECT 'All Channels' AS channel_group, 'upvHsi' AS metric_name, 'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo' AS source_table, TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') AS week_start, TRY_CAST(t.visitors AS DOUBLE) AS value, t.File_Load_datetime AS file_load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'upvByod', 'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'upvTotalAdobe', 'sdi_raw_pp_pro_uvnb_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_pp_pro_uvnb_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_all_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_all_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_all_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_all_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_allup_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_allup_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_allup_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_allup_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_allup_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_allup_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'All Channels', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'upvByod', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_direct_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_direct_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_direct_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_direct_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_direct_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_direct_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_direct_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_direct_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_direct_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_direct_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_direct_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_direct_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Direct', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'upvByod', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_organic_search_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_organic_search_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_organic_search_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_organic_search_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_organic_search_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_organic_search_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_organic_search_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_organic_search_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_organicsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_organicsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_organicsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_organicsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Organic Search', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'upvByod', 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_other_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_other_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_other_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_other_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_other_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_other_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_other_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_other_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_otherltc_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_otherltc_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_otherltc_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_otherltc_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Other', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'upvByod', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_paid_search_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_paid_search_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_paid_search_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_paid_search_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_paid_search_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_paid_search_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_paid_search_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_paid_search_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_paidsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_paidsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_paidsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_paidsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Search', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'upvByod', 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_social_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_social_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_social_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_social_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_social_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_social_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_social_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_social_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_social_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_social_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_social_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_social_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Paid Social', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'upvByod', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_programmatic_c2c_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_programmatic_c2c_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_programmatic_chat_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_programmatic_chat_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_programmatic_ec_completed_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.page_events AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_programmatic_ec_completed_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_programmatic_ec_successful_visits_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visits AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_ec_programmatic_ec_successful_visits_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_programmatic_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_programmatic_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_programmatic_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_programmatic_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event104 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event95 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.event21 AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.orders AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
    UNION ALL
    SELECT 'Programmatic', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo', TO_DATE(t.date_yyyymmdd, 'yyyyMMdd'), TRY_CAST(t.visitors AS DOUBLE), t.File_Load_datetime FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo t WHERE TO_DATE(t.date_yyyymmdd, 'yyyyMMdd') BETWEEN DATE_SUB(v_week_start, 28) AND v_week_start
  ),
  expected AS (
    SELECT * FROM VALUES
      ('All Channels', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_all_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('All Channels', 'upvByod', 'sdi_raw_adobe_pp_uvnb_all_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('All Channels', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_all_flow_total_visitors_weekly_tmo', FALSE),
      ('All Channels', 'upvTotalAdobe', 'sdi_raw_pp_pro_uvnb_weekly_tmo', FALSE),
      ('All Channels', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_all_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('All Channels', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_all_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('All Channels', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_all_byod_cartstart_visits_weekly_tmo', TRUE),
      ('All Channels', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_weekly_tmo', TRUE),
      ('All Channels', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_all_hsi_order_weekly_tmo', TRUE),
      ('All Channels', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_all_byod_order_weekly_tmo', TRUE),
      ('All Channels', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_all_postpaid_order_assisted_weekly_tmo', TRUE),
      ('All Channels', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_all_hsi_order_assisted_weekly_tmo', TRUE),
      ('All Channels', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_all_byod_order_assisted_weekly_tmo', TRUE),
      ('All Channels', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_all_c2c_weekly_tmo', FALSE),
      ('All Channels', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_all_chat_weekly_tmo', FALSE),
      ('All Channels', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo', TRUE),
      ('All Channels', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo', TRUE),
      ('All Channels', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_allup_weekly_tmo_tmo', FALSE),
      ('All Channels', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_allup_weekly_tmo_tmo', FALSE),
      ('All Channels', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo', TRUE),
      ('All Channels', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo', FALSE),
      ('All Channels', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo', TRUE),
      ('All Channels', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo', TRUE),
      ('All Channels', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_allup_weekly_tmo', TRUE),
      ('All Channels', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo', TRUE),
      ('All Channels', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_allup_weekly_tmo_tmo', FALSE),
      ('All Channels', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo', TRUE),
      ('All Channels', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo', FALSE),
      ('All Channels', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo', FALSE),
      ('Direct', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_postpaid_flow_visitors_weekly_tmo', FALSE),
      ('Direct', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('Direct', 'upvByod', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('Direct', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_direct_flow_total_visitors_weekly_tmo', FALSE),
      ('Direct', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_direct_uvnb_visitors_weekly_tmo', FALSE),
      ('Direct', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('Direct', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_direct_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('Direct', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_direct_byod_cartstart_visits_weekly_tmo', TRUE),
      ('Direct', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_weekly_tmo', TRUE),
      ('Direct', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_weekly_tmo', TRUE),
      ('Direct', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_direct_byod_order_weekly_tmo', TRUE),
      ('Direct', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_direct_postpaid_order_assisted_weekly_tmo', TRUE),
      ('Direct', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_direct_hsi_order_assisted_weekly_tmo', TRUE),
      ('Direct', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_direct_byod_order_assisted_weekly_tmo', TRUE),
      ('Direct', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_direct_c2c_weekly_tmo', FALSE),
      ('Direct', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_direct_chat_weekly_tmo', FALSE),
      ('Direct', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_direct_ec_completed_weekly_tmo', TRUE),
      ('Direct', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_direct_ec_successful_visits_weekly_tmo', TRUE),
      ('Direct', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_direct_weekly_tmo_tmo', FALSE),
      ('Direct', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_direct_weekly_tmo_tmo', FALSE),
      ('Direct', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo', TRUE),
      ('Direct', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo', FALSE),
      ('Direct', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo', TRUE),
      ('Direct', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo', TRUE),
      ('Direct', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_direct_weekly_tmo', TRUE),
      ('Direct', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo', TRUE),
      ('Direct', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo_tmo', FALSE),
      ('Direct', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo', TRUE),
      ('Direct', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo', FALSE),
      ('Direct', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo', FALSE),
      ('Organic Search', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_postpaid_flow_visitors_weekly_tmo', FALSE),
      ('Organic Search', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('Organic Search', 'upvByod', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('Organic Search', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_organic_search_flow_total_visitors_weekly_tmo', FALSE),
      ('Organic Search', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_organic_search_uvnb_visitors_weekly_tmo', FALSE),
      ('Organic Search', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('Organic Search', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('Organic Search', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_organic_search_byod_cartstart_visits_weekly_tmo', TRUE),
      ('Organic Search', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_weekly_tmo', TRUE),
      ('Organic Search', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_weekly_tmo', TRUE),
      ('Organic Search', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_weekly_tmo', TRUE),
      ('Organic Search', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_organic_search_postpaid_order_assisted_weekly_tmo', TRUE),
      ('Organic Search', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_organic_search_hsi_order_assisted_weekly_tmo', TRUE),
      ('Organic Search', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_organic_search_byod_order_assisted_weekly_tmo', TRUE),
      ('Organic Search', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_organic_search_c2c_weekly_tmo', FALSE),
      ('Organic Search', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_organic_search_chat_weekly_tmo', FALSE),
      ('Organic Search', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_organic_search_ec_completed_weekly_tmo', TRUE),
      ('Organic Search', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_organic_search_ec_successful_visits_weekly_tmo', TRUE),
      ('Organic Search', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_organicsearch_weekly_tmo_tmo', FALSE),
      ('Organic Search', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_organicsearch_weekly_tmo_tmo', FALSE),
      ('Organic Search', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo', TRUE),
      ('Organic Search', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo', FALSE),
      ('Organic Search', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo', TRUE),
      ('Organic Search', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo', TRUE),
      ('Organic Search', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_organicsearch_weekly_tmo', TRUE),
      ('Organic Search', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo', TRUE),
      ('Organic Search', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo_tmo', FALSE),
      ('Organic Search', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo', TRUE),
      ('Organic Search', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo', FALSE),
      ('Organic Search', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo', FALSE),
      ('Other', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_other_uvnb_postpaid_flow_visitors_weekly_tmo', FALSE),
      ('Other', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_other_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('Other', 'upvByod', 'sdi_raw_adobe_pp_uvnb_other_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('Other', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_other_flow_total_visitors_weekly_tmo', FALSE),
      ('Other', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_other_uvnb_visitors_weekly_tmo', FALSE),
      ('Other', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_other_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('Other', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_other_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('Other', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_other_byod_cartstart_visits_weekly_tmo', TRUE),
      ('Other', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_weekly_tmo', TRUE),
      ('Other', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_other_hsi_order_weekly_tmo', TRUE),
      ('Other', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_other_byod_order_weekly_tmo', TRUE),
      ('Other', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_other_postpaid_order_assisted_weekly_tmo', TRUE),
      ('Other', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_other_hsi_order_assisted_weekly_tmo', TRUE),
      ('Other', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_other_byod_order_assisted_weekly_tmo', TRUE),
      ('Other', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_other_c2c_weekly_tmo', FALSE),
      ('Other', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_other_chat_weekly_tmo', FALSE),
      ('Other', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_other_ec_completed_weekly_tmo', TRUE),
      ('Other', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_other_ec_successful_visits_weekly_tmo', TRUE),
      ('Other', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_otherltc_weekly_tmo_tmo', FALSE),
      ('Other', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_otherltc_weekly_tmo_tmo', FALSE),
      ('Other', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo', TRUE),
      ('Other', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo', FALSE),
      ('Other', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo', TRUE),
      ('Other', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo', TRUE),
      ('Other', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_otherltc_weekly_tmo', TRUE),
      ('Other', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo', TRUE),
      ('Other', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo_tmo', FALSE),
      ('Other', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo', TRUE),
      ('Other', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo', FALSE),
      ('Other', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo', FALSE),
      ('Paid Search', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_postpaid_flow_visitors_weekly_tmo', FALSE),
      ('Paid Search', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('Paid Search', 'upvByod', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('Paid Search', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_paid_search_flow_total_visitors_weekly_tmo', FALSE),
      ('Paid Search', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_paid_search_uvnb_visitors_weekly_tmo', FALSE),
      ('Paid Search', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('Paid Search', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('Paid Search', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_paid_search_byod_cartstart_visits_weekly_tmo', TRUE),
      ('Paid Search', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_weekly_tmo', TRUE),
      ('Paid Search', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_weekly_tmo', TRUE),
      ('Paid Search', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_weekly_tmo', TRUE),
      ('Paid Search', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_paid_search_postpaid_order_assisted_weekly_tmo', TRUE),
      ('Paid Search', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_paid_search_hsi_order_assisted_weekly_tmo', TRUE),
      ('Paid Search', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_paid_search_byod_order_assisted_weekly_tmo', TRUE),
      ('Paid Search', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_paid_search_c2c_weekly_tmo', FALSE),
      ('Paid Search', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_paid_search_chat_weekly_tmo', FALSE),
      ('Paid Search', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_paid_search_ec_completed_weekly_tmo', TRUE),
      ('Paid Search', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_paid_search_ec_successful_visits_weekly_tmo', TRUE),
      ('Paid Search', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_paidsearch_weekly_tmo_tmo', FALSE),
      ('Paid Search', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_paidsearch_weekly_tmo_tmo', FALSE),
      ('Paid Search', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo', TRUE),
      ('Paid Search', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo', FALSE),
      ('Paid Search', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo', TRUE),
      ('Paid Search', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo', TRUE),
      ('Paid Search', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_paidsearch_weekly_tmo', TRUE),
      ('Paid Search', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo', TRUE),
      ('Paid Search', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo_tmo', FALSE),
      ('Paid Search', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo', TRUE),
      ('Paid Search', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo', FALSE),
      ('Paid Search', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo', FALSE),
      ('Paid Social', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_social_uvnb_postpaid_flow_visitors_weekly_tmo', FALSE),
      ('Paid Social', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_social_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('Paid Social', 'upvByod', 'sdi_raw_adobe_pp_uvnb_social_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('Paid Social', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_social_flow_total_visitors_weekly_tmo', FALSE),
      ('Paid Social', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_social_uvnb_visitors_weekly_tmo', FALSE),
      ('Paid Social', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_social_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('Paid Social', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_social_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('Paid Social', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_social_byod_cartstart_visits_weekly_tmo', TRUE),
      ('Paid Social', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_weekly_tmo', TRUE),
      ('Paid Social', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_social_hsi_order_weekly_tmo', TRUE),
      ('Paid Social', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_social_byod_order_weekly_tmo', TRUE),
      ('Paid Social', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_social_postpaid_order_assisted_weekly_tmo', TRUE),
      ('Paid Social', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_social_hsi_order_assisted_weekly_tmo', TRUE),
      ('Paid Social', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_social_byod_order_assisted_weekly_tmo', TRUE),
      ('Paid Social', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_social_c2c_weekly_tmo', FALSE),
      ('Paid Social', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_social_chat_weekly_tmo', FALSE),
      ('Paid Social', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_social_ec_completed_weekly_tmo', TRUE),
      ('Paid Social', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_social_ec_successful_visits_weekly_tmo', TRUE),
      ('Paid Social', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_social_weekly_tmo_tmo', FALSE),
      ('Paid Social', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_social_weekly_tmo_tmo', FALSE),
      ('Paid Social', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo', TRUE),
      ('Paid Social', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo', FALSE),
      ('Paid Social', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo', TRUE),
      ('Paid Social', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo', TRUE),
      ('Paid Social', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_social_weekly_tmo', TRUE),
      ('Paid Social', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo', TRUE),
      ('Paid Social', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo_tmo', FALSE),
      ('Paid Social', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo', TRUE),
      ('Paid Social', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo', FALSE),
      ('Paid Social', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo', FALSE),
      ('Programmatic', 'upvPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_postpaid_flow_visitors_weekly_tmo', FALSE),
      ('Programmatic', 'upvHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_hsi_flow_visitors_weekly_tmo', FALSE),
      ('Programmatic', 'upvByod', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_byod_flow_visitors_weekly_tmo', FALSE),
      ('Programmatic', 'upvFlowTotal', 'sdi_raw_adobe_pp_uvnb_programmatic_flow_total_visitors_weekly_tmo', FALSE),
      ('Programmatic', 'upvTotalAdobe', 'sdi_raw_adobe_pp_uvnb_programmatic_uvnb_visitors_weekly_tmo', FALSE),
      ('Programmatic', 'cartstartPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_cartstart_visits_weekly_tmo', TRUE),
      ('Programmatic', 'cartstartHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_cartstart_visits_weekly_tmo', TRUE),
      ('Programmatic', 'cartstartByod', 'sdi_raw_adobe_pp_uvnb_programmatic_byod_cartstart_visits_weekly_tmo', TRUE),
      ('Programmatic', 'ordersUnassistedPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_weekly_tmo', TRUE),
      ('Programmatic', 'ordersUnassistedHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_weekly_tmo', TRUE),
      ('Programmatic', 'ordersUnassistedByod', 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_weekly_tmo', TRUE),
      ('Programmatic', 'ordersAssistedPostpaid', 'sdi_raw_adobe_pp_uvnb_programmatic_postpaid_order_assisted_weekly_tmo', TRUE),
      ('Programmatic', 'ordersAssistedHsi', 'sdi_raw_adobe_pp_uvnb_programmatic_hsi_order_assisted_weekly_tmo', TRUE),
      ('Programmatic', 'ordersAssistedByod', 'sdi_raw_adobe_pp_uvnb_programmatic_byod_order_assisted_weekly_tmo', TRUE),
      ('Programmatic', 'c2cVisitors', 'sdi_raw_adobe_pp_upv_programmatic_c2c_weekly_tmo', FALSE),
      ('Programmatic', 'chatEngagementVisitors', 'sdi_raw_adobe_pp_upv_programmatic_chat_weekly_tmo', FALSE),
      ('Programmatic', 'ecCompletedPageEvents', 'sdi_raw_adobe_pp_ec_programmatic_ec_completed_weekly_tmo', TRUE),
      ('Programmatic', 'ecSuccessfulVisits', 'sdi_raw_adobe_pp_ec_programmatic_ec_successful_visits_weekly_tmo', TRUE),
      ('Programmatic', 'metroProspectVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'metroHomepageEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'metroDealsEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'metroProductPlansEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'metroOtherEntryVisitors', 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'metroActivationIntentVisitors', 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'metroCartStartVisitors', 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visitors_programmatic_weekly_tmo_tmo', FALSE),
      ('Programmatic', 'metroProspectClicktocallVisitors', 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_visitors_programmatic_weekly_tmo_tmo', FALSE),
      ('Programmatic', 'metroOrderConfirmationOrders', 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo', TRUE),
      ('Programmatic', 'tfbExclSinglePageVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo', FALSE),
      ('Programmatic', 'tfbLeadgenFormCompleteEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo', TRUE),
      ('Programmatic', 'tfbCreditCheckEvent104', 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo', TRUE),
      ('Programmatic', 'tfbAllClicktocallEvent95', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_clicktocall_programmatic_weekly_tmo', TRUE),
      ('Programmatic', 'tfbAllChatEngagementEvent21', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo', TRUE),
      ('Programmatic', 'tfbCartStartsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo_tmo', FALSE),
      ('Programmatic', 'tfbEcommOrderHitOrders', 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo', TRUE),
      ('Programmatic', 'tfbClicktocallExistsVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo', FALSE),
      ('Programmatic', 'tfbChatEngagementVisitors', 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo', FALSE)
    AS t(channel_group, metric_name, source_table, is_additive)
  ),
  latest_per_week AS (
    SELECT source_table, week_start, value, file_load_datetime
    FROM loads
    QUALIFY ROW_NUMBER() OVER (PARTITION BY source_table, week_start ORDER BY file_load_datetime DESC) = 1
  ),
  load_stats AS (
    SELECT source_table, week_start, COUNT(*) AS load_count
    FROM loads
    GROUP BY source_table, week_start
  ),
  earlier_loads AS (
    SELECT l.source_table, l.week_start, MAX(l.value) AS earlier_max_value
    FROM loads l
    JOIN latest_per_week p ON p.source_table = l.source_table AND p.week_start = l.week_start
    WHERE l.file_load_datetime < p.file_load_datetime
    GROUP BY l.source_table, l.week_start
  ),
  prior AS (
    SELECT source_table, AVG(value) AS prior_avg, COUNT(value) AS prior_weeks
    FROM latest_per_week
    WHERE week_start < v_week_start AND week_start >= DATE_SUB(v_week_start, 28)
    GROUP BY source_table
  ),
  joined AS (
    SELECT e.channel_group, e.metric_name, e.source_table, e.is_additive,
           (c.source_table IS NOT NULL) AS has_file,
           c.value, c.file_load_datetime,
           COALESCE(ls.load_count, 0) AS load_count,
           em.earlier_max_value,
           pr.prior_avg, COALESCE(pr.prior_weeks, 0) AS prior_weeks,
           (unix_timestamp(c.file_load_datetime) - unix_timestamp(CAST(DATE_ADD(v_week_end, 1) AS TIMESTAMP))) / 3600.0 AS hours_after_week_end
    FROM expected e
    LEFT JOIN latest_per_week c ON c.source_table = e.source_table AND c.week_start = v_week_start
    LEFT JOIN load_stats ls ON ls.source_table = e.source_table AND ls.week_start = v_week_start
    LEFT JOIN earlier_loads em ON em.source_table = e.source_table AND em.week_start = v_week_start
    LEFT JOIN prior pr ON pr.source_table = e.source_table
  ),
  table_rows AS (
    SELECT j.*,
           CASE
             WHEN NOT j.has_file                                                                       THEN 'NO_FILE_FOR_WEEK'
             WHEN j.file_load_datetime < CAST(DATE_ADD(v_week_end, 1) AS TIMESTAMP)                                                 THEN 'LOADED_BEFORE_WEEK_ENDED'
             WHEN j.value IS NULL                                                                      THEN 'NULL_VALUE'
             WHEN j.value = 0 AND j.prior_avg >= v_min_prior_avg                                  THEN 'ZERO_AFTER_NONZERO'
             WHEN j.load_count > 1 AND j.earlier_max_value >= v_min_prior_avg
                  AND j.value < v_restate_ratio * j.earlier_max_value                             THEN 'RESTATED_DOWN'
             WHEN j.prior_avg >= v_min_prior_avg AND j.value <= v_drop_ratio * j.prior_avg   THEN 'DROP_VS_HISTORY'
             WHEN j.prior_avg >= v_min_prior_avg AND j.value >= v_jump_ratio * j.prior_avg   THEN 'JUMP_VS_HISTORY'
             WHEN j.value = 0                                                                          THEN 'ZERO_VALUE'
             ELSE 'OK'
           END AS problem_code
    FROM joined j
  ),
  channel_rows AS (
    SELECT metric_name,
           MAX(CASE WHEN channel_group = 'All Channels' AND has_file AND problem_code <> 'LOADED_BEFORE_WEEK_ENDED' THEN value END)  AS all_value,
           SUM(CASE WHEN channel_group <> 'All Channels' AND has_file AND problem_code <> 'LOADED_BEFORE_WEEK_ENDED' THEN value END) AS channel_sum,
           MAX(CASE WHEN channel_group = 'All Channels' THEN source_table END)                                                       AS all_table,
           COUNT(CASE WHEN channel_group <> 'All Channels' THEN 1 END)                                                               AS channels_expected,
           COUNT(CASE WHEN channel_group <> 'All Channels' AND has_file AND problem_code <> 'LOADED_BEFORE_WEEK_ENDED' THEN 1 END)  AS channels_usable
    FROM table_rows
    WHERE is_additive
    GROUP BY metric_name
  ),
  channel_classified AS (
    SELECT c.*,
           c.channel_sum / NULLIF(c.all_value, 0) AS ratio,
           CASE
             WHEN GREATEST(COALESCE(c.all_value, 0), COALESCE(c.channel_sum, 0)) < v_min_prior_avg THEN 'OK'
             WHEN c.all_value = 0 AND c.channel_sum > 0                                          THEN 'CHANNEL_SUM_MISMATCH'
             WHEN c.channel_sum / NULLIF(c.all_value, 0) > v_channel_hi                     THEN 'CHANNEL_SUM_MISMATCH'
             WHEN c.channel_sum / NULLIF(c.all_value, 0) < v_channel_lo                     THEN 'CHANNEL_SUM_MISMATCH'
             ELSE 'OK'
           END AS problem_code
    FROM channel_rows c
    WHERE c.all_value IS NOT NULL AND c.channels_expected > 0 AND c.channels_usable = c.channels_expected
  ),
  final_rows AS (
    SELECT 'TABLE_WEEK' AS check_name, channel_group, metric_name, source_table, value,
           prior_avg AS prior_4wk_avg, prior_weeks AS prior_weeks_count,
           value / NULLIF(prior_avg, 0) AS ratio,
           load_count, file_load_datetime, hours_after_week_end, earlier_max_value,
           CAST(NULL AS DOUBLE) AS compare_value, problem_code
    FROM table_rows
    UNION ALL
    SELECT 'CHANNEL_SUM', 'All Channels vs channels', metric_name, all_table, all_value,
           CAST(NULL AS DOUBLE), 0, ratio,
           0, CAST(NULL AS TIMESTAMP), CAST(NULL AS DOUBLE), CAST(NULL AS DOUBLE),
           channel_sum, problem_code
    FROM channel_classified
  )
  INSERT INTO prdrzranalytics.lab42.sdi_tbl_improvadoMonitor_adobeWeekly_history
  (monitor_run_id, monitor_run_ts, week_start, week_end, check_name, channel_group, metric_name, source_table, value, prior_4wk_avg, prior_weeks_count, ratio, load_count, file_load_datetime, hours_after_week_end, earlier_max_value, compare_value, problem_code, severity, suggested_next_step, created_ts)
  SELECT v_run_id, v_run_ts, v_week_start, v_week_end, check_name, channel_group, metric_name, source_table, value, prior_4wk_avg, prior_weeks_count, ratio, load_count, file_load_datetime, hours_after_week_end, earlier_max_value, compare_value, problem_code, severity, suggested_next_step, current_timestamp()
  FROM (
  SELECT check_name, channel_group, metric_name, source_table, value, prior_4wk_avg, prior_weeks_count, ratio,
         load_count, file_load_datetime, hours_after_week_end, earlier_max_value, compare_value, problem_code,
         CASE
           WHEN problem_code = 'OK' THEN 'OK'
           WHEN problem_code IN ('NO_FILE_FOR_WEEK', 'LOADED_BEFORE_WEEK_ENDED', 'NULL_VALUE', 'ZERO_AFTER_NONZERO') THEN 'HIGH'
           WHEN problem_code = 'CHANNEL_SUM_MISMATCH' AND (ratio > 1.5 OR ratio < 0.67 OR ratio IS NULL) THEN 'HIGH'
           WHEN problem_code = 'ZERO_VALUE' THEN 'INFO'
           ELSE 'MEDIUM'
         END AS severity,
         CASE problem_code
           WHEN 'OK' THEN 'No action required.'
           WHEN 'NO_FILE_FOR_WEEK' THEN CONCAT('Ask Improvado to run the weekly load for this week: there is no file in ', source_table, '.')
           WHEN 'LOADED_BEFORE_WEEK_ENDED' THEN CONCAT('The only file was loaded ', CAST(ROUND(-hours_after_week_end / 24.0, 1) AS STRING), ' day(s) before the week ended, so ', COALESCE(CAST(CAST(ROUND(value, 0) AS BIGINT) AS STRING), 'its value'), ' is a partial count. Ask Improvado to reload this week for ', source_table, '.')
           WHEN 'NULL_VALUE' THEN CONCAT('The file exists but the value is empty. Ask Improvado to check the extract for ', source_table, ' and reload the week.')
           WHEN 'ZERO_AFTER_NONZERO' THEN CONCAT('Value is 0 against a prior 4-week average of ', CAST(CAST(ROUND(prior_4wk_avg, 0) AS BIGINT) AS STRING), '. Check whether the file is stale or the Adobe segment returned nothing; ask Improvado to re-pull ', source_table, ' for this week.')
           WHEN 'RESTATED_DOWN' THEN CONCAT('A later file (', CAST(CAST(ROUND(value, 0) AS BIGINT) AS STRING), ') is lower than an earlier one (', CAST(CAST(ROUND(earlier_max_value, 0) AS BIGINT) AS STRING), ') for this week. Ask Improvado which file is right and why the value was restated.')
           WHEN 'DROP_VS_HISTORY' THEN CONCAT('Value is ', CAST(ROUND(100 * ratio, 0) AS STRING), '% of its prior 4-week average. Ask the Adobe owner whether the segment, event or report behind ', source_table, ' changed; re-pull an earlier week as a test (a changed definition makes old weeks drop too).')
           WHEN 'JUMP_VS_HISTORY' THEN CONCAT('Value is ', CAST(ROUND(100 * ratio, 0) AS STRING), '% of its prior 4-week average. Check for a campaign or event, a duplicated load, or a changed definition behind ', source_table, '.')
           WHEN 'ZERO_VALUE' THEN 'Value is 0 on a low-volume metric. Usually harmless; check only if the metric should be active.'
           WHEN 'CHANNEL_SUM_MISMATCH' THEN CONCAT('All Channels is ', CAST(CAST(ROUND(value, 0) AS BIGINT) AS STRING), ' but the channel tables sum to ', CAST(CAST(ROUND(compare_value, 0) AS BIGINT) AS STRING), ' (ratio ', CAST(ROUND(ratio, 2) AS STRING), '). Ask which extract is correct and whether the All and channel tables use the same segments.')
           ELSE 'Review.'
         END AS suggested_next_step
  FROM final_rows
  ) x;

  CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_improvadoMonitor_adobeWeekly_latest
  COMMENT 'Rows of the latest Improvado monitor run.'
  AS
  SELECT h.*
  FROM prdrzranalytics.lab42.sdi_tbl_improvadoMonitor_adobeWeekly_history h
  WHERE h.monitor_run_id = (SELECT x.monitor_run_id FROM prdrzranalytics.lab42.sdi_tbl_improvadoMonitor_adobeWeekly_history x ORDER BY x.monitor_run_ts DESC, x.monitor_run_id DESC LIMIT 1);

  CREATE OR REPLACE VIEW prdrzranalytics.lab42.sdi_vw_improvadoMonitor_adobeWeekly_latestProblems
  COMMENT 'Problems found by the latest Improvado monitor run, worst first, with what to ask and of whom.'
  AS
  SELECT week_start, severity, problem_code, check_name, channel_group, metric_name, source_table,
         value, prior_4wk_avg, ratio, file_load_datetime, suggested_next_step
  FROM prdrzranalytics.lab42.sdi_vw_improvadoMonitor_adobeWeekly_latest
  WHERE problem_code <> 'OK'
  ORDER BY CASE severity WHEN 'HIGH' THEN 1 WHEN 'MEDIUM' THEN 2 ELSE 3 END, problem_code, metric_name, channel_group;

END;