/* =================================================================================================
FILE: 10_sdi_sp_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly.sql   
LAYER: Bronze Table (via Stored Procedure)
DATASET: prdrzranalytics.lab42
TABLE: sdi_tbl_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly
PROCEDURE: sdi_sp_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly

PURPOSE:
  Canonical Bronze weekly Adobe TFB and Metro metrics at LTC_GROUPS granularity, side by side in one table
  (Tfb* columns and Metro* columns), the same way Postpaid / HSI / BYOD sit side by side in the funnel Bronze.
  Separate from the postpaid/HSI/BYOD funnel Bronze tables; these metrics are NOT mixed into those columns.

METRICS (column <- value column of source family):
  [TFB] TfbOrders  <- orders of ...ecomm_order_hit_orders_{group}_weekly_tmo
  [TFB] TfbCartstartVisits  <- visits of ...cart_starts_visitors_{group}_weekly_tmo  (all-up: ...cart_starts_visits_allup_weekly_tmo)
  [TFB] TfbCartstartVisitors  <- visitors of ...cart_starts_visitors_{group}_weekly_tmo  (all-up: ...cart_starts_visits_allup_weekly_tmo)
  [TFB] TfbChatVisitors  <- visitors of ...alltfb_chatengagement_{group}_weekly_tmo_tmo
  [TFB] TfbC2cVisitors  <- visitors of ...clicktocall_exists_visitors_{group}_weekly_tmo_tmo
  [TFB] TfbCreditCheckEvents  <- event104 of ...credit_check_ev104_{group}_weekly_tmo
  [TFB] TfbLeadgenFormCompleteEvents  <- event104 of ...leadgen_form_complete_ev104_{group}_weekly_tmo
  [TFB] TfbVisitorsExclSinglePage  <- visitors of ...excl_single_page_visitors_{group}_weekly_tmo
  [Metro] MetroOrders  <- orders of ...order_confirmation_orders_{group}_weekly_tmo
  [Metro] MetroCartstartVisits  <- visits of ...cart_start_visits_{group}_weekly_tmo
  [Metro] MetroCartstartVisitors  <- visitors of ...cart_start_visits_{group}_weekly_tmo
  [Metro] MetroC2cVisitors  <- visitors of ...prospect_clicktocall_{group}_weekly_tmo
  [Metro] MetroActivationIntentVisitors  <- visitors of ...activation_intent_visitors_{group}_weekly_tmo
  [Metro] MetroEntryHomePageVisitors  <- visitors of ...homepage_entry_visitors_{group}_weekly_tmo
  [Metro] MetroEntryPlansPageVisitors  <- visitors of ...product_plans_entry_visitors_{group}_weekly_tmo
  [Metro] MetroEntryDealsPageVisitors  <- visitors of ...deals_entry_visitors_{group}_weekly_tmo
  [Metro] MetroEntryOtherPageVisitors  <- visitors of ...other_entry_visitors_{group}_weekly_tmo
  [Metro] MetroProspectVisitors  <- visitors of ...prospect_visitors_{group}_weekly_tmo

BUSINESS GRAIN:
  One row per: WeekSunSat + LtcGroup

BUSINESS RULES:
  - WeekSunSat = date_yyyymmdd + 6 days (raw date_yyyymmdd is the week-starting Sunday).
  - DataGranularity is fixed as LTC_GROUPS.
  - LastTouchChannel is NULL.
  - Raw group tokens map to output labels: direct=Direct, organicsearch=Organic Search, otherltc=Other,
    paidsearch=Paid Search, programmatic=Programmatic, social=Paid Social.
  - TFB and Metro are separate brands: never add them together; the brand prefix is part of every column name.
  - Column names end in their unit (Visitors / Visits / Orders / Events / PageVisitors) — do not mix units in ratios.
  - Sources avoid the `_tmo_tmo` tables wherever a clean-named table carries the needed column.
    Forced `_tmo_tmo` sources: TfbChatVisitors, TfbC2cVisitors.
  - Missing metric values remain NULL.
  - FAILURE SCOPE: one missing source table fails this whole procedure (both brands). Run the pre-flight first.

KEY DEDUPE RULE:
  - One row per source table per week + LtcGroup, latest File_Load_datetime, then Filename, then __insert_date.

SOURCES (96 tables):
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly
  USING DELTA
  AS

WITH RawUnion AS (
  /* ------------------------------------------------ DIRECT (direct) ------------------------------------------------ */
  SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat, 'LTC_GROUPS' AS DataGranularity, CAST(NULL AS STRING) AS LastTouchChannel, 'Direct' AS LtcGroup, 'TfbOrders' AS MetricName, TRY_CAST(orders AS DOUBLE) AS MetricValue, 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename AS Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Direct', 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo
  /* ------------------------------------------------ ORGANIC SEARCH (organicsearch) ------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Organic Search', 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo
  /* ------------------------------------------------ OTHER (otherltc) ------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Other', 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo
  /* ------------------------------------------------ PAID SEARCH (paidsearch) ------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Search', 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo
  /* ------------------------------------------------ PROGRAMMATIC (programmatic) ------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Programmatic', 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo
  /* ------------------------------------------------ PAID SOCIAL (social) ------------------------------------------------ */
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'LTC_GROUPS', CAST(NULL AS STRING), 'Paid Social', 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
      LtcGroup,
      MetricName,
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

  -- TFB
  MAX(IF(MetricName = 'TfbOrders', MetricValue, NULL)) AS TfbOrders,
  MAX(IF(MetricName = 'TfbCartstartVisits', MetricValue, NULL)) AS TfbCartstartVisits,
  MAX(IF(MetricName = 'TfbCartstartVisitors', MetricValue, NULL)) AS TfbCartstartVisitors,
  MAX(IF(MetricName = 'TfbChatVisitors', MetricValue, NULL)) AS TfbChatVisitors,
  MAX(IF(MetricName = 'TfbC2cVisitors', MetricValue, NULL)) AS TfbC2cVisitors,
  MAX(IF(MetricName = 'TfbCreditCheckEvents', MetricValue, NULL)) AS TfbCreditCheckEvents,
  MAX(IF(MetricName = 'TfbLeadgenFormCompleteEvents', MetricValue, NULL)) AS TfbLeadgenFormCompleteEvents,
  MAX(IF(MetricName = 'TfbVisitorsExclSinglePage', MetricValue, NULL)) AS TfbVisitorsExclSinglePage,
  -- Metro
  MAX(IF(MetricName = 'MetroOrders', MetricValue, NULL)) AS MetroOrders,
  MAX(IF(MetricName = 'MetroCartstartVisits', MetricValue, NULL)) AS MetroCartstartVisits,
  MAX(IF(MetricName = 'MetroCartstartVisitors', MetricValue, NULL)) AS MetroCartstartVisitors,
  MAX(IF(MetricName = 'MetroC2cVisitors', MetricValue, NULL)) AS MetroC2cVisitors,
  MAX(IF(MetricName = 'MetroActivationIntentVisitors', MetricValue, NULL)) AS MetroActivationIntentVisitors,
  MAX(IF(MetricName = 'MetroEntryHomePageVisitors', MetricValue, NULL)) AS MetroEntryHomePageVisitors,
  MAX(IF(MetricName = 'MetroEntryPlansPageVisitors', MetricValue, NULL)) AS MetroEntryPlansPageVisitors,
  MAX(IF(MetricName = 'MetroEntryDealsPageVisitors', MetricValue, NULL)) AS MetroEntryDealsPageVisitors,
  MAX(IF(MetricName = 'MetroEntryOtherPageVisitors', MetricValue, NULL)) AS MetroEntryOtherPageVisitors,
  MAX(IF(MetricName = 'MetroProspectVisitors', MetricValue, NULL)) AS MetroProspectVisitors,

  ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(SourceTable)), ', ') AS SourceTablesUsed,
  MAX(FileLoadDatetime) AS MaxFileLoadDatetime,
  ARRAY_JOIN(SORT_ARRAY(COLLECT_SET(Filename)), ', ') AS FilenamesUsed

FROM Deduped
GROUP BY
  WeekSunSat,
  DataGranularity,
  LastTouchChannel,
  LtcGroup;

END;