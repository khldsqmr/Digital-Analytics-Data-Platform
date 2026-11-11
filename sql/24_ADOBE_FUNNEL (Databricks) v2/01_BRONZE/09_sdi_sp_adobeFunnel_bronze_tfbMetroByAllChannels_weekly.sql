/* =================================================================================================
FILE: 09_sdi_sp_adobeFunnel_bronze_tfbMetroByAllChannels_weekly.sql   
LAYER: Bronze Table (via Stored Procedure)
DATASET: prdrzranalytics.lab42
TABLE: sdi_tbl_adobeFunnel_bronze_tfbMetroByAllChannels_weekly
PROCEDURE: sdi_sp_adobeFunnel_bronze_tfbMetroByAllChannels_weekly

PURPOSE:
  Canonical Bronze weekly Adobe TFB and Metro metrics at ALL_CHANNELS granularity, side by side in one table
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
  One row per: WeekSunSat

BUSINESS RULES:
  - WeekSunSat = date_yyyymmdd + 6 days (raw date_yyyymmdd is the week-starting Sunday).
  - DataGranularity is fixed as ALL_CHANNELS.
  - LastTouchChannel is NULL.
  - LtcGroup is NULL.
  - Raw group tokens map to output labels: direct=Direct, organicsearch=Organic Search, otherltc=Other,
    paidsearch=Paid Search, programmatic=Programmatic, social=Paid Social, allup=ALL (LtcGroup NULL).
  - TFB and Metro are separate brands: never add them together; the brand prefix is part of every column name.
  - Column names end in their unit (Visitors / Visits / Orders / Events / PageVisitors) — do not mix units in ratios.
  - Sources avoid the `_tmo_tmo` tables wherever a clean-named table carries the needed column.
    Forced `_tmo_tmo` sources: TfbChatVisitors, TfbC2cVisitors.
  - Missing metric values remain NULL.
  - FAILURE SCOPE: one missing source table fails this whole procedure (both brands). Run the pre-flight first.

KEY DEDUPE RULE:
  - One row per source table per week, latest File_Load_datetime, then Filename, then __insert_date.

SOURCES (16 tables):
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visits_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo
  prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo
================================================================================================= */

CREATE OR REPLACE PROCEDURE
prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_tfbMetroByAllChannels_weekly()
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
AS
BEGIN

  CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_adobeFunnel_bronze_tfbMetroByAllChannels_weekly
  USING DELTA
  AS

WITH RawUnion AS (
  SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6) AS WeekSunSat, 'ALL_CHANNELS' AS DataGranularity, CAST(NULL AS STRING) AS LastTouchChannel, CAST(NULL AS STRING) AS LtcGroup, 'TfbOrders' AS MetricName, TRY_CAST(orders AS DOUBLE) AS MetricValue, 'sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo' AS SourceTable, __insert_date AS InsertDate, File_Load_datetime AS FileLoadDatetime, Filename AS Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visits_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visits_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visits_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visits_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbChatVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbCreditCheckEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbLeadgenFormCompleteEvents', TRY_CAST(event104 AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'TfbVisitorsExclSinglePage', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroOrders', TRY_CAST(orders AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroCartstartVisits', TRY_CAST(visits AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroCartstartVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroC2cVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroActivationIntentVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroEntryHomePageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroEntryPlansPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroEntryDealsPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroEntryOtherPageVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo
  UNION ALL SELECT DATE_ADD(TO_DATE(date_yyyymmdd, 'yyyyMMdd'), 6), 'ALL_CHANNELS', CAST(NULL AS STRING), CAST(NULL AS STRING), 'MetroProspectVisitors', TRY_CAST(visitors AS DOUBLE), 'sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo', __insert_date, File_Load_datetime, Filename FROM prd_dbi_analytics.improvado.sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo
),

Deduped AS (
  SELECT *
  FROM RawUnion
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      WeekSunSat,
      DataGranularity,
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