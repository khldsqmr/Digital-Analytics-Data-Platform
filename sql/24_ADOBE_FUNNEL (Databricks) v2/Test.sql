/* =================================================================================================
DEPLOY + VALIDATE — Adobe funnel: C2C, Chat engagement, broadband EC, TFB, Metro
Run these in order. Each step is a separate statement block; stop at the first unexpected result.

WHAT CHANGED (everything else is untouched):
  Bronze 01, 02  : modified  (+ C2cVisitors, ChatEngagementVisitors, BroadbandEcCompletedPageEvents, BroadbandEcSuccessfulVisits)
  Bronze 09, 10  : NEW       (tfbMetroByAllChannels / tfbMetroByLtcGroups — 8 Tfb* + 10 Metro* columns)
  Silver 01      : modified  (+ the 4 funnel columns above, + 18 TFB/Metro columns via one LEFT JOIN)
  Gold           : modified  (37 -> 59 columns; still 3 joins: funnel, BYOD entry pages, BYOD outcomes)
  Orchestrator   : modified  (sdi_sp_adobeFunnel_orchestration_allLayers_weekly — the one the scheduled job runs — +2 CALLs
                   for Bronze 09 and 10; the older sdi_sp_adobeFunnel_call_all_weekly has the same change, keep them in sync or retire one)
UNCHANGED: Bronze 03-08, Silver 02, Silver 03.
================================================================================================= */


/* ===== STEP 0 — BACKUP (Lab42 naming: <table>_bkup_YYYYMMDD). Silver 01 and Gold only; Bronze rebuilds from raw. ===== */
CREATE TABLE prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly_bkup_20261106 AS SELECT * FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly;
CREATE TABLE prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly_bkup_20261106 AS SELECT * FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly;


/* ===== STEP 1 — PRE-FLIGHT: expect ZERO rows. (140 raw tables that are new to the pipeline.)
   A missing table makes its whole Bronze procedure fail. ===== */
WITH expected(table_name) AS (VALUES
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_activation_intent_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_cart_start_visits_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_deals_entry_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_homepage_entry_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_order_confirmation_orders_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_other_entry_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_product_plans_entry_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_clicktocall_social_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_metro_mpulse_pa_prospect_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_all_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_all_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_direct_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_direct_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_organic_search_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_organic_search_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_other_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_other_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_paid_search_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_paid_search_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_programmatic_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_programmatic_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_social_ec_completed_weekly_tmo'),
  ('sdi_raw_adobe_pp_ec_social_ec_successful_visits_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_all_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_all_chat_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_direct_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_direct_chat_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_organic_search_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_organic_search_chat_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_other_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_other_chat_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_paid_search_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_paid_search_chat_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_programmatic_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_programmatic_chat_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_social_c2c_weekly_tmo'),
  ('sdi_raw_adobe_pp_upv_social_chat_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_allup_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_direct_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_organicsearch_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_otherltc_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_paidsearch_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_programmatic_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_alltfb_chatengagement_social_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_cart_starts_visits_allup_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_allup_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_direct_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_organicsearch_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_otherltc_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_paidsearch_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_programmatic_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_clicktocall_exists_visitors_social_weekly_tmo_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_allup_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_direct_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_credit_check_ev104_social_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_allup_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_direct_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_ecomm_order_hit_orders_social_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_allup_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_direct_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_excl_single_page_visitors_social_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_allup_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_direct_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_organicsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_otherltc_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_paidsearch_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_programmatic_weekly_tmo'),
  ('sdi_raw_adobe_tfb_mpulse_pb_leadgen_form_complete_ev104_social_weekly_tmo')
)
SELECT e.table_name AS missing_table
FROM expected e
LEFT JOIN prd_dbi_analytics.information_schema.tables t
  ON t.table_schema = 'improvado' AND t.table_name = e.table_name
WHERE t.table_name IS NULL;

/* Multi-segment check — the Bronze dedupe keeps ONE row per table per week and ignores segments_id.
   Spot-check one table per new family; expect no rows. Anything returned would be dropped silently.
   SELECT date_yyyymmdd, COUNT(DISTINCT segments_id) AS segs
   FROM prd_dbi_analytics.improvado.sdi_raw_adobe_pp_upv_all_c2c_weekly_tmo
   GROUP BY date_yyyymmdd HAVING COUNT(DISTINCT segments_id) > 1; */


/* ===== STEP 2 — CREATE / REPLACE the procedures (run the .sql files for Bronze 01, 02, 09, 10, Silver 01, Gold,
   and orchestrator/01_sdi_sp_adobeFunnel_orchestration_allLayers_weekly.sql).
   DO THE ORCHESTRATOR LAST, after STEP 3 and STEP 4 pass — the scheduled job (daily 07:01 PT) runs it. ===== */


/* ===== STEP 3 — RUN. Either call the orchestrator once (it runs all 14 in the order below):
   CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_orchestration_allLayers_weekly();   -- only after it has been updated (see STEP 2)
   or run the 14 calls one by one on the FIRST deployment so you can see exactly which one fails. ===== */
-- Bronze (funnel)
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvFunnelByLtcGroups_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_upvTotalByLtcGroups_weekly();
-- Bronze (BYOD) — unchanged
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowEntryPagesByLtcGroups_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_byodFlowOutcomesByLtcGroups_weekly();
-- Bronze (NEW: TFB + Metro)
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_tfbMetroByAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_bronze_tfbMetroByLtcGroups_weekly();
-- Silver
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowEntryPagesByLtcGroupsPlusAllChannels_weekly();
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_silver_byodFlowOutcomesByLtcGroupsPlusAllChannels_weekly();
-- Gold
CALL prdrzranalytics.lab42.sdi_sp_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly();


/* ===== STEP 4 — VALIDATE ===== */

-- 4a. Shape: 7 rows per week, 59 columns. Expect rows_per_week = 7 for every week.
SELECT WeekSunSat, COUNT(*) AS rows_per_week
FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
GROUP BY WeekSunSat ORDER BY WeekSunSat DESC LIMIT 8;

-- 4b. Join keys: every ChannelGroup should have data. A group with 0 in a column means a label mismatch.
--     The new brands/EC have shorter history than the funnel's 154 weeks, so lower counts are expected.
SELECT ChannelGroup, COUNT(*) AS weeks,
       COUNT(C2cVisitors) AS c2c, COUNT(ChatEngagementVisitors) AS chat,
       COUNT(BroadbandEcCompletedPageEvents) AS ec_completed, COUNT(BroadbandEcSuccessfulVisits) AS ec_successful,
       COUNT(TfbOrders) AS tfb_orders, COUNT(MetroOrders) AS metro_orders,
       COUNT(ByodUpvVisitors) AS byod_entry, COUNT(ByodVrChatVisitors) AS byod_outcome
FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
GROUP BY ChannelGroup ORDER BY ChannelGroup;

-- 4c. Fan-out: expect NO rows (the TFB/Metro join must not duplicate funnel rows).
SELECT WeekSunSat, ChannelGroup, COUNT(*) AS n
FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly
GROUP BY WeekSunSat, ChannelGroup HAVING COUNT(*) > 1;

-- 4d. Regression: the original 37 Gold columns must be unchanged for settled weeks (older than 21 days).
--     Expect 0. A non-zero count means an existing metric moved — investigate before sign-off.
SELECT COUNT(*) AS changed_or_missing_rows FROM (
  SELECT WeekSunSat, ChannelGroup, UpvTotalAdobe, UpvPostpaid, UpvHsi, UpvByod, UpvTrackedFlowSum, UpvFlowTotal, CartstartTotal, CartstartPostpaid, CartstartHsi, CartstartByod, CartstartTrackedFlowSum, OrdersTotal, OrdersUnassistedTotal, OrdersUnassistedPostpaid, OrdersUnassistedHsi, OrdersUnassistedByod, OrdersAssistedTotal, OrdersAssistedPostpaid, OrdersAssistedHsi, OrdersAssistedByod, ByodUpvVisitors, ByodEntryByodPageVisitors, ByodEntryHomePageVisitors, ByodEntryDevicePageVisitors, ByodEntryPlansPageVisitors, ByodEntryOtherPageVisitors, ByodEntryStorePageVisitors, ByodEntryByodLandingPageVisitors, ByodEntryOffersSwitchVisitors, ByodVrChatVisitors, ByodCallVisitors, ByodStoreLocatorVisitors, ByodInternalTmoVisitors, ByodBouncersVisitors, ByodOrders FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly_bkup_20261106 WHERE WeekSunSat <= DATE_SUB(CURRENT_DATE(), 21)
  EXCEPT
  SELECT WeekSunSat, ChannelGroup, UpvTotalAdobe, UpvPostpaid, UpvHsi, UpvByod, UpvTrackedFlowSum, UpvFlowTotal, CartstartTotal, CartstartPostpaid, CartstartHsi, CartstartByod, CartstartTrackedFlowSum, OrdersTotal, OrdersUnassistedTotal, OrdersUnassistedPostpaid, OrdersUnassistedHsi, OrdersUnassistedByod, OrdersAssistedTotal, OrdersAssistedPostpaid, OrdersAssistedHsi, OrdersAssistedByod, ByodUpvVisitors, ByodEntryByodPageVisitors, ByodEntryHomePageVisitors, ByodEntryDevicePageVisitors, ByodEntryPlansPageVisitors, ByodEntryOtherPageVisitors, ByodEntryStorePageVisitors, ByodEntryByodLandingPageVisitors, ByodEntryOffersSwitchVisitors, ByodVrChatVisitors, ByodCallVisitors, ByodStoreLocatorVisitors, ByodInternalTmoVisitors, ByodBouncersVisitors, ByodOrders FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly    WHERE WeekSunSat <= DATE_SUB(CURRENT_DATE(), 21)
);

-- 4e. Sanity: C2C is a subset of total UPV only if the definitions line up. Expect 0; if not, the pp_upv family
--     is defined differently from the pp_uvnb funnel and C2C must not be used as a share of UpvTotalAdobe.
SELECT COUNT(*) AS weeks_where_c2c_exceeds_total_upv
FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly WHERE C2cVisitors > UpvTotalAdobe;

-- 4f. Sanity: TFB/Metro visitor metrics are NOT additive across channel groups (a visitor can appear in several),
--     orders should be. Skip the in-progress latest week.
SELECT WeekSunSat,
  MAX(IF(ChannelGroup = 'All Channels', MetroProspectVisitors, NULL)) AS visitors_all,
  SUM(IF(ChannelGroup <> 'All Channels', MetroProspectVisitors, NULL)) AS visitors_sum_of_groups,
  MAX(IF(ChannelGroup = 'All Channels', MetroOrders, NULL)) AS orders_all,
  SUM(IF(ChannelGroup <> 'All Channels', MetroOrders, NULL)) AS orders_sum_of_groups
FROM prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly
GROUP BY WeekSunSat ORDER BY WeekSunSat DESC LIMIT 8;


/* ===== STEP 5 — CLEAN UP (only after 4a-4d pass; Lab42 is a shared environment) ===== */
-- DROP TABLE prdrzranalytics.lab42.sdi_tbl_adobeFunnel_silver_flowPerformanceByLtcGroupsPlusAllChannels_weekly_bkup_20261106;
-- DROP TABLE prdrzranalytics.lab42.sdi_tbl_adobeFunnel_gold_flowPerformanceByChannelGroups_weekly_bkup_20261106;