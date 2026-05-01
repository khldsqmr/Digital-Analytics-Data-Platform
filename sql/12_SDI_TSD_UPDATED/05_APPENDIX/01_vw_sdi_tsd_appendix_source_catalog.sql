/* =================================================================================================
FILE: 01_vw_sdi_tsd_appendix_source_catalog.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_tsd_appendix_source_catalog

SOURCE:
  Manual metadata mapping maintained in SQL

DESTINATION:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_appendix_source_catalog

PURPOSE:
  Appendix A source-system catalog for the Total Search Dashboard.
  This view documents the major source families, ownership, storage location,
  cadence, business purpose, and downstream usage.

BUSINESS GRAIN:
  One row per:
      source_family

OUTPUT COLUMNS:
  - source_family
  - primary_source_tables
  - data_source_owner
  - data_source_location
  - data_source_refresh_cadence
  - data_source_description
  - key_metrics_or_content
  - downstream_views
  - notes

KEY MODELING NOTES:
  - This is a documentation / appendix lookup view, not a transactional reporting mart
  - Ownership is standardized mainly to SDI Team as requested
  - Multiple raw tables for the same source family are grouped into one row
  - This view is designed for appendix / governance / stakeholder reference use

================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_tsd_appendix_source_catalog`
AS

SELECT
    'Adobe V2 Funnel' AS source_family,
    'prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_custom_postpaid_voice_v2_tmo' AS primary_source_tables,
    'SDI Team' AS data_source_owner,
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master' AS data_source_location,
    'Daily snapshot / batch refresh' AS data_source_refresh_cadence,
    'Primary Adobe Postpaid funnel source used for enterprise prospect entries, PSPV, cart starts, checkout-step visits, and TSR order activity.' AS data_source_description,
    'adobe_entries, adobe_pspv_actuals, adobe_cart_starts, adobe_cart_checkout_visits, adobe_checkout_review_visits, adobe_postpaid_orders_tsr' AS key_metrics_or_content,
    'vw_sdi_tsd_bronze_adobeV2_daily -> vw_sdi_tsd_silver_adobe_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long' AS downstream_views,
    'Standardized to event_date + lob + channel in Bronze. Channel conformance is applied in Silver.' AS notes

UNION ALL
SELECT
    'Adobe Orders',
    'prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_adobePpPulsePro_gold_orders_daily',
    'SDI Team',
    'BigQuery curated view -> prj-dbi-prd-1.ds_dbi_digitalmedia_automation',
    'Daily snapshot / batch refresh',
    'Curated Adobe digital orders source used for web and app assisted / unassisted order reporting.',
    'adobe_orders_web_unassisted, adobe_orders_web_assisted, adobe_orders_app_unassisted, adobe_orders_app_assisted, adobe_orders_web_all, adobe_orders_app_all, adobe_orders_fully_unassisted, adobe_orders_fully_assisted, adobe_orders_all',
    'vw_sdi_tsd_bronze_adobeOrders_daily -> vw_sdi_tsd_silver_adobe_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Already curated before entering the TSD Bronze layer.'

UNION ALL
SELECT
    'Adobe Cart Start Plus',
    'prj-dbi-prd-1.ds_dbi_improvado_master.adobe_cs_day_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'Adobe source used to capture cart-start-plus activity from event190.',
    'adobe_cart_start_plus',
    'vw_sdi_tsd_bronze_adobeCartStartPlus_daily -> vw_sdi_tsd_silver_adobe_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Snapshot-style source. Latest row is selected before downstream conformance.'

UNION ALL
SELECT
    'Adobe Store Locator',
    'prj-dbi-prd-1.ds_dbi_improvado_master.sdi_raw_adobe_pp_ts_pro_storelocator_visits_daily_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'Adobe source used to track Store Locator visits as a local-intent / store-discovery signal.',
    'adobe_storelocator_visits',
    'vw_sdi_tsd_bronze_adobeStoreLocator_daily -> vw_sdi_tsd_silver_adobe_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Snapshot-style source. NATURAL SEARCH remains source-close until Silver channel conformance.'

UNION ALL
SELECT
    'Adobe T-Life App Visits',
    'prj-dbi-prd-1.ds_dbi_improvado_master.adobe_analytics_prospect_customer_web_app_da_all_postpaid_apps_visits_da_enterprise_prospect_visits_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'Adobe source used to track T-Life app visits for enterprise prospect activity by channel.',
    'adobeTLifeAppVisits',
    'vw_sdi_tsd_bronze_adobeTLifeAppVisits_daily -> vw_sdi_tsd_silver_adobe_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'New Adobe source family. Snapshot records are deduplicated independently before integration into the shared Adobe Silver mart.'

UNION ALL
SELECT
    'SA360 Performance',
    'prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_campaigns_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'SA360 campaign daily performance source containing clicks, impressions, and cart-start-related activity.',
    'sa360_clicks_brand, sa360_clicks_nonbrand, sa360_clicks_all, sa360_cart_start_plus_brand, sa360_cart_start_plus_nonbrand, sa360_cart_start_plus_all',
    'vw_sdi_tsd_bronze_sa360Perf_daily + vw_sdi_tsd_bronze_sa360Entity_daily -> vw_sdi_tsd_silver_sa360_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Joined with SA360 entity metadata in Silver for campaign classification and brand / nonbrand rollups.'

UNION ALL
SELECT
    'SA360 Entity Metadata',
    'prj-dbi-prd-1.ds_dbi_improvado_master.google_search_ads_360_beta_campaign_entity_custom_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'SA360 campaign metadata source used to resolve campaign naming and classify campaigns into brand / generic / shopping / pmax / demandgen groupings.',
    'campaign metadata, latest non-null campaign_name, campaign_type',
    'vw_sdi_tsd_bronze_sa360Entity_daily -> vw_sdi_tsd_silver_sa360_daily',
    'Supports classification logic rather than direct metric publishing.'

UNION ALL
SELECT
    'Google Search Console Query',
    'prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_query_search_type_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'Google Search Console query-level source used for brand / nonbrand classification before aggregation.',
    'gsc_clicks_brand, gsc_clicks_nonbrand, gsc_clicks_all, gsc_impressions_brand, gsc_impressions_nonbrand, gsc_impressions_all',
    'vw_sdi_tsd_bronze_gscQuery_daily -> vw_sdi_tsd_silver_gsc_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Query-level logic is required so classification happens before rollup.'

UNION ALL
SELECT
    'Google Search Console Site Totals',
    'prj-dbi-prd-1.ds_dbi_improvado_master.google_search_console_site_totals_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'Google Search Console site-level totals used mainly for QA and reconciliation against the query-level model.',
    'site-level clicks, impressions, sum_position',
    'vw_sdi_tsd_bronze_gscSite_daily',
    'Supports QA and reconciliation rather than final long-format metric publishing.'

UNION ALL
SELECT
    'Platform Spend',
    'prj-dbi-prd-1.ds_dbi_marketing.agg_day_media_and_outcomes',
    'SDI Team',
    'BigQuery -> prj-dbi-prd-1.ds_dbi_marketing',
    'Daily batch refresh',
    'Aggregated daily media spend source used for paid-search spend alignment and reporting.',
    'platform_spend',
    'vw_sdi_tsd_bronze_platformSpend_daily -> vw_sdi_tsd_silver_platformSpend_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Bronze preserves channel_raw. Silver conforms to reporting channel.'

UNION ALL
SELECT
    'Google Business Profile / GMB',
    'prj-dbi-prd-1.ds_dbi_improvado_master.google_business_profile_google_my_business_location_insights_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Daily snapshot / batch refresh',
    'Google Business Profile source used for maps, local search impressions, and local engagement reporting.',
    'gmb_search_impressions_all, gmb_maps_impressions_all, gmb_impressions_all, gmb_call_clicks, gmb_website_clicks, gmb_directions_clicks',
    'vw_sdi_tsd_bronze_gmb_daily -> vw_sdi_tsd_silver_gmb_daily -> vw_sdi_tsd_gold_unified_daily -> vw_sdi_tsd_gold_unifiedSunSat_weekly / vw_sdi_tsd_gold_unified_monthly -> vw_sdi_tsd_gold_long',
    'Bronze preserves account and location context. Silver derives reporting LOB and conformed channel.'

UNION ALL
SELECT
    'ProFound AI Search',
    'prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_weekly_sunday_tmo; prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_weekly_sunday_tmo; prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_monthly_tmo; prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_monthly_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Weekly / monthly batch refresh',
    'AI Search nonbrand source family used for citation and visibility reporting across TMO, ATT, and Verizon slices.',
    'profound_* execution, citation_count, citation_share, visibility_score metrics',
    'vw_sdi_tsd_bronze_profoundVisCitTag_weekly / vw_sdi_tsd_bronze_profoundVisCitTag_monthly -> vw_sdi_tsd_silver_profound_weekly / vw_sdi_tsd_silver_profound_monthly -> vw_sdi_tsd_gold_profound_weekly / vw_sdi_tsd_gold_profound_monthly -> vw_sdi_tsd_gold_long',
    'Handled separately from the operational daily unified mart because the source is weekly / monthly and structurally different.'

UNION ALL
SELECT
    'GoFish AI Search',
    'prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_vis_tag_weekly_sunday_tmo; prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_cit_tag_weekly_sunday_tmo; prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_vis_tag_monthly_tmo; prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_gofish_cit_tag_monthly_tmo',
    'SDI Team',
    'Improvado -> BigQuery -> prj-dbi-prd-1.ds_dbi_improvado_master',
    'Weekly / monthly batch refresh',
    'AI Search brand source family used for citation and visibility reporting across TMO, ATT, and Verizon slices.',
    'gofish_* execution, citation_count, citation_share, visibility_score metrics',
    'vw_sdi_tsd_bronze_profoundVisCitTag_weekly / vw_sdi_tsd_bronze_profoundVisCitTag_monthly -> vw_sdi_tsd_silver_profound_weekly / vw_sdi_tsd_silver_profound_monthly -> vw_sdi_tsd_gold_profound_weekly / vw_sdi_tsd_gold_profound_monthly -> vw_sdi_tsd_gold_long',
    'Handled separately from the operational daily unified mart because the source is weekly / monthly and structurally different.'
;