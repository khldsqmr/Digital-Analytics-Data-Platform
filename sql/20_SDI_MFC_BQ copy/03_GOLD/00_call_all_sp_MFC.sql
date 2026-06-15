-- ============================================================
-- MFC SPEND PIPELINE — BigQuery
-- Run this script weekly to refresh the full pipeline.
--
-- Step 0: Drop old Bronze/Silver views (one-time migration)
--         These are replaced by tables created by the SPs below.
--
-- Execution order:
--   1. Bronze Actuals (non-granular)
--   2. Bronze Actuals Granular
--   3. Bronze Forecasts (non-granular)
--   4. Bronze Forecasts Granular
--   5. Silver 1 — LOB-level
--   6. Silver 2 — Granular
--
-- Views (no refresh needed — always live):
--   sdi_vw_mfc_dim_qgp_calendar
--   sdi_vw_mfc_gold_spend_weekly
--   sdi_vw_mfc_gold_spendGranular_weekly
-- ============================================================


-- ============================================================
-- STEP 0: ONE-TIME — drop old Bronze/Silver views
-- Run once after deploying the stored procedures.
-- Safe to re-run; IF EXISTS prevents errors if already dropped.
-- ============================================================
DROP VIEW IF EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_bronze_spendActuals_weekly`;
DROP VIEW IF EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_bronze_spendActualsGranular_weekly`;
DROP VIEW IF EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_bronze_spendForecasts_weekly`;
DROP VIEW IF EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_bronze_spendForecastsGranular_weekly`;
DROP VIEW IF EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spend_weekly`;
DROP VIEW IF EXISTS `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`;


-- ============================================================
-- STEP 1–6: Weekly refresh — call stored procedures in order
-- ============================================================
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActuals_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActualsGranular_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecasts_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecastsGranular_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spendGranular_weekly`();