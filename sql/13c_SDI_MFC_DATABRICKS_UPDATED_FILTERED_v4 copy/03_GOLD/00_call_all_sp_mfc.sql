-- ============================================================
-- MFC SPEND PIPELINE: CALL ALL STORED PROCEDURES
-- Run this file weekly to refresh the full pipeline.
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

CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActuals_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActualsGranular_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecasts_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecastsGranular_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly();