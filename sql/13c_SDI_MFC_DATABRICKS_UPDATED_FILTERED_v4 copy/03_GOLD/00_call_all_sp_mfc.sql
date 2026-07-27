-- ============================================================
-- BRONZE — must run first (Silver reads from these tables)
-- ============================================================
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActuals_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendActualsGranular_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecast_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_bronze_spendForecastGranular_weekly();

-- ============================================================
-- SILVER — must run after Bronze (Gold reads from these tables)
-- ============================================================
CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spend_weekly();
CALL prdrzranalytics.lab42.sdi_sp_mfc_silver_spendGranular_weekly();