-- ============================================================
-- BRONZE — must run first (Silver reads from these tables)
-- ============================================================
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActuals_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActualsGranular_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecast_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecastGranular_weekly`();

-- ============================================================
-- SILVER — must run after Bronze (Gold reads from these tables)
-- ============================================================
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spendGranular_weekly`();