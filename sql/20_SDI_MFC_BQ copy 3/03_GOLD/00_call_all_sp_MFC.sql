-- Bronze — all 4 must complete before Silver runs
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActuals_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendActualsGranular_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecast_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_bronze_spendForecastGranular_weekly`();

-- Silver — reads from the Bronze tables above
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_sp_mfc_silver_spendGranular_weekly`();