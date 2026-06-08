/* =================================================================================================
FILE:         00_call_all_sp_pulseTms.sql
PURPOSE:      Executes all PulseTMS stored procedures in dependency order.

  EXECUTION ORDER:
    Bronze first (Silver depends on Bronze tables):
      1. sp_sdi_pulseTms_bronze_adobeFunnel_weekly
      2. sp_sdi_pulseTms_bronze_mfcSpend_weekly
      3. sp_sdi_pulseTms_bronze_platformSpend_weekly
    Silver second (Gold view reads from Silver tables):
      4. sp_sdi_pulseTms_silver_adobeFunnel_weekly
      5. sp_sdi_pulseTms_silver_mfcSpend_weekly
      6. sp_sdi_pulseTms_silver_platformSpend_weekly

  Gold view (vw_sdi_pulseTms_gold_unified_long) requires no refresh —
  it reads directly from the Silver tables and is always current.

SCHEDULE:
  Run weekly after source data lands — typically Monday morning after Saturday week close.
================================================================================================= */

-- Bronze
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_adobeFunnel_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_mfcSpend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_platformSpend_weekly`();

-- Silver
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_adobeFunnel_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_mfcSpend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_platformSpend_weekly`();