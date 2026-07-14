/* =================================================================================================
FILE:         00_call_all_sp_pulseTms.sql
PURPOSE:      Executes all PulseTMS stored procedures in dependency order.

  EXECUTION ORDER:
    Bronze first (Silver depends on Bronze physical tables):
      1. sp_sdi_pulseTms_bronze_adobeFunnel_weekly
      2. sp_sdi_pulseTms_bronze_mfcSpend_weekly
      3. sp_sdi_pulseTms_bronze_platformSpend_weekly

    Silver second (Gold views read directly from Silver physical tables):
      4. sp_sdi_pulseTms_silver_adobeFunnel_weekly
      5. sp_sdi_pulseTms_silver_mfcSpend_weekly
      6. sp_sdi_pulseTms_silver_platformSpend_weekly

  Gold views require no refresh — they read directly from Silver tables and are always current:
      vw_sdi_pulseTms_gold_unified_long   (Tableau production data source)
      vw_sdi_pulseTms_gold_wide_channel   (sense-check only)

  Dim view requires no refresh:
      vw_sdi_pulseTms_dim_qgp_calendar    (derived from Gregorian calendar; always current)

SCHEDULE:
  Run weekly after source data lands — typically Monday morning after Saturday week close.

FILE INVENTORY:
  00_call_all_sp_pulseTms.sql                      — this file
  01_vw_sdi_pulseTms_dim_qgp_calendar.sql          — QGP calendar dimension view
  02_sp_sdi_pulseTms_bronze_adobeFunnel_weekly.sql — Adobe funnel Bronze SP
  03_sp_sdi_pulseTms_bronze_mfcSpend_weekly.sql    — MFC spend Bronze SP
  04_sp_sdi_pulseTms_bronze_platformSpend_weekly.sql — Platform spend Bronze SP
  05_sp_sdi_pulseTms_silver_adobeFunnel_weekly.sql — Adobe funnel Silver SP
  06_sp_sdi_pulseTms_silver_mfcSpend_weekly.sql    — MFC spend Silver SP
  07_sp_sdi_pulseTms_silver_platformSpend_weekly.sql — Platform spend Silver SP
  08_vw_sdi_pulseTms_gold_unified_long.sql         — Gold unified long view (Tableau source)
  09_vw_sdi_pulseTms_gold_wide_channel.sql         — Gold wide channel view (sense-check)
================================================================================================= */

-- ---------------------------------------------------------------------------
-- Bronze — must complete before Silver runs
-- ---------------------------------------------------------------------------
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_adobeFunnel_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_mfcSpend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_bronze_platformSpend_weekly`();

-- ---------------------------------------------------------------------------
-- Silver — must complete before Tableau queries Gold views
-- ---------------------------------------------------------------------------
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_adobeFunnel_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_mfcSpend_weekly`();
CALL `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_platformSpend_weekly`();
call `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sp_sdi_pulseTms_silver_upvForecast_weekly`();
