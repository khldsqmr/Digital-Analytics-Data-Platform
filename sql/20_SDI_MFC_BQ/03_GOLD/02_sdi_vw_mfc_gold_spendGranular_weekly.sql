-- =============================================
-- GOLD: Granular Weekly (BQ)
-- CHANGED: Agency added to SELECT and ORDER BY
-- =============================================
CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly` AS

WITH latest_file AS (
  SELECT
    Quarter,
    QGP_Week,
    LOB_Supported,
    MAX(FileLoad_Date) AS latest_file_load_date
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`
  GROUP BY Quarter, QGP_Week, LOB_Supported
),

latest AS (
  SELECT s.*
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly` s
  JOIN latest_file f
    ON s.Quarter       = f.Quarter
   AND s.QGP_Week      = f.QGP_Week
   AND s.LOB_Supported = f.LOB_Supported
   AND s.FileLoad_Date = f.latest_file_load_date
)

SELECT
  Quarter,
  Period_Start,
  Period_End,
  QGP_Week,
  Quarter_End_Date,
  FileLoad_Date,
  LOB_Supported,
  Channel,
  Tactic,
  Message_Type,
  Agency,
  weekly_actual    AS spend_actual,
  weekly_forecast  AS spend_forecast,
  weekly_display   AS spend_display,
  week_type
FROM latest
ORDER BY
  LOB_Supported DESC,
  Quarter DESC,
  QGP_Week DESC,
  Channel,
  Tactic,
  Agency;