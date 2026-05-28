CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_gold_spendGranular_weekly` AS

WITH deduped AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY
        Quarter,
        QGP_Week,
        LOB_Supported,
        Channel,
        Tactic,
        Message_Type,
        Agency
      ORDER BY FileLoad_Date DESC
    ) AS rn
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_vw_mfc_silver_spendGranular_weekly`
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
FROM deduped
WHERE rn = 1
ORDER BY LOB_Supported DESC, Quarter DESC, QGP_Week DESC, Channel, Tactic;