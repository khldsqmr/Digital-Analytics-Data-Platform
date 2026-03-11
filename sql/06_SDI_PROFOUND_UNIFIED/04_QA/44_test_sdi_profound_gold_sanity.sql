/* =================================================================================================
FILE: 44_test_sdi_profound_gold_sanity.sql
PURPOSE:
  Sanity checks for Gold unified wide and unified long views.
================================================================================================= */

-- -------------------------------------------------------------------------------------------------
-- 44.1 Gold unified wide row count
-- -------------------------------------------------------------------------------------------------
SELECT
  COUNT(*) AS gold_unified_wide_row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`;

-- -------------------------------------------------------------------------------------------------
-- 44.2 Gold unified long row count by metric
-- -------------------------------------------------------------------------------------------------
SELECT
  metric_name,
  COUNT(*) AS row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_long`
GROUP BY metric_name
ORDER BY metric_name;

-- -------------------------------------------------------------------------------------------------
-- 44.3 Ensure wide rows do not have both visibility and citation populated at once
-- -------------------------------------------------------------------------------------------------
SELECT
  COUNT(*) AS invalid_rows
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified_wide`
WHERE
  (
    vis_executions IS NOT NULL
    OR vis_mentions_count IS NOT NULL
    OR vis_share_of_voice IS NOT NULL
    OR vis_visibility_score IS NOT NULL
  )
  AND
  (
    cit_count IS NOT NULL
    OR cit_share_of_voice IS NOT NULL
  );