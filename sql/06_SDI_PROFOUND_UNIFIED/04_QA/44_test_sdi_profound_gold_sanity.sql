/* =================================================================================================
FILE: 44_test_sdi_profound_gold_sanity.sql
PURPOSE:
  Sanity checks for Gold unified and Gold melted views.
================================================================================================= */

-- Gold unified row count
SELECT
  COUNT(*) AS gold_unified_row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified`;

-- Gold melted row count by metric
SELECT
  metric_name,
  COUNT(*) AS row_count
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_melted`
GROUP BY metric_name
ORDER BY metric_name;

-- Ensure unified rows do not have both visibility and citation populated at once
SELECT
  COUNT(*) AS invalid_rows
FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_unified`
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