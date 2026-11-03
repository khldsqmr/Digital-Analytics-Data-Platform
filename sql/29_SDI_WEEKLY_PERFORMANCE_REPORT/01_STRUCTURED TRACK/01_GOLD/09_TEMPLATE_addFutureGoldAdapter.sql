/* DOCUMENTED TEMPLATE — NOT DEPLOYED. This file contains no executable DDL on purpose.
   For a future source, create e.g.
   sdi_vw_weeklyNarrative_gold_ordersFactAdapter_long
   with SAME columns/types as 08 and pin to a source-owned immutable snapshot.
   REQUIRED: source_project,source_snapshot_id,week_end,metric_key,display_name,
   metric_lob,channel_group,unit,week_value,prior_week_value,wow_pct,
   target_week_value,vs_target_pct,qtd_vs_target,is_data_current,
   source_validation_status,row_role,highlight_score,is_highlight,
   data_quality_issue,fact_ref.

   Each branch in 08 should explicitly UNION ALL the approved adapter:
     SELECT ... FROM ...sdi_vw_weeklyNarrative_gold_ordersFactAdapter_long

   Never concatenate arbitrary table names from LLM output into SQL.
   Every fact_ref MUST identify the immutable source snapshot and metric slice.
   New registry metric keys should be <sourceProject>.<metricName>.
*/
