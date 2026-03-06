/* =================================================================================================
FILE: 07_create_vw_sdi_profound_gold_visibility_topic_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_visibility_topic_daily
SOURCE TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_daily

PURPOSE:
  Tableau-facing Gold daily view for ProFound Visibility by Topic.

BUSINESS GRAIN:
  One row per:
    - date
    - account_id
    - account_name
    - asset_name
    - topic
    - metric_variant_id

DESIGN NOTES:
  1) This Gold view is built from the Bronze table only.
  2) Exact duplicate rows with the same business grain + same metric values are collapsed.
  3) If the same business grain has multiple distinct metric combinations, they are preserved
     as metric variants:
       - variant_1
       - variant_2
       - variant_3
       - etc.
  4) Lineage columns such as file_load_datetime / filename / insert_date are intentionally NOT
     exposed because this is a Tableau-facing reporting view.
  5) This means the dashboard remains stable while still surfacing upstream metric conflicts.

OUTPUT COLUMNS:
  date
  account_id
  account_name
  asset_name
  topic
  executions
  mentions_count
  share_of_voice
  visibility_score
  metric_variant_id
  metric_variant_count
  has_metric_variants
  collapsed_bronze_row_count

REFRESH STRATEGY:
  Safe to use as a view on top of the daily-refreshed Bronze table.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_visibility_topic_daily` AS

WITH bronze_source AS (
  SELECT
    date,
    account_id,
    account_name,
    asset_name,
    topic,
    executions,
    mentions_count,
    share_of_voice,
    visibility_score
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibility_topic_daily`
),

collapsed_exact_duplicates AS (
  SELECT
    date,
    account_id,
    account_name,
    asset_name,
    topic,
    executions,
    mentions_count,
    share_of_voice,
    visibility_score,
    COUNT(*) AS collapsed_bronze_row_count
  FROM bronze_source
  GROUP BY
    date,
    account_id,
    account_name,
    asset_name,
    topic,
    executions,
    mentions_count,
    share_of_voice,
    visibility_score
),

variant_enriched AS (
  SELECT
    date,
    account_id,
    account_name,
    asset_name,
    topic,
    executions,
    mentions_count,
    share_of_voice,
    visibility_score,
    collapsed_bronze_row_count,
    CONCAT(
      'variant_',
      CAST(
        DENSE_RANK() OVER (
          PARTITION BY date, account_id, account_name, asset_name, topic
          ORDER BY executions, mentions_count, share_of_voice, visibility_score
        ) AS STRING
      )
    ) AS metric_variant_id,
    COUNT(*) OVER (
      PARTITION BY date, account_id, account_name, asset_name, topic
    ) AS metric_variant_count,
    COUNT(*) OVER (
      PARTITION BY date, account_id, account_name, asset_name, topic
    ) > 1 AS has_metric_variants
  FROM collapsed_exact_duplicates
)

SELECT
  date,
  account_id,
  account_name,
  asset_name,
  topic,
  executions,
  mentions_count,
  share_of_voice,
  visibility_score,
  metric_variant_id,
  metric_variant_count,
  has_metric_variants,
  collapsed_bronze_row_count
FROM variant_enriched;