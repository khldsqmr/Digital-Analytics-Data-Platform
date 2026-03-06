/* =================================================================================================
FILE: 03_create_vw_sdi_profound_gold_citations_topic_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_topic_daily
SOURCE TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily

PURPOSE:
  Tableau-facing Gold daily view for ProFound Citations by Topic.

BUSINESS GRAIN:
  date + account_id + account_name + topic

DESIGN NOTES:
  - Exact duplicate metric rows are collapsed.
  - Distinct metric combinations for the same business grain are preserved as variants.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_topic_daily` AS

WITH bronze_base AS (
  SELECT
    date,
    account_id,
    account_name,
    topic,
    count,
    share_of_voice
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_daily`
),

collapsed_metric_rows AS (
  SELECT
    date,
    account_id,
    account_name,
    topic,
    count,
    share_of_voice,
    COUNT(*) AS collapsed_bronze_row_count
  FROM bronze_base
  GROUP BY
    date,
    account_id,
    account_name,
    topic,
    count,
    share_of_voice
),

variant_labeled AS (
  SELECT
    date,
    account_id,
    account_name,
    topic,
    count,
    share_of_voice,
    collapsed_bronze_row_count,

    ROW_NUMBER() OVER (
      PARTITION BY date, account_id, account_name, topic
      ORDER BY count, share_of_voice
    ) AS metric_variant_number,

    COUNT(*) OVER (
      PARTITION BY date, account_id, account_name, topic
    ) AS metric_variant_count
  FROM collapsed_metric_rows
)

SELECT
  date,
  account_id,
  account_name,
  topic,
  count,
  share_of_voice,
  metric_variant_number,
  CONCAT('variant_', CAST(metric_variant_number AS STRING)) AS metric_variant_id,
  metric_variant_count,
  metric_variant_count > 1 AS has_metric_variants,
  collapsed_bronze_row_count
FROM variant_labeled;