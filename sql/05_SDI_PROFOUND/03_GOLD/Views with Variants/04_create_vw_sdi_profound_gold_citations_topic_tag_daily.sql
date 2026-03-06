/* =================================================================================================
FILE: 04_create_vw_sdi_profound_gold_citations_topic_tag_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_topic_tag_daily
SOURCE TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily

PURPOSE:
  Tableau-facing Gold daily view for ProFound Citations by Topic and Tag.

BUSINESS GRAIN:
  date + account_id + account_name + root_domain + topic + tag

IMPORTANT:
  root_domain is intentionally retained here because your earlier mismatch analysis clearly showed
  that topic+tag alone is not sufficient for this dataset. Different root domains can exist under
  the same topic/tag on the same day and must remain distinct in reporting.

DESIGN NOTES:
  - Exact duplicate metric rows are collapsed.
  - Distinct metric combinations for the same business grain are preserved as variants.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_topic_tag_daily` AS

WITH bronze_base AS (
  SELECT
    date,
    account_id,
    account_name,
    root_domain,
    topic,
    tag,
    count,
    share_of_voice
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_topic_tag_daily`
),

collapsed_metric_rows AS (
  SELECT
    date,
    account_id,
    account_name,
    root_domain,
    topic,
    tag,
    count,
    share_of_voice,
    COUNT(*) AS collapsed_bronze_row_count
  FROM bronze_base
  GROUP BY
    date,
    account_id,
    account_name,
    root_domain,
    topic,
    tag,
    count,
    share_of_voice
),

variant_labeled AS (
  SELECT
    date,
    account_id,
    account_name,
    root_domain,
    topic,
    tag,
    count,
    share_of_voice,
    collapsed_bronze_row_count,

    ROW_NUMBER() OVER (
      PARTITION BY date, account_id, account_name, root_domain, topic, tag
      ORDER BY count, share_of_voice
    ) AS metric_variant_number,

    COUNT(*) OVER (
      PARTITION BY date, account_id, account_name, root_domain, topic, tag
    ) AS metric_variant_count
  FROM collapsed_metric_rows
)

SELECT
  date,
  account_id,
  account_name,
  root_domain,
  topic,
  tag,
  count,
  share_of_voice,
  metric_variant_number,
  CONCAT('variant_', CAST(metric_variant_number AS STRING)) AS metric_variant_id,
  metric_variant_count,
  metric_variant_count > 1 AS has_metric_variants,
  collapsed_bronze_row_count
FROM variant_labeled;