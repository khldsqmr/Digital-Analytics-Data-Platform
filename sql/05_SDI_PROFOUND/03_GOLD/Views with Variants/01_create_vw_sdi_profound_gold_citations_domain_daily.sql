/* =================================================================================================
FILE: 01_create_vw_sdi_profound_gold_citations_domain_daily.sql
LAYER: Gold
OBJECT TYPE: View
OBJECT NAME: vw_sdi_profound_gold_citations_domain_daily
SOURCE TABLE: prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_domain_daily

PURPOSE:
  Tableau-facing Gold daily view for ProFound Citations by Domain.

BUSINESS GRAIN:
  date + account_id + account_name + root_domain

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
  root_domain
  count
  share_of_voice
  metric_variant_number
  metric_variant_id
  metric_variant_count
  has_metric_variants
  collapsed_bronze_row_count

REFRESH STRATEGY:
  Safe to use as a view on top of the daily-refreshed Bronze table.
================================================================================================= */

CREATE OR REPLACE VIEW `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_profound_gold_citations_domain_daily` AS

WITH bronze_base AS (
  /* ---------------------------------------------------------------------------------------------
  Step 1: Read the required Tableau-facing columns from Bronze.
  --------------------------------------------------------------------------------------------- */
  SELECT
    date,
    account_id,
    account_name,
    root_domain,
    count,
    share_of_voice
  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citations_domain_daily`
),

collapsed_metric_rows AS (
  /* ---------------------------------------------------------------------------------------------
  Step 2: Collapse exact duplicate rows.

  Why:
    If Bronze contains repeated rows that are identical across both:
      - business grain columns
      - metric columns
    then Tableau should not see those as multiple rows.

  collapsed_bronze_row_count tells us how many identical Bronze rows collapsed into this record.
  --------------------------------------------------------------------------------------------- */
  SELECT
    date,
    account_id,
    account_name,
    root_domain,
    count,
    share_of_voice,
    COUNT(*) AS collapsed_bronze_row_count
  FROM bronze_base
  GROUP BY
    date,
    account_id,
    account_name,
    root_domain,
    count,
    share_of_voice
),

variant_labeled AS (
  /* ---------------------------------------------------------------------------------------------
  Step 3: Assign metric variants within each business grain.

  Business grain for this view:
    date + account_id + account_name + root_domain

  If only one distinct metric combination exists:
    metric_variant_number = 1
    metric_variant_count  = 1
    has_metric_variants   = FALSE

  If multiple distinct metric combinations exist for the same grain:
    each distinct metric row gets variant_1 / variant_2 / ...
  --------------------------------------------------------------------------------------------- */
  SELECT
    date,
    account_id,
    account_name,
    root_domain,
    count,
    share_of_voice,
    collapsed_bronze_row_count,

    ROW_NUMBER() OVER (
      PARTITION BY date, account_id, account_name, root_domain
      ORDER BY count, share_of_voice
    ) AS metric_variant_number,

    COUNT(*) OVER (
      PARTITION BY date, account_id, account_name, root_domain
    ) AS metric_variant_count
  FROM collapsed_metric_rows
)

SELECT
  date,
  account_id,
  account_name,
  root_domain,
  count,
  share_of_voice,
  metric_variant_number,
  CONCAT('variant_', CAST(metric_variant_number AS STRING)) AS metric_variant_id,
  metric_variant_count,
  metric_variant_count > 1 AS has_metric_variants,
  collapsed_bronze_row_count
FROM variant_labeled;