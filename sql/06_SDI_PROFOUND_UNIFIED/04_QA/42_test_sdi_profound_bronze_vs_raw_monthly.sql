/* =================================================================================================
FILE: 42_test_sdi_profound_bronze_vs_raw_monthly.sql
PURPOSE:
  Monthly Bronze vs Raw reconciliation checks for ProFound Unified.
  These tests compare raw latest-snapshot deduped output against Bronze row counts.
================================================================================================= */

-- -------------------------------------------------------------------------------------------------
-- 42.1 Visibility Asset Monthly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      asset_id,
      asset_name,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_asset_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibilityAsset_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityAsset_monthly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 42.2 Visibility Tag Monthly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      asset_id,
      asset_name,
      tag,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, tag, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibilityTag_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityTag_monthly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 42.3 Visibility Tag Topic Monthly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      asset_id,
      asset_name,
      tag,
      topic,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, asset_id, asset_name, tag, topic, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_vis_tag_topic_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'visibilityTagTopic_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_visibilityTagTopic_monthly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 42.4 Citation Domain Monthly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      root_domain,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_domain_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citationDomain_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationDomain_monthly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 42.5 Citation Tag Monthly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      root_domain,
      tag,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, tag, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citationTag_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationTag_monthly`) AS bronze_count;

-- -------------------------------------------------------------------------------------------------
-- 42.6 Citation Tag Topic Monthly
-- -------------------------------------------------------------------------------------------------
WITH raw_deduped AS (
  SELECT *
  FROM (
    SELECT
      account_id,
      root_domain,
      tag,
      topic,
      date_yyyymmdd,
      ROW_NUMBER() OVER (
        PARTITION BY account_id, root_domain, tag, topic, date_yyyymmdd
        ORDER BY File_Load_datetime DESC, __insert_date DESC, Filename DESC
      ) AS rn
    FROM `prj-dbi-prd-1.ds_dbi_improvado_master.sdi_seo_profound_cit_tag_topic_monthly_tmo`
    WHERE SAFE.PARSE_DATE('%Y%m%d', date_yyyymmdd) IS NOT NULL
  )
  WHERE rn = 1
)
SELECT
  'citationTagTopic_monthly' AS table_name,
  (SELECT COUNT(*) FROM raw_deduped) AS raw_deduped_count,
  (SELECT COUNT(*) FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_profound_bronze_citationTagTopic_monthly`) AS bronze_count;