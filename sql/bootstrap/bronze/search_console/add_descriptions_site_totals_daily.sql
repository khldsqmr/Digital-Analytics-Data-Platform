-- ============================================================================
-- METADATA UPDATE | BRONZE | SEARCH CONSOLE | SITE TOTALS
-- PURPOSE:
--   Adds table and column descriptions to existing Bronze table.
--   Metadata-only operation.
-- ============================================================================

ALTER TABLE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_site_totals_daily`
SET OPTIONS (
  description = "Bronze-level Google Search Console site totals. Highest aggregation level from source, used for executive SEO KPIs, trend analysis, and parent-level validation."
);

-- =========================
-- COLUMN DESCRIPTIONS
-- =========================

ALTER COLUMN account_id SET OPTIONS (
  description = "Search Console property identifier (sc-domain or URL-prefix)"
);

ALTER COLUMN account_name SET OPTIONS (
  description = "Human-readable name of the Search Console property"
);

ALTER COLUMN site_url SET OPTIONS (
  description = "Site or domain associated with the Search Console property"
);

ALTER COLUMN date_yyyymmdd SET OPTIONS (
  description = "Source date in YYYYMMDD format retained exactly as received from upstream system"
);

ALTER COLUMN date SET OPTIONS (
  description = "Derived DATE parsed from date_yyyymmdd, used for partitioning and analytics"
);

ALTER COLUMN clicks SET OPTIONS (
  description = "Total number of organic clicks recorded at the site level"
);

ALTER COLUMN impressions SET OPTIONS (
  description = "Total number of impressions recorded at the site level"
);

ALTER COLUMN sum_position SET OPTIONS (
  description = "Sum of impression-weighted positions used to compute average ranking"
);

ALTER COLUMN __insert_date SET OPTIONS (
  description = "Unix epoch timestamp representing when the record was inserted into Bronze"
);

ALTER COLUMN file_load_datetime SET OPTIONS (
  description = "Timestamp when the source file was loaded into the landing layer"
);

ALTER COLUMN filename SET OPTIONS (
  description = "Source filename and path from the landing zone"
);
