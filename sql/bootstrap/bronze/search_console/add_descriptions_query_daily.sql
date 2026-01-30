-- ============================================================================
-- METADATA UPDATE | BRONZE | SEARCH CONSOLE | QUERY DAILY
-- PURPOSE:
--   Adds table and column descriptions to existing Bronze table.
--   Safe to re-run. Metadata only.
-- ============================================================================

ALTER TABLE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
SET OPTIONS (
  description = "Bronze-level Google Search Console query, page, and search-type data. Lowest-grain dataset preserving full source fidelity for SEO diagnostics, ranking analysis, and content performance."
);

-- =========================
-- COLUMN DESCRIPTIONS
-- =========================

ALTER TABLE
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_bronze_search_console_query_daily`
ALTER COLUMN account_id SET OPTIONS (
  description = "Search Console property identifier (sc-domain or URL-prefix)"
);

ALTER COLUMN account_name SET OPTIONS (
  description = "Human-readable name of the Search Console property"
);

ALTER COLUMN site_url SET OPTIONS (
  description = "Site or domain associated with the Search Console property"
);

ALTER COLUMN page SET OPTIONS (
  description = "Landing page URL that appeared in Google Search results"
);

ALTER COLUMN query SET OPTIONS (
  description = "User search query that triggered the impression"
);

ALTER COLUMN search_type SET OPTIONS (
  description = "Search result type (web, image, video, news)"
);

ALTER COLUMN date_yyyymmdd SET OPTIONS (
  description = "Source date in YYYYMMDD format retained exactly as received from upstream system"
);

ALTER COLUMN date SET OPTIONS (
  description = "Derived DATE parsed from date_yyyymmdd, used for partitioning and analytics"
);

ALTER COLUMN clicks SET OPTIONS (
  description = "Number of organic clicks recorded for the query-page-search type combination"
);

ALTER COLUMN impressions SET OPTIONS (
  description = "Number of impressions recorded for the query-page-search type combination"
);

ALTER COLUMN position SET OPTIONS (
  description = "Average search result position reported by Google Search Console"
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
