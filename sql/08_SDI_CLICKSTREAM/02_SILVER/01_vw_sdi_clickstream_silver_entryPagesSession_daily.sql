/* =================================================================================================
FILE: 01_vw_sdi_clickstream_silver_entryPagesSession_daily.sql
LAYER: Silver
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_clickstream_silver_entryPagesSession_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily

PURPOSE:
  Business-friendly Silver session-entry view for Entry Pages reporting.

  This view enriches the Bronze entry-session table by:
  - standardizing the raw page label
  - assigning starter business page groups

BUSINESS GRAIN:
  session_id + session_day

WHY THIS VIEW EXISTS:
  Bronze should remain close to raw. Silver is the appropriate place to centralize business logic,
  especially entry page grouping, while still preserving raw fields for traceability.

IMPORTANT:
  - This is a starter grouping logic, not a final taxonomy.
  - The best way to refine it is by reviewing top raw entry pages after Bronze load.

GROUPING STRATEGY:
  - Separate app-first experiences from classic web pages
  - Separate commerce discovery pages from account/service pages
  - Group support/store/order-status operational pages separately
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_silver_entryPagesSession_daily`
AS

SELECT
  session_id,
  session_day,
  lob,

  entry_page_name_raw,
  REGEXP_REPLACE(TRIM(entry_page_name_raw), r'\s+\|\s+', ' | ') AS entry_page_name_clean,
  visit_start_pagename,

  full_url,
  url_path,
  visit_start_page_url,

  site_name,
  device_type,

  session_channel_name,
  adobe_last_touch_channel_name,
  page_view_channel_name,

  CASE
    WHEN LOWER(entry_page_name_raw) LIKE 'tlife app%'
      OR LOWER(entry_page_name_raw) LIKE '%t-life%'
    THEN 'App / T-Life'

    WHEN LOWER(entry_page_name_raw) LIKE '%home : home%'
    THEN 'Homepage'

    WHEN LOWER(entry_page_name_raw) LIKE '%cell phone detail%'
      OR LOWER(entry_page_name_raw) LIKE '%product detail%'
      OR LOWER(entry_page_name_raw) LIKE '%phone detail%'
    THEN 'PDP'

    WHEN LOWER(entry_page_name_raw) LIKE '%browse%'
      OR LOWER(entry_page_name_raw) LIKE '%cell phones%'
      OR LOWER(entry_page_name_raw) LIKE '%plan : cell phone plans%'
      OR LOWER(entry_page_name_raw) LIKE '%bring your own phone%'
      OR LOWER(entry_page_name_raw) LIKE '%shop : plan%'
    THEN 'PLP'

    WHEN LOWER(entry_page_name_raw) LIKE '%deals%'
      OR LOWER(entry_page_name_raw) LIKE '%offers%'
      OR LOWER(entry_page_name_raw) LIKE '%promotions%'
      OR LOWER(entry_page_name_raw) LIKE '%carrier freedom%'
    THEN 'Deals / Offers'

    WHEN LOWER(entry_page_name_raw) LIKE '%landing page%'
      OR LOWER(entry_page_name_raw) LIKE '%join us%'
      OR LOWER(entry_page_name_raw) LIKE '%switch%'
      OR LOWER(entry_page_name_raw) LIKE '%brand%'
      OR LOWER(entry_page_name_raw) LIKE '%coverage%'
      OR LOWER(entry_page_name_raw) LIKE '%network%'
    THEN 'Brand / Why T-Mobile'

    WHEN LOWER(entry_page_name_raw) LIKE '%authentication%'
      OR LOWER(entry_page_name_raw) LIKE '%login%'
      OR LOWER(entry_page_name_raw) LIKE '%sign in%'
      OR LOWER(entry_page_name_raw) LIKE '%my account%'
      OR LOWER(entry_page_name_raw) LIKE '%billing%'
      OR LOWER(entry_page_name_raw) LIKE '%guestpay%'
      OR LOWER(entry_page_name_raw) LIKE '%payment arrangement%'
      OR LOWER(entry_page_name_raw) LIKE '%eipjod%'
    THEN 'Account / Login'

    WHEN LOWER(entry_page_name_raw) LIKE '%support%'
      OR LOWER(entry_page_name_raw) LIKE '%contact us%'
      OR LOWER(entry_page_name_raw) LIKE '%privacy notices%'
      OR LOWER(entry_page_name_raw) LIKE '%resources%'
      OR LOWER(entry_page_name_raw) LIKE '%chat%'
    THEN 'Support'

    WHEN LOWER(entry_page_name_raw) LIKE '%store%'
      OR LOWER(entry_page_name_raw) LIKE '%locator%'
      OR LOWER(entry_page_name_raw) LIKE '%qr redirect%'
    THEN 'Store / Locator'

    WHEN LOWER(entry_page_name_raw) LIKE '%order status%'
    THEN 'Order Status'

    ELSE 'Other'
  END AS entry_page_group

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily`;