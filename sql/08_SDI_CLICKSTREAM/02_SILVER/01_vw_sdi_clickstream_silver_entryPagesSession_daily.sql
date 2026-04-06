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
  - deriving structured tokens from page_name
  - assigning business-friendly entry page groups using those tokens

BUSINESS GRAIN:
  session_id + session_day

WHY THIS VERSION IS BETTER:
  The raw page_name field clearly follows a structured taxonomy such as:
    token_1 | token_2 : token_3
  Examples seen in profiling include:
    - TLife App | Onboarding : Launch
    - TMO | Home : Home
    - TMO | Marketing : Landing Page
    - TMO | Shop : Browse
    - TMO | Shop : Cell Phone Detail
    - TMO | Shop : Order Status
  This makes token-based grouping more reliable than broad LIKE-only logic.

IMPORTANT:
  - This is still a starter taxonomy, but much stronger than free-text-only grouping.
  - Raw page name and cleaned page name are preserved for auditability.
  - Tokens are exposed for debugging and future refinement.

TOKEN EXTRACTION APPROACH:
  token_1:
    first segment before the first pipe '|'

  token_2:
    first segment after the first pipe, before the first colon ':'

  token_3:
    first segment after the first colon ':'

NOTES:
  - Some rows are malformed, blank, or URL-like. These will usually fall to Other unless covered.
  - TLife App is kept as a dedicated top-level app grouping in this version.
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_silver_entryPagesSession_daily`
AS

WITH base AS (
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

    TRIM(SPLIT(entry_page_name_raw, '|')[SAFE_OFFSET(0)]) AS token_1,
    TRIM(SPLIT(SPLIT(entry_page_name_raw, '|')[SAFE_OFFSET(1)], ':')[SAFE_OFFSET(0)]) AS token_2,
    TRIM(SPLIT(SPLIT(entry_page_name_raw, ':')[SAFE_OFFSET(1)], ':')[SAFE_OFFSET(0)]) AS token_3

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesSession_daily`
)

SELECT
  session_id,
  session_day,
  lob,

  entry_page_name_raw,
  entry_page_name_clean,
  visit_start_pagename,

  full_url,
  url_path,
  visit_start_page_url,

  site_name,
  device_type,

  session_channel_name,
  adobe_last_touch_channel_name,
  page_view_channel_name,

  token_1,
  token_2,
  token_3,

  CASE
    /* ---------------------------------------------------------------------------------------------
       APP / T-LIFE
       Keep the TLife family together for now because it is a very large and distinct app ecosystem.
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) = 'tlife app' THEN 'App / T-Life'

    /* ---------------------------------------------------------------------------------------------
       HOMEPAGE
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'home'
    THEN 'Homepage'

    WHEN LOWER(token_1) = 'mbyt'
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'home'
    THEN 'Homepage'

    WHEN LOWER(token_1) = 't-mo prepaid'
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'home'
    THEN 'Homepage'

    /* ---------------------------------------------------------------------------------------------
       PDP
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tlife app')
         AND LOWER(token_2) IN ('shop', 'store')
         AND LOWER(token_3) IN (
           'cell phone detail',
           'cellphone detail',
           'product detail',
           'detalle de producto',
           'smart watch detail',
           'tablet detail',
           'tablet & device detail',
           'hotspot & iot detail',
           'hotspot & iot detail',
           'accessory detail'
         )
    THEN 'PDP'

    /* ---------------------------------------------------------------------------------------------
       PLP
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tlife app', 't-mo prepaid')
         AND LOWER(token_2) IN ('shop', 'store')
         AND LOWER(token_3) IN (
           'browse',
           'browse test',
           'product list',
           'lista de productos',
           'plan',
           'plans',
           'bring your own phone',
           'bring your own device',
           'devices'
         )
    THEN 'PLP'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('cell-phone-plans', 'flex')
    THEN 'PLP'

    /* ---------------------------------------------------------------------------------------------
       DEALS / OFFERS
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) IN ('marketing', 'promotions')
         AND LOWER(token_3) IN (
           'offers',
           'deals',
           'landing page',
           'benefits'
         )
    THEN 'Deals / Offers'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'carrier freedom'
         AND LOWER(token_3) = 'home'
    THEN 'Deals / Offers'

    /* ---------------------------------------------------------------------------------------------
       BRAND / WHY T-MOBILE
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('marketing', 'coverage', 'brand')
    THEN 'Brand / Why T-Mobile'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('resources', 'benefits')
    THEN 'Brand / Why T-Mobile'

    /* ---------------------------------------------------------------------------------------------
       ACCOUNT / LOGIN
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tmo app')
         AND LOWER(token_2) IN (
           'billing',
           'account',
           'autenticación',
           'cuenta',
           'guest pay',
           'guestpay',
           'my phone',
           'payments'
         )
    THEN 'Account / Login'

    /* ---------------------------------------------------------------------------------------------
       SUPPORT
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tfb')
         AND LOWER(token_2) IN ('support', 'asistencia', 'contact-us', 'contactus')
    THEN 'Support'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('privacy center', 'legal')
    THEN 'Support'

    /* ---------------------------------------------------------------------------------------------
       STORE / LOCATOR
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'store'
    THEN 'Store / Locator'

    /* ---------------------------------------------------------------------------------------------
       ORDER STATUS
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tlife app')
         AND LOWER(token_2) = 'shop'
         AND LOWER(token_3) = 'order status'
    THEN 'Order Status'

    WHEN LOWER(entry_page_name_clean) LIKE '%checkorder%'
      OR LOWER(entry_page_name_clean) LIKE '%order-status%'
    THEN 'Order Status'

    /* ---------------------------------------------------------------------------------------------
       FALLBACKS
    --------------------------------------------------------------------------------------------- */
    WHEN LOWER(entry_page_name_clean) LIKE '%cell phone detail%'
      OR LOWER(entry_page_name_clean) LIKE '%product detail%'
      OR LOWER(entry_page_name_clean) LIKE '%phone detail%'
    THEN 'PDP'

    WHEN LOWER(entry_page_name_clean) LIKE '%browse%'
      OR LOWER(entry_page_name_clean) LIKE '%cell phones%'
      OR LOWER(entry_page_name_clean) LIKE '%plan%'
    THEN 'PLP'

    WHEN LOWER(entry_page_name_clean) LIKE '%deals%'
      OR LOWER(entry_page_name_clean) LIKE '%offers%'
      OR LOWER(entry_page_name_clean) LIKE '%promotions%'
    THEN 'Deals / Offers'

    WHEN LOWER(entry_page_name_clean) LIKE '%support%'
      OR LOWER(entry_page_name_clean) LIKE '%contact us%'
      OR LOWER(entry_page_name_clean) LIKE '%privacy%'
      OR LOWER(entry_page_name_clean) LIKE '%legal%'
    THEN 'Support'

    ELSE 'Other'
  END AS entry_page_group

FROM base;