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
  - assigning a clean business-friendly entry page group taxonomy

BUSINESS GRAIN:
  session_id + session_day

WHY THIS VERSION IS BETTER:
  The raw page_name field clearly follows structured patterns such as:
    token_1 | token_2 : token_3

  Profiling showed strong recurring patterns like:
    - TLife App | Onboarding : Launch
    - TMO | Home : Home
    - TMO | Marketing : Landing Page
    - TMO | Marketing : Offers
    - TMO | Shop : Browse
    - TMO | Shop : Cell Phone Detail
    - TMO | Shop : Order Status
    - TMO | Store : Business Detail
    - TMO | Support : Resources
  so token-based grouping is more reliable than broad free-text matching. 

TARGET TAXONOMY (MAX 15 GROUPS):
  1. App / T-Life
  2. Homepage
  3. PLP / Browse
  4. PDP / Detail
  5. Deals / Offers
  6. Brand / Marketing
  7. Coverage / Network
  8. Support / Help
  9. Store / Locator
  10. Order Status
  11. Cart / Checkout
  12. Account / Billing / Login
  13. Privacy / Legal
  14. Search / Tools
  15. Other

WHAT THIS CLEANUP VERSION FIXES:
  - Promotions rows now map to Deals / Offers
  - Tienda rows are classified into PLP / Browse or Store / Locator where appropriate
  - T-Fiber rows no longer fall to Other
  - T-Money rows no longer fall to Other
  - TMO App billandpay-style rows map to Account / Billing / Login
  - Order-status and checkout/cart URLs are handled more explicitly

IMPORTANT:
  - This is still a business mapping layer and can be refined further.
  - Raw page name and tokens are preserved for auditability.
  - TLife App is intentionally kept as one major group for now because it is a very large and
    distinct app ecosystem in the data. :contentReference[oaicite:2]{index=2}

TOKEN EXTRACTION APPROACH:
  token_1:
    first segment before the first pipe '|'

  token_2:
    first segment after the first pipe, before the first colon ':'

  token_3:
    first segment after the first colon ':'

NOTES:
  - Some rows are malformed, blank, numeric, URL-like, or error-like. These usually fall to Other
    unless clearly mapped into Order Status, Cart / Checkout, Search / Tools, or Privacy / Legal.
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
    /* =============================================================================================
       1. APP / T-LIFE
       Keep all TLife App / Metro App together for now as one app ecosystem.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tlife app', 'metro app')
    THEN 'App / T-Life'

    /* =============================================================================================
       2. HOMEPAGE
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 't-mo prepaid')
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'home'
    THEN 'Homepage'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'landing page'
    THEN 'Homepage'

    /* =============================================================================================
       3. PLP / BROWSE
       Listing, browse, plan exploration, BYOD and similar discovery pages.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 't-mo prepaid')
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
    THEN 'PLP / Browse'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('cell-phone-plans', 'flex', 'plans', 'customer')
    THEN 'PLP / Browse'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'resources'
         AND LOWER(token_3) = 'bring your own phone'
    THEN 'PLP / Browse'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'tienda'
         AND LOWER(token_3) IN ('lista de productos')
    THEN 'PLP / Browse'

    /* =============================================================================================
       4. PDP / DETAIL
       Product/device detail pages.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
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
           'accessory detail'
         )
    THEN 'PDP / Detail'

    /* =============================================================================================
       5. DEALS / OFFERS
       Promotions, offers, campaign pages, discounts, benefits-led deal landers.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) IN ('marketing', 'promotions')
         AND LOWER(token_3) IN ('offers', 'deals', 'benefits', 'home')
    THEN 'Deals / Offers'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'carrier freedom'
         AND LOWER(token_3) = 'home'
    THEN 'Deals / Offers'

    /* =============================================================================================
       6. BRAND / MARKETING
       Brand, acquisition/landing, campaign narrative, benefits hub, T-Fiber, T-Money,
       advertising/solutions, content families, etc.
    ============================================================================================= */
    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN (
           'marketing',
           'brand',
           'benefits',
           'resources',
           'advertising solutions',
           'advertising',
           'content',
           't-fiber',
           't-money'
         )
    THEN 'Brand / Marketing'

    WHEN LOWER(token_1) IN ('tmo:nextgen', 'tmo:uno', 'tmo:nextgen+')
    THEN 'Brand / Marketing'

    WHEN LOWER(token_1) = 'mbyt'
         AND LOWER(token_2) = 'marketing'
         AND LOWER(token_3) = 'landing page'
    THEN 'Brand / Marketing'

    /* =============================================================================================
       7. COVERAGE / NETWORK
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'assurance')
         AND LOWER(token_2) IN ('coverage', 'network')
    THEN 'Coverage / Network'

    /* =============================================================================================
       8. SUPPORT / HELP
       Support, assistance, contact/help style sections.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tfb')
         AND LOWER(token_2) IN ('support', 'asistencia')
    THEN 'Support / Help'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('contact-us', 'contactus')
    THEN 'Support / Help'

    /* =============================================================================================
       9. STORE / LOCATOR
       Store listings, city pages, locator/search/detail pages, Tienda business-detail style pages.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'store'
    THEN 'Store / Locator'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'tienda'
         AND LOWER(token_3) IN ('detalle del negocio', 'buscar', 'página de información', 'city page')
    THEN 'Store / Locator'

    /* =============================================================================================
       10. ORDER STATUS
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'shop'
         AND LOWER(token_3) = 'order status'
    THEN 'Order Status'

    WHEN LOWER(entry_page_name_clean) LIKE '%checkorder%'
      OR LOWER(entry_page_name_clean) LIKE '%order-status%'
      OR LOWER(entry_page_name_clean) LIKE '%/order-status%'
    THEN 'Order Status'

    /* =============================================================================================
       11. CART / CHECKOUT
       Cart, checkout, shipping-payment, finish, review, order creation flow.
    ============================================================================================= */
    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'shop'
         AND LOWER(token_3) IN ('cart', 'empty cart', 'checkout')
    THEN 'Cart / Checkout'

    WHEN LOWER(entry_page_name_clean) LIKE '%/checkout%'
      OR LOWER(entry_page_name_clean) LIKE '%cart%'
      OR LOWER(entry_page_name_clean) LIKE '%shipping-payment%'
      OR LOWER(entry_page_name_clean) LIKE '%esign-agreement%'
      OR LOWER(entry_page_name_clean) LIKE '%finish%'
      OR LOWER(entry_page_name_clean) LIKE '%review%'
      OR LOWER(entry_page_name_clean) LIKE '%hint-order%'
    THEN 'Cart / Checkout'

    /* =============================================================================================
       12. ACCOUNT / BILLING / LOGIN
       Billing, account, guest pay, login, auth, My Phone, payment arrangements, TMO App billandpay.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tmo app')
         AND LOWER(token_2) IN (
           'billing',
           'account',
           'autenticación',
           'cuenta',
           'guest pay',
           'guestpay',
           'my phone',
           'payments',
           'log in',
           'billandpay'
         )
    THEN 'Account / Billing / Login'

    /* =============================================================================================
       13. PRIVACY / LEGAL
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'assurance')
         AND LOWER(token_2) IN ('privacy center', 'legal')
    THEN 'Privacy / Legal'

    /* =============================================================================================
       14. SEARCH / TOOLS
       Search, DNS, HINT, utility/tooling style pages and system-style utility events.
    ============================================================================================= */
    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) IN ('search', 'dns', 'hint')
    THEN 'Search / Tools'

    WHEN LOWER(token_1) IN ('invoca phone call', 'qualtrics survey response', 'mytmo app lifecycle event')
    THEN 'Search / Tools'

    /* =============================================================================================
       15. OTHER
       Fallback bucket for malformed, numeric, error, invalid, unknown, or uncategorized rows.
    ============================================================================================= */
    ELSE 'Other'
  END AS entry_page_group

FROM base;