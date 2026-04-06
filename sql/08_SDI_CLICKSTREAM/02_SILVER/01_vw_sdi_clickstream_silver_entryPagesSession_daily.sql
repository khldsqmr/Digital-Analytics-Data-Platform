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
  - assigning standardized channel hierarchy fields:
      * TYPE
      * SUB_TYPE
      * MEDIA_TYPE

BUSINESS GRAIN:
  session_id + session_day

IMPORTANT:
  - This view preserves one row per session_id + session_day from Bronze.
  - No aggregation is done here.
  - Channel hierarchy is derived from session_channel_name only, so it is deterministic and safe
    to carry into Gold.

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
    UPPER(TRIM(session_channel_name)) AS channel_raw_upper,
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
  channel_raw_upper,
  adobe_last_touch_channel_name,
  page_view_channel_name,

  token_1,
  token_2,
  token_3,

  /* Better business-friendly tactic names */
  COALESCE(NULLIF(token_1, ''), 'UNKNOWN') AS TACTIC_LEVEL_1,
  COALESCE(NULLIF(token_2, ''), 'UNKNOWN') AS TACTIC_LEVEL_2,
  COALESCE(NULLIF(token_3, ''), 'UNKNOWN') AS TACTIC_LEVEL_3,

  /* Channel hierarchy */
  CASE
    WHEN channel_raw_upper = 'NATURAL SEARCH' THEN 'Media'
    WHEN channel_raw_upper IN (
      'AFFILIATE','PODCAST','STREAMING RADIO','BROADCAST TV','CABLE TV','DIRECT TV',
      'LIVE SPORTS TV','LOCAL TV','SL TV','SUPER BOWL','TUESDAYS','OVER THE TOP',
      'PAID SEARCH: BRAND','PAID SEARCH: NON-BRAND','PAID SEARCH: PLAS','PERFORMANCE MAX',
      'SOCIAL NETWORK - CAMPAIGN','DISPLAY','ON DEVICE','PROGRAMMATIC DISPLAY','ONLINE VIDEO'
    ) THEN 'Media'
    WHEN channel_raw_upper = 'DIRECT' THEN 'Non-Media'
    WHEN channel_raw_upper IN (
      '? TFB _EFL_ _SLN_ ?','CONTENT SYNDICATION','DIRECT MAIL','EMAIL - CAMPAIGN',
      'EMAIL - ORGANIC','OFFLINE','OTHER CAMPAIGNS','REFERRING DOMAINS','SESSION REFRESH',
      'OUT OF HOME','RETAIL STORE','SMS','SOCIAL NETWORK - NATURAL','B2B','NONE'
    ) THEN 'Non-Media'
    ELSE 'UNMAPPED'
  END AS TYPE,

  CASE
    WHEN channel_raw_upper = 'NATURAL SEARCH' THEN 'Organic Search'
    WHEN channel_raw_upper IN ('PAID SEARCH: BRAND','PAID SEARCH: NON-BRAND','PAID SEARCH: PLAS','PERFORMANCE MAX') THEN 'Paid Search'
    WHEN channel_raw_upper = 'SOCIAL NETWORK - CAMPAIGN' THEN 'Paid Social'
    WHEN channel_raw_upper IN ('DISPLAY','ON DEVICE','PROGRAMMATIC DISPLAY','ONLINE VIDEO') THEN 'Programmatic'
    WHEN channel_raw_upper = 'DIRECT' THEN 'Direct'
    WHEN channel_raw_upper IN (
      'AFFILIATE','PODCAST','STREAMING RADIO','BROADCAST TV','CABLE TV','DIRECT TV',
      'LIVE SPORTS TV','LOCAL TV','SL TV','SUPER BOWL','TUESDAYS','OVER THE TOP',
      '? TFB _EFL_ _SLN_ ?','CONTENT SYNDICATION','DIRECT MAIL','EMAIL - CAMPAIGN',
      'EMAIL - ORGANIC','OFFLINE','OTHER CAMPAIGNS','REFERRING DOMAINS','SESSION REFRESH',
      'OUT OF HOME','RETAIL STORE','SMS','SOCIAL NETWORK - NATURAL','B2B','NONE'
    ) THEN 'Other'
    ELSE 'Other'
  END AS SUB_TYPE,

  CASE
    WHEN channel_raw_upper = 'NATURAL SEARCH' THEN 'Search'
    WHEN channel_raw_upper = 'AFFILIATE' THEN 'Affiliate'
    WHEN channel_raw_upper IN ('PODCAST','STREAMING RADIO') THEN 'Audio'
    WHEN channel_raw_upper IN (
      'BROADCAST TV','CABLE TV','DIRECT TV','LIVE SPORTS TV','LOCAL TV','SL TV','SUPER BOWL','TUESDAYS'
    ) THEN 'TV'
    WHEN channel_raw_upper = 'OVER THE TOP' THEN 'Video'
    WHEN channel_raw_upper IN ('PAID SEARCH: BRAND','PAID SEARCH: NON-BRAND','PAID SEARCH: PLAS','PERFORMANCE MAX') THEN 'Search'
    WHEN channel_raw_upper = 'SOCIAL NETWORK - CAMPAIGN' THEN 'Social'
    WHEN channel_raw_upper IN ('DISPLAY','ON DEVICE','PROGRAMMATIC DISPLAY') THEN 'Display'
    WHEN channel_raw_upper = 'ONLINE VIDEO' THEN 'Video'
    WHEN channel_raw_upper = 'DIRECT' THEN 'Other Channels'
    WHEN channel_raw_upper IN (
      '? TFB _EFL_ _SLN_ ?','CONTENT SYNDICATION','DIRECT MAIL','EMAIL - CAMPAIGN',
      'EMAIL - ORGANIC','OFFLINE','OTHER CAMPAIGNS','REFERRING DOMAINS','SESSION REFRESH',
      'B2B','NONE'
    ) THEN 'Other Channels'
    WHEN channel_raw_upper = 'OUT OF HOME' THEN 'Out Of Home'
    WHEN channel_raw_upper = 'RETAIL STORE' THEN 'Retail'
    WHEN channel_raw_upper = 'SMS' THEN 'SMS'
    WHEN channel_raw_upper = 'SOCIAL NETWORK - NATURAL' THEN 'Social'
    ELSE 'UNMAPPED'
  END AS MEDIA_TYPE,

  /* Entry page grouping */
  CASE
    WHEN LOWER(token_1) IN ('tlife app', 'metro app') THEN 'App / T-Life'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 't-mo prepaid')
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'home'
    THEN 'Homepage'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'home'
         AND LOWER(token_3) = 'landing page'
    THEN 'Homepage'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 't-mo prepaid')
         AND LOWER(token_2) IN ('shop', 'store')
         AND LOWER(token_3) IN (
           'browse','browse test','product list','lista de productos','plan','plans',
           'bring your own phone','bring your own device','devices'
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
         AND LOWER(token_3) = 'lista de productos'
    THEN 'PLP / Browse'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) IN ('shop', 'store')
         AND LOWER(token_3) IN (
           'cell phone detail','cellphone detail','product detail','detalle de producto',
           'smart watch detail','tablet detail','tablet & device detail',
           'hotspot & iot detail','accessory detail'
         )
    THEN 'PDP / Detail'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) IN ('marketing', 'promotions')
         AND LOWER(token_3) IN ('offers', 'deals', 'benefits', 'home')
    THEN 'Deals / Offers'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'carrier freedom'
         AND LOWER(token_3) = 'home'
    THEN 'Deals / Offers'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN (
           'marketing','brand','benefits','resources','advertising solutions',
           'advertising','content','t-fiber','t-money'
         )
    THEN 'Brand / Marketing'

    WHEN LOWER(token_1) IN ('tmo:nextgen', 'tmo:uno', 'tmo:nextgen+')
    THEN 'Brand / Marketing'

    WHEN LOWER(token_1) = 'mbyt'
         AND LOWER(token_2) = 'marketing'
         AND LOWER(token_3) = 'landing page'
    THEN 'Brand / Marketing'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'assurance')
         AND LOWER(token_2) IN ('coverage', 'network')
    THEN 'Coverage / Network'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tfb')
         AND LOWER(token_2) IN ('support', 'asistencia')
    THEN 'Support / Help'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) IN ('contact-us', 'contactus')
    THEN 'Support / Help'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'store'
    THEN 'Store / Locator'

    WHEN LOWER(token_1) = 'tmo'
         AND LOWER(token_2) = 'tienda'
         AND LOWER(token_3) IN ('detalle del negocio', 'buscar', 'página de información', 'city page')
    THEN 'Store / Locator'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) = 'shop'
         AND LOWER(token_3) = 'order status'
    THEN 'Order Status'

    WHEN LOWER(entry_page_name_clean) LIKE '%checkorder%'
      OR LOWER(entry_page_name_clean) LIKE '%order-status%'
      OR LOWER(entry_page_name_clean) LIKE '%/order-status%'
    THEN 'Order Status'

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

    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'tmo app')
         AND LOWER(token_2) IN (
           'billing','account','autenticación','cuenta','guest pay','guestpay',
           'my phone','payments','log in','billandpay'
         )
    THEN 'Account / Billing / Login'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt', 'assurance')
         AND LOWER(token_2) IN ('privacy center', 'legal')
    THEN 'Privacy / Legal'

    WHEN LOWER(token_1) IN ('tmo', 'mbyt')
         AND LOWER(token_2) IN ('search', 'dns', 'hint')
    THEN 'Search / Tools'

    WHEN LOWER(token_1) IN ('invoca phone call', 'qualtrics survey response', 'mytmo app lifecycle event')
    THEN 'Search / Tools'

    ELSE 'Other'
  END AS entry_page_group

FROM base;