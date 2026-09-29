
-- ###########################################################################
-- BEGIN silver/01_sdi_tbl_mip_silver_detailsPerHit_daily.sql
-- ###########################################################################

-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_silver_detailsPerHit_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   Canonical hit enrichment; business derivations are centralized here once.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;
-- ----------------------------------------------------------------------------
-- Canonical enriched hit
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_silver_detailsPerHit_daily (
  rowIdentityHash          STRING,
  eventDate                DATE,
  sourceTable              STRING,
  eventTimestampUtc        TIMESTAMP,
  eventTimestampPst        TIMESTAMP,
  eventDatePst             DATE,
  loadDatetimePst          TIMESTAMP,

  sessionId                STRING,
  canonicalUserId          STRING,
  identityStatus           STRING,
  visitorKeyType           STRING,
  visitorKeyValue          STRING,
  sessionAssignmentMethod  STRING,
  sessionAssignmentVersion BIGINT,
  isSessionized            INT,

  resolvedIdentityId       STRING COMMENT 'Raw UDI fallback identity: customer_id, profile_uid, encrypted_ban_msisdn, first_party_id, app_instance_id',
  visitorId                STRING COMMENT 'canonicalUserId first; raw resolvedIdentityId fallback. NULL if neither exists',
  identitySource           STRING,

  siteName                 STRING,
  platform                 STRING COMMENT 'page_app_type',
  pageDomain               STRING,
  pageLayoutState          STRING,
  osName                   STRING,
  pageLanguage             STRING,
  siteSection              STRING COMMENT 'raw channel page-context field; NOT marketing channel',
  pageCategory             STRING COMMENT 'site_sub_section',
  pageName                 STRING,
  fullPageName             STRING,
  linkName                 STRING,
  modalName                STRING,
  flowName                 STRING,

  customerType             STRING COMMENT 'Prospect | Customer | Care | Unknown',
  customerTypeRank         INT,
  authState                STRING COMMENT 'Authenticated | Network Authenticated | LoggedOut | Ambiguous | (not set)',
  authStateRank            INT,

  channelName              STRING COMMENT 'canonical marketing channel_name',
  externalCampaignCode     STRING,
  campaignCode             STRING COMMENT '4th underscore token of external_campaign_code',

  lob                      STRING,
  device                   STRING COMMENT 'PROPOSED device grouping for prototype; confirm business definition',
  buyFlowStep              STRING,
  buyFlowStepOrder         INT,

  entryEventPageUrlPath    STRING COMMENT 'hit page_url_path; session entry page comes from SSF',
  hitPageUrlFull           STRING,

  orderId                  STRING,
  productOrderType         STRING,

  isPageView               INT,
  isOrder                  INT,
  isVrCall                 INT,
  isVrChat                 INT,
  isStoreLocator           INT,
  isConfigure              INT,
  isCheckoutStart          INT,
  isProductView            INT,
  isBuyFlow                INT,
  isAssistedOrder          INT,

  isTmoNetwork             INT COMMENT 'Diagnostic / Adobe-comparison helper only; not a general exclusion',

  _runId                   STRING,
  silverProcessedAt        TIMESTAMP
)
USING DELTA
CLUSTER BY (eventDate, sourceTable)
COMMENT 'Silver: one enriched row per Bronze UDI hit. Business derivations are centralized here once.';

CREATE OR REPLACE PROCEDURE sdi_sp_mip_silver_detailsPerHit_daily(
  p_runId           STRING,
  p_asOfDate        DATE DEFAULT NULL,
  p_eventWindowDays INT DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
AS
BEGIN
  DECLARE v_asOfDate    DATE DEFAULT coalesce(
    p_asOfDate,
    to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
  );
  DECLARE v_windowEnd   DATE DEFAULT v_asOfDate;
  DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));
  DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

  IF p_eventWindowDays < 1 THEN
    SIGNAL SQLSTATE '45000'
      SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1';
  END IF;

  INSERT INTO sdi_tbl_mip_silver_detailsPerHit_daily
  REPLACE WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
  WITH linkDedup AS (
    SELECT *
    FROM sdi_tbl_mip_bronze_edlHitSessionLinks_daily
    WHERE event_date BETWEEN v_windowStart AND v_windowEnd
    QUALIFY row_number() OVER (
      PARTITION BY row_identity_hash, event_date, source_table
      ORDER BY load_datetime_pst DESC NULLS LAST
    ) = 1
  ),
  base AS (
    SELECT
      h.*,
      l.session_id,
      l.canonical_user_id,
      l.identity_status,
      l.visitor_key_type,
      l.visitor_key_value,
      l.session_assignment_method,
      l.assignment_version
    FROM sdi_tbl_mip_bronze_edlHits_daily h
    LEFT JOIN linkDedup l
      ON  l.row_identity_hash = h.row_identity_hash
      AND l.event_date = h.event_date
      AND l.source_table = h.source_table
    WHERE h.event_date BETWEEN v_windowStart AND v_windowEnd
  ),
  normalized AS (
    SELECT
      b.*,
      coalesce(
        nullif(trim(cast(customer_id AS STRING)), ''),
        nullif(trim(cast(profile_uid AS STRING)), ''),
        nullif(trim(cast(encrypted_ban_msisdn AS STRING)), ''),
        nullif(trim(cast(first_party_id AS STRING)), ''),
        nullif(trim(cast(app_instance_id AS STRING)), '')
      ) AS resolvedIdentityId,

      CASE lower(trim(cast(customer_type AS STRING)))
        WHEN 'prospect' THEN 'Prospect'
        WHEN 'customer' THEN 'Customer'
        WHEN 'care' THEN 'Care'
        ELSE 'Unknown'
      END AS normalizedCustomerType,

      CASE lower(trim(cast(customer_type AS STRING)))
        WHEN 'customer' THEN 3
        WHEN 'care' THEN 2
        WHEN 'prospect' THEN 1
        ELSE 0
      END AS normalizedCustomerTypeRank,

      CASE lower(trim(cast(user_auth_state AS STRING)))
        WHEN 'authenticated' THEN 'Authenticated'
        WHEN 'network authenticated' THEN 'Network Authenticated'
        WHEN 'loggedout' THEN 'LoggedOut'
        WHEN 'ambiguous' THEN 'Ambiguous'
        ELSE '(not set)'
      END AS normalizedAuthState,

      CASE lower(trim(cast(user_auth_state AS STRING)))
        WHEN 'authenticated' THEN 3
        WHEN 'network authenticated' THEN 2
        WHEN 'loggedout' THEN 1
        WHEN 'ambiguous' THEN 0
        ELSE -1
      END AS normalizedAuthStateRank
    FROM base b
  )
  SELECT
    cast(row_identity_hash AS STRING) AS rowIdentityHash,
    cast(event_date AS DATE) AS eventDate,
    cast(source_table AS STRING) AS sourceTable,
    try_cast(event_timestamp_utc AS TIMESTAMP) AS eventTimestampUtc,
    try_cast(event_timestamp_pst AS TIMESTAMP) AS eventTimestampPst,
    to_date(try_cast(event_timestamp_pst AS TIMESTAMP)) AS eventDatePst,
    try_cast(load_datetime_pst AS TIMESTAMP) AS loadDatetimePst,

    cast(session_id AS STRING) AS sessionId,
    nullif(trim(cast(canonical_user_id AS STRING)), '') AS canonicalUserId,
    cast(identity_status AS STRING) AS identityStatus,
    cast(visitor_key_type AS STRING) AS visitorKeyType,
    cast(visitor_key_value AS STRING) AS visitorKeyValue,
    cast(session_assignment_method AS STRING) AS sessionAssignmentMethod,
    try_cast(assignment_version AS BIGINT) AS sessionAssignmentVersion,
    CASE WHEN session_id IS NOT NULL THEN 1 ELSE 0 END AS isSessionized,

    resolvedIdentityId,
    coalesce(nullif(trim(cast(canonical_user_id AS STRING)), ''), resolvedIdentityId) AS visitorId,
    CASE
      WHEN nullif(trim(cast(canonical_user_id AS STRING)), '') IS NOT NULL THEN 'canonicalUserId'
      WHEN nullif(trim(cast(customer_id AS STRING)), '') IS NOT NULL THEN 'customerId'
      WHEN nullif(trim(cast(profile_uid AS STRING)), '') IS NOT NULL THEN 'profileUid'
      WHEN nullif(trim(cast(encrypted_ban_msisdn AS STRING)), '') IS NOT NULL THEN 'encryptedBanMsisdn'
      WHEN nullif(trim(cast(first_party_id AS STRING)), '') IS NOT NULL THEN 'firstPartyId'
      WHEN nullif(trim(cast(app_instance_id AS STRING)), '') IS NOT NULL THEN 'appInstanceId'
      ELSE NULL
    END AS identitySource,

    cast(site_name AS STRING) AS siteName,
    cast(page_app_type AS STRING) AS platform,
    cast(page_domain AS STRING) AS pageDomain,
    cast(page_layout_state AS STRING) AS pageLayoutState,
    cast(attribute_os_name AS STRING) AS osName,
    cast(page_language AS STRING) AS pageLanguage,
    cast(channel AS STRING) AS siteSection,
    nullif(trim(cast(site_sub_section AS STRING)), '') AS pageCategory,
    nullif(trim(cast(page_name AS STRING)), '') AS pageName,
    cast(full_page_name AS STRING) AS fullPageName,
    cast(link_name AS STRING) AS linkName,
    cast(modal_name AS STRING) AS modalName,
    nullif(trim(cast(flow_name AS STRING)), '') AS flowName,

    normalizedCustomerType AS customerType,
    normalizedCustomerTypeRank AS customerTypeRank,
    normalizedAuthState AS authState,
    normalizedAuthStateRank AS authStateRank,

    nullif(trim(cast(channel_name AS STRING)), '') AS channelName,
    cast(external_campaign_code AS STRING) AS externalCampaignCode,
    nullif(trim(split_part(cast(external_campaign_code AS STRING), '_', 4)), '') AS campaignCode,

    CASE
      WHEN site_name = 'TMO'
       AND lower(coalesce(page_url_path, '')) LIKE '%/home-internet%' THEN 'HSI'
      WHEN site_name = 'TMO' THEN 'Postpaid'
      WHEN site_name = 'TFB' THEN 'TFB'
      WHEN site_name IN ('Metro App', 'MbyT') THEN 'Metro'
      WHEN site_name = 'T-Mo Prepaid' THEN 'Prepaid'
      ELSE 'Other'
    END AS lob,

    -- PROPOSED only: retained because the prototype requires Device.
    CASE
      WHEN page_app_type IN ('TLife App', 'Metro App', 'Flagship App')
       AND lower(coalesce(attribute_os_name, '')) = 'ios' THEN 'iOS App'
      WHEN page_app_type IN ('TLife App', 'Metro App', 'Flagship App')
       AND lower(coalesce(attribute_os_name, '')) = 'android' THEN 'Android App'
      WHEN page_layout_state = 'Native App' THEN 'App Web View'
      WHEN page_layout_state = 'Desktop' THEN 'Desktop'
      WHEN page_layout_state = 'Mobile' THEN 'Mobile Web'
      WHEN page_layout_state = 'Tablet' THEN 'Tablet'
      ELSE 'Unknown'
    END AS device,

    CASE
      WHEN site_sub_section IN ('Plan', 'Plans', 'Change Plan') THEN 'Plan select'
      WHEN site_sub_section = 'Cart' THEN 'Cart'
      WHEN site_sub_section IN ('Accessory Detail', 'Accessories') THEN 'Accessories add-on'
      WHEN site_sub_section = 'Upgrade Trade-In' THEN 'Trade-in'
      WHEN site_sub_section IN ('Checkout', 'Slim Checkout', 'Choose Payment Method') THEN 'Checkout'
      WHEN site_sub_section IN ('Confirmation', 'One Time Payment Confirmation') THEN 'Confirmation'
      ELSE NULL
    END AS buyFlowStep,

    CASE
      WHEN site_sub_section IN ('Plan', 'Plans', 'Change Plan') THEN 1
      WHEN site_sub_section = 'Cart' THEN 2
      WHEN site_sub_section IN ('Accessory Detail', 'Accessories') THEN 3
      WHEN site_sub_section = 'Upgrade Trade-In' THEN 4
      WHEN site_sub_section IN ('Checkout', 'Slim Checkout', 'Choose Payment Method') THEN 5
      WHEN site_sub_section IN ('Confirmation', 'One Time Payment Confirmation') THEN 6
      ELSE NULL
    END AS buyFlowStepOrder,

    cast(page_url_path AS STRING) AS entryEventPageUrlPath,
    cast(page_url_full AS STRING) AS hitPageUrlFull,

    nullif(trim(cast(order_id AS STRING)), '') AS orderId,
    cast(product_order_type AS STRING) AS productOrderType,

    CASE WHEN coalesce(try_cast(event_page_view AS INT), 0) > 0 THEN 1 ELSE 0 END AS isPageView,
    CASE WHEN coalesce(try_cast(event_purchase AS INT), 0) > 0 THEN 1 ELSE 0 END AS isOrder,
    CASE WHEN coalesce(try_cast(event_click_to_call AS INT), 0) > 0 THEN 1 ELSE 0 END AS isVrCall,
    CASE WHEN coalesce(try_cast(event_chat_engage AS INT), 0) > 0 THEN 1 ELSE 0 END AS isVrChat,
    CASE WHEN coalesce(try_cast(event_store_search AS INT), 0) > 0 THEN 1 ELSE 0 END AS isStoreLocator,
    CASE WHEN coalesce(try_cast(event_cart_add AS INT), 0) > 0 THEN 1 ELSE 0 END AS isConfigure,
    CASE WHEN coalesce(try_cast(event_cart_checkout AS INT), 0) > 0 THEN 1 ELSE 0 END AS isCheckoutStart,
    CASE WHEN coalesce(try_cast(event_product_view AS INT), 0) > 0 THEN 1 ELSE 0 END AS isProductView,

    CASE
      WHEN nullif(trim(cast(flow_name AS STRING)), '') IS NULL THEN 0
      WHEN lower(trim(cast(flow_name AS STRING))) IN (
        'guestpay', 'otp', 'otp 31+ days past due',
        'set up auto pay', 'manage autopay', 'autopay',
        'rateplanchange', 'billing', 'profile', 'my-wallet',
        'payment-arrangement', 'choose-payment-method',
        'raf-customer', 'raf-prospect', 'raf-common',
        'insider-submission', 'na', 'pa', '0:0', 'statementcount:0'
      ) THEN 0
      ELSE 1
    END AS isBuyFlow,

    CASE
      WHEN coalesce(try_cast(event_purchase AS INT), 0) > 0
       AND (
         lower(coalesce(modal_name, '')) LIKE '%buy online while in store is available%'
         OR lower(coalesce(shipping_method, '')) LIKE '%while in store%'
         OR lower(coalesce(shipping_method, '')) LIKE '%comprar por internet desde una tienda%'
         OR external_campaign_code = 'MGPO_RS_P_PPMGNWLRSU_9FED46B36BD7D485135485'
         OR lower(coalesce(page_shipping_options, '')) LIKE '%online while in store%'
         OR (
           modal_name IN (
             'Welcome to T-Mobile - Store In Store - Sam''s Club',
             'Welcome to T-Mobile - Store In Store - Costco'
           )
           AND full_page_name LIKE 'TLife App |%'
         )
         OR alert_message = 'Message: Screen share in progress'
         OR (page_name = 'About Screen Share' AND link_name = 'clickToAction')
         OR lower(coalesce(page_url_full, '')) LIKE '%assist.t-mobile%'
       )
      THEN 1 ELSE 0
    END AS isAssistedOrder,

    CASE
      WHEN lower(coalesce(user_carrier_isp, '')) LIKE 't-mobile%' THEN 1
      ELSE 0
    END AS isTmoNetwork,

    p_runId AS _runId,
    v_processedAt AS silverProcessedAt
  FROM normalized;
END;

-- ----------------------------------------------------------------------------


-- ###########################################################################
-- END silver/01_sdi_tbl_mip_silver_detailsPerHit_daily.sql
-- ###########################################################################

