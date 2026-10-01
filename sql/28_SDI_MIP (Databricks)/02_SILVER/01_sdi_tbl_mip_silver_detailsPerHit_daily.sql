-- ============================================================================
-- FILE  : 01_sdi_sp_mip_silver_detailsPerHit_daily.sql
-- LAYER : SILVER
-- PURPOSE:
--   Canonical hit enrichment; business derivations are centralized here once.
--   Bronze hit-session links are joined directly at their validated unique grain.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Silver canonical hit enrichment from MIP Bronze hits plus validated-unique hit-to-session assignments.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles')),
            -1
        )
    );
    DECLARE v_windowStart DATE;
    DECLARE v_windowEnd DATE;
    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();
    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;
    SET v_windowEnd = v_asOfDate;
    SET v_windowStart = date_add(v_asOfDate, -(p_eventWindowDays - 1));
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Bronze UDI hits returned no rows for the requested Silver window.';
    END IF;
    IF NOT EXISTS (
        SELECT 1
        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'Bronze hit-to-session links returned no rows for the requested Silver window.';
    END IF;
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedWindowStart,
            v_windowEnd AS requestedWindowEnd,
            'sdi_tbl_mip_bronze_edlHits_daily + sdi_tbl_mip_bronze_edlHitSessionLinks_daily' AS sourceObjects,
            'No Silver table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily (
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
            resolvedIdentityId       STRING COMMENT 'Raw UDI fallback identity',
            visitorId                STRING COMMENT 'canonicalUserId first; raw resolvedIdentityId fallback',
            identitySource           STRING,
            siteName                 STRING,
            platform                 STRING,
            pageDomain               STRING,
            pageLayoutState          STRING,
            osName                   STRING,
            pageLanguage             STRING,
            siteSection              STRING,
            pageCategory             STRING,
            pageName                 STRING,
            fullPageName             STRING,
            linkName                 STRING,
            modalName                STRING,
            flowName                 STRING,
            customerType             STRING,
            customerTypeRank         INT,
            authState                STRING,
            authStateRank            INT,
            channelName              STRING,
            externalCampaignCode     STRING,
            campaignCode             STRING,
            lob                      STRING,
            device                   STRING,
            buyFlowStep              STRING,
            buyFlowStepOrder         INT,
            entryEventPageUrlPath    STRING,
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
            isTmoNetwork             INT,
            silverProcessedAt        TIMESTAMP
        )
        USING DELTA
        CLUSTER BY (eventDate, sourceTable)
        COMMENT 'Silver: one enriched row per Bronze UDI hit. Business derivations are centralized here once.';
        WITH scopedLinks AS (
            SELECT
                row_identity_hash,
                event_date,
                source_table,
                session_id,
                canonical_user_id,
                identity_status,
                visitor_key_type,
                visitor_key_value,
                session_assignment_method,
                assignment_version
            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHitSessionLinks_daily
            WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        ),
        scopedHits AS (
            SELECT *
            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
            WHERE event_date BETWEEN v_windowStart AND v_windowEnd
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
            FROM scopedHits h
            LEFT JOIN scopedLinks l
              ON l.row_identity_hash=h.row_identity_hash
             AND l.event_date=h.event_date
             AND l.source_table=h.source_table
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
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
        REPLACE WHERE eventDate BETWEEN v_windowStart AND v_windowEnd
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
                WHEN site_name = 'TMO' AND lower(coalesce(page_url_path, '')) LIKE '%/home-internet%' THEN 'HSI'
                WHEN site_name = 'TMO' THEN 'Postpaid'
                WHEN site_name = 'TFB' THEN 'TFB'
                WHEN site_name IN ('Metro App', 'MbyT') THEN 'Metro'
                WHEN site_name = 'T-Mo Prepaid' THEN 'Prepaid'
                ELSE 'Other'
            END AS lob,
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
                    'guestpay','otp','otp 31+ days past due','set up auto pay','manage autopay','autopay',
                    'rateplanchange','billing','profile','my-wallet','payment-arrangement','choose-payment-method',
                    'raf-customer','raf-prospect','raf-common','insider-submission','na','pa','0:0','statementcount:0'
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
            CASE WHEN lower(coalesce(user_carrier_isp, '')) LIKE 't-mobile%' THEN 1 ELSE 0 END AS isTmoNetwork,
            v_processedAt AS silverProcessedAt
        FROM normalized;
        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedWindowStart,
            v_windowEnd AS loadedWindowEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily' AS targetObject;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / BACKFILL EXAMPLES
-- ============================================================================
-- IMPORTANT PERFORMANCE / DATA-CONTRACT NOTE:
-- sdi_tbl_mip_bronze_edlHitSessionLinks_daily is expected to be unique on
-- (row_identity_hash,event_date,source_table) before this procedure runs.
-- Do not reintroduce ROW_NUMBER() deduplication here; it forces a very large
-- distributed sort/shuffle. The pipeline validation layer should block duplicate
-- Bronze link keys before SILVER_HIT execution.
-- For large historical backfills, run this high-volume procedure one day at a time.
-- Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
--     p_asOfDate=>DATE '2026-09-28',p_eventWindowDays=>1,p_validateOnly=>TRUE
-- );
-- Load/rebuild one completed day:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(
--     p_asOfDate=>DATE '2026-09-28',p_eventWindowDays=>1,p_validateOnly=>FALSE
-- );
-- Do NOT use p_eventWindowDays=>14 for a large backfill. Invoke each date separately
-- with p_eventWindowDays=>1 so each join, write and retry remains day-bounded.
-- Post-load row-count reconciliation:
-- SELECT
--     h.event_date,
--     count(*) AS bronzeHitRows,
--     s.silverRows,
--     s.silverRows-count(*) AS rowDiff
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily h
-- LEFT JOIN (
--     SELECT eventDate,count(*) AS silverRows
--     FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily
--     WHERE eventDate=DATE '2026-09-28'
--     GROUP BY eventDate
-- ) s ON s.eventDate=h.event_date
-- WHERE h.event_date=DATE '2026-09-28'
-- GROUP BY h.event_date,s.silverRows;
-- Expected rowDiff=0 because the SESSION_EVENT_FACT link is a LEFT enrichment.
