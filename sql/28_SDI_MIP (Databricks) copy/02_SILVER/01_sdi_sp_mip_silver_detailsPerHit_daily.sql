-- ============================================================================

-- FILE  : 01_sdi_sp_mip_silver_detailsPerHit_daily.sql

-- LAYER : SILVER

-- PURPOSE:

--   Canonical MIP hit enrichment from Bronze UDI + SEF + SSF + Marketing Code.

--

-- GRAIN:

--   One row per valid sessionized UDI hit for OPEN/CLOSED sessions.

--

-- IMPORTANT:

--   NBV is NOT decided here. A session can cross event-date boundaries, so

--   non-bounce qualification is resolved once at attributesPerSession using the

--   complete loaded session hit set: SUM(isPageView) > 1.

--

-- IDENTITY CONTRACT:

--   SSF canonical_user_id is authoritative when present.

--   Raw UDI fallback preserves the manager-approved proven source contract:

--   customer_id -> profile_uid -> encrypted_ban_msisdn ->

--   first_party_id -> app_instance_id.

--

-- ACTION CONTRACT:

--   Preserve proven raw UDI action metrics:

--   event_click_to_call, event_chat_engage, event_store_search,

--   event_cart_add, event_cart_checkout.

--   Assisted-order logic retains page_shipping_options.
--
-- CHANNEL CONTRACT:
--   channelName = exact UDI channel_name acquisition value. Values such as
--   Paid Search: Brand, Paid Search: PLAs and Paid Search: Non-Brand remain
--   separate. navigationChannel/appChannel are different navigation-context
--   fields and are not substituted for the marketing Channel breakout.
--
-- UTM CONTRACT:
--   Web utmSource/utmMedium/utmCampaign are parsed from SSF entry_page_url_full
--   and propagated to the session's hits. App entry URLs are normally NULL, so
--   App UTM values legitimately remain (not set).

--

-- TEMPORARY GEO CONTRACT:

--   Web geoRegion = geo_postal_code.

--   App geoRegion = attribute_country.

--   This is a placeholder dimension, not a normalized geographic region.

--   Missing hit geography remains NULL so session-level first-hit/earliest

--   fallback can still resolve geography from a later hit. Source values such
--   as METRO/RETAIL are intentionally preserved until the upstream geo solution
--   changes them.
--
-- PREFLIGHT / VALIDATION CONTRACT:
--   p_validateOnly=TRUE verifies complete Bronze UDI/SEF date coverage, exact
--   SEF->SSF session availability and the marketing snapshot before writing.
--
-- PEER / IMPACT CONTRACT:
--   No peer-set or impact-on-topline calculation belongs at hit grain. Exact
--   hit-level channelName is retained so a future peer-membership Silver can be
--   derived later without changing this source contract.

-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(

    IN p_asOfDate DATE DEFAULT NULL,

    IN p_eventWindowDays INT DEFAULT 1,

    IN p_validateOnly BOOLEAN DEFAULT FALSE

)

LANGUAGE SQL

SQL SECURITY INVOKER

MODIFIES SQL DATA

COMMENT 'Silver canonical MIP hit enrichment using the proven UDI identity/action contract plus current device and temporary geography logic.'

AS

BEGIN

    DECLARE v_asOfDate DATE DEFAULT coalesce(

        p_asOfDate,

        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)

    );

    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;

    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate,-(p_eventWindowDays-1));

    DECLARE v_processedAt TIMESTAMP DEFAULT current_timestamp();

    DECLARE v_udiDateCount BIGINT DEFAULT 0;

    DECLARE v_sefDateCount BIGINT DEFAULT 0;

    DECLARE v_missingSsfSessionCount BIGINT DEFAULT 0;

    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';

    END IF;

    SET v_udiDateCount=(

        SELECT COUNT(DISTINCT event_date)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily

        WHERE event_date BETWEEN v_windowStart AND v_windowEnd

    );

    IF v_udiDateCount<>p_eventWindowDays THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze UDI does not contain every requested event date. Rebuild Bronze UDI before Silver.';

    END IF;

    SET v_sefDateCount=(

        SELECT COUNT(DISTINCT event_date)

        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

        WHERE event_date BETWEEN v_windowStart AND v_windowEnd

    );

    IF v_sefDateCount<>p_eventWindowDays THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_EVENT_FACT does not contain every requested event date. Rebuild Bronze SEF before Silver.';

    END IF;

    SET v_missingSsfSessionCount=(

        SELECT COUNT(*)

        FROM (

            SELECT DISTINCT e.session_id

            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily e

            LEFT ANTI JOIN prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily s

              ON s.session_id=e.session_id

            WHERE e.event_date BETWEEN v_windowStart AND v_windowEnd

              AND e.session_id IS NOT NULL

        ) missing

    );

    IF v_missingSsfSessionCount>0 THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_SUMMARY_FACT is incomplete for the requested SEF window. Run Bronze SEF first, then Bronze SSF, then rerun Silver.';

    END IF;

    IF NOT EXISTS (

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily

        WHERE session_status IN ('OPEN','CLOSED')

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze SESSION_SUMMARY_FACT returned no OPEN/CLOSED sessions.';

    END IF;

    IF NOT EXISTS (

        SELECT 1

        FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot

        LIMIT 1

    ) THEN

        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='Bronze Marketing Code snapshot is empty.';

    END IF;

    IF p_validateOnly THEN

        SELECT

            'VALIDATION_ONLY' AS status,

            v_windowStart AS requestedEventWindowStart,

            v_windowEnd AS requestedEventWindowEnd,

            v_udiDateCount AS bronzeUdiDateCount,

            v_sefDateCount AS bronzeSefDateCount,

            v_missingSsfSessionCount AS missingBronzeSsfSessions,

            'Bronze UDI + SEF + SSF + Marketing Code' AS sourceObjects,

            'No Silver table was created or modified.' AS message;

    ELSE

        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily (

            rowIdentityHash STRING,

            eventDate DATE,

            sourceTable STRING,

            eventTimestampUtc TIMESTAMP,

            eventTimestampPst TIMESTAMP,

            sessionId STRING,

            hitNumberInSession BIGINT,

            canonicalUserId STRING,

            identityStatus STRING,

            sessionStatus STRING,

            sessionStartTsUtc TIMESTAMP,

            sessionEndTsUtc TIMESTAMP,

            sessionStartTsPst TIMESTAMP,

            sessionStartDatePst DATE,

            weekStartDate DATE,

            weekEndDate DATE,

            resolvedIdentityId STRING,

            visitorId STRING,

            identitySource STRING,

            siteName STRING,

            platform STRING,

            channelType STRING,

            pageLayoutState STRING COMMENT 'UDI page_layout_state; Web responsive form factor',

            operatingSystem STRING COMMENT 'UDI attribute_os_name; primarily App OS',

            device STRING COMMENT 'Derived App Web View / iOS App / Android App / App / Desktop / Mobile Web / Web',

            navigationChannel STRING COMMENT 'Raw UDI channel: site section/top-level navigation bucket',

            appChannel STRING COMMENT 'Raw UDI attribute_channel: app-side channel grouping',

            geoPostalCode STRING COMMENT 'Raw UDI geo_postal_code; Web geography source',

            geoCountry STRING COMMENT 'Raw UDI attribute_country; App geography source',

            geoRegion STRING COMMENT 'Temporary Region placeholder: Web=geo_postal_code, App=attribute_country',

            geoContext STRING COMMENT 'Temporary labeled context: source=web|postalCode=... or source=app|country=...',

            pageCategory STRING,

            pageName STRING,

            fullPageName STRING,

            linkName STRING,

            modalName STRING,

            flowName STRING,

            customerType STRING,

            customerTypeRank INT,

            authState STRING,

            authStateRank INT,

            channelName STRING COMMENT 'Exact UDI channel_name acquisition value; no Paid Search roll-up',

            externalCampaignCode STRING,

            campaignCode STRING,

            campaignName STRING,

            campaignCategory STRING,

            campaignIsActive BOOLEAN,

            lob STRING,

            buyFlowStep STRING,

            buyFlowStepOrder INT,

            sessionEntryPageUrlPath STRING,

            sessionEntryPageUrlFull STRING,

            utmSource STRING,

            utmMedium STRING,

            utmCampaign STRING,

            hitPageUrlPath STRING,

            hitPageUrlFull STRING,

            orderId STRING,

            productOrderType STRING,

            isPageView INT,

            isOrder INT,

            isVrCall INT,

            isVrChat INT,

            isStoreLocator INT,

            isConfigure INT,

            isCheckoutStart INT,

            isBuyFlow INT,

            isAssistedOrder INT,

            isTmoNetwork INT,

            silverProcessedAt TIMESTAMP

        )

        USING DELTA

        CLUSTER BY (sessionStartDatePst,sourceTable)

        COMMENT 'Silver: one enriched row per valid sessionized UDI hit. NBV qualification occurs at session grain downstream.';

        WITH marketingCodeResolved AS (

            SELECT

                cast(MKT_CODE AS STRING) AS MKT_CODE,

                max_by(

                    named_struct(

                        'campaignName',cast(MKT_CODE_NAME AS STRING),

                        'campaignCategory',cast(Category AS STRING),

                        'campaignIsActive',try_cast(is_active AS BOOLEAN)

                    ),

                    struct(

                        CASE WHEN try_cast(is_active AS BOOLEAN)=TRUE THEN 1 ELSE 0 END,

                        coalesce(cast(MKT_CODE_NAME AS STRING),'')

                    )

                ) AS marketing

            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlMarketingCodeDim_snapshot

            GROUP BY cast(MKT_CODE AS STRING)

        ),

        scopedLinks AS (

            SELECT

                row_identity_hash,

                event_date,

                source_table,

                session_id,

                hit_number_in_session,

                flow_name

            FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionEventFact_daily

            WHERE event_date BETWEEN v_windowStart AND v_windowEnd

        ),

        validLinks AS (

            SELECT

                l.row_identity_hash,

                l.event_date,

                l.source_table,

                l.session_id,

                l.hit_number_in_session,

                l.flow_name,

                s.canonical_user_id,

                s.identity_status,

                s.session_status,

                s.session_start_time,

                s.session_end_time,

                s.entry_page_url_path,

                s.entry_page_url_full

            FROM scopedLinks l

            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlSessionSummaryFact_daily s

              ON s.session_id=l.session_id

            WHERE s.session_status IN ('OPEN','CLOSED')

        ),

        base AS (

            SELECT

                h.row_identity_hash,

                h.event_date,

                h.source_table,

                h.event_timestamp_utc,

                h.event_timestamp_pst,

                l.session_id,

                l.hit_number_in_session,

                l.flow_name,

                l.canonical_user_id,

                l.identity_status,

                l.session_status,

                l.session_start_time,

                l.session_end_time,

                l.entry_page_url_path,

                l.entry_page_url_full,

                h.customer_id,

                h.profile_uid,

                h.encrypted_ban_msisdn,

                h.first_party_id,

                h.app_instance_id,

                h.site_name,

                h.page_app_type,

                h.page_layout_state,

                h.attribute_os_name,

                h.channel,

                h.attribute_channel,

                h.geo_postal_code,

                h.attribute_country,

                h.site_sub_section,

                h.page_name,

                h.full_page_name,

                h.link_name,

                h.modal_name,

                h.customer_type,

                h.user_auth_state,

                h.channel_name,

                h.external_campaign_code,

                h.user_carrier_isp,

                h.shipping_method,

                h.page_shipping_options,

                h.alert_message,

                h.page_url_path,

                h.page_url_full,

                h.order_id,

                h.product_order_type,

                h.event_page_view,

                h.event_purchase,

                h.event_click_to_call,

                h.event_chat_engage,

                h.event_store_search,

                h.event_cart_add,

                h.event_cart_checkout

            FROM validLinks l

            INNER JOIN prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily h

              ON h.row_identity_hash=l.row_identity_hash

             AND h.event_date=l.event_date

             AND h.source_table=l.source_table

            WHERE h.event_date BETWEEN v_windowStart AND v_windowEnd

        ),

        normalized AS (

            SELECT

                b.*,

                from_utc_timestamp(try_cast(session_start_time AS TIMESTAMP),'America/Los_Angeles') AS sessionStartTsPst,

                coalesce(

                    nullif(trim(cast(customer_id AS STRING)),''),

                    nullif(trim(cast(profile_uid AS STRING)),''),

                    nullif(trim(cast(encrypted_ban_msisdn AS STRING)),''),

                    nullif(trim(cast(first_party_id AS STRING)),''),

                    nullif(trim(cast(app_instance_id AS STRING)),'')

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

                END AS normalizedAuthStateRank,

                nullif(trim(cast(geo_postal_code AS STRING)),'') AS normalizedGeoPostalCode,

                nullif(trim(cast(attribute_country AS STRING)),'') AS normalizedGeoCountry,

                CASE

                    WHEN source_table='t_web_interactions' THEN nullif(trim(cast(geo_postal_code AS STRING)),'')

                    WHEN source_table='t_app_interactions' THEN nullif(trim(cast(attribute_country AS STRING)),'')

                    ELSE NULL

                END AS normalizedGeoRegion,

                CASE

                    WHEN source_table='t_web_interactions'

                     AND nullif(trim(cast(geo_postal_code AS STRING)),'') IS NOT NULL

                        THEN concat('source=web|postalCode=',trim(cast(geo_postal_code AS STRING)))

                    WHEN source_table='t_app_interactions'

                     AND nullif(trim(cast(attribute_country AS STRING)),'') IS NOT NULL

                        THEN concat('source=app|country=',trim(cast(attribute_country AS STRING)))

                    ELSE NULL

                END AS geoContext,

                nullif(trim(split_part(cast(external_campaign_code AS STRING),'_',4)),'') AS parsedCampaignCode

            FROM base b

        )

        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily

        REPLACE WHERE eventDate BETWEEN v_windowStart AND v_windowEnd

        SELECT

            cast(n.row_identity_hash AS STRING) AS rowIdentityHash,

            cast(n.event_date AS DATE) AS eventDate,

            cast(n.source_table AS STRING) AS sourceTable,

            try_cast(n.event_timestamp_utc AS TIMESTAMP) AS eventTimestampUtc,

            try_cast(n.event_timestamp_pst AS TIMESTAMP) AS eventTimestampPst,

            cast(n.session_id AS STRING) AS sessionId,

            try_cast(n.hit_number_in_session AS BIGINT) AS hitNumberInSession,

            nullif(trim(cast(n.canonical_user_id AS STRING)),'') AS canonicalUserId,

            cast(n.identity_status AS STRING) AS identityStatus,

            cast(n.session_status AS STRING) AS sessionStatus,

            try_cast(n.session_start_time AS TIMESTAMP) AS sessionStartTsUtc,

            try_cast(n.session_end_time AS TIMESTAMP) AS sessionEndTsUtc,

            n.sessionStartTsPst,

            to_date(n.sessionStartTsPst) AS sessionStartDatePst,

            date_add(to_date(n.sessionStartTsPst),1-dayofweek(to_date(n.sessionStartTsPst))) AS weekStartDate,

            date_add(to_date(n.sessionStartTsPst),7-dayofweek(to_date(n.sessionStartTsPst))) AS weekEndDate,

            n.resolvedIdentityId,

            coalesce(nullif(trim(cast(n.canonical_user_id AS STRING)),''),n.resolvedIdentityId) AS visitorId,

            CASE

                WHEN nullif(trim(cast(n.canonical_user_id AS STRING)),'') IS NOT NULL THEN 'canonicalUserId'

                WHEN nullif(trim(cast(n.customer_id AS STRING)),'') IS NOT NULL THEN 'customerId'

                WHEN nullif(trim(cast(n.profile_uid AS STRING)),'') IS NOT NULL THEN 'profileUid'

                WHEN nullif(trim(cast(n.encrypted_ban_msisdn AS STRING)),'') IS NOT NULL THEN 'encryptedBanMsisdn'

                WHEN nullif(trim(cast(n.first_party_id AS STRING)),'') IS NOT NULL THEN 'firstPartyId'

                WHEN nullif(trim(cast(n.app_instance_id AS STRING)),'') IS NOT NULL THEN 'appInstanceId'

                ELSE NULL

            END AS identitySource,

            cast(n.site_name AS STRING) AS siteName,

            cast(n.page_app_type AS STRING) AS platform,

            CASE

                WHEN n.source_table='t_app_interactions' THEN 'App'

                WHEN n.source_table='t_web_interactions' THEN 'Web'

                ELSE 'Unknown'

            END AS channelType,

            nullif(trim(cast(n.page_layout_state AS STRING)),'') AS pageLayoutState,

            nullif(trim(cast(n.attribute_os_name AS STRING)),'') AS operatingSystem,

            CASE

                WHEN n.source_table='t_web_interactions'

                 AND lower(trim(coalesce(cast(n.page_app_type AS STRING),''))) IN ('tlife app','metro app','flagship app')

                    THEN 'App Web View'

                WHEN n.source_table='t_app_interactions'

                 AND lower(trim(coalesce(cast(n.attribute_os_name AS STRING),'')))='ios'

                    THEN 'iOS App'

                WHEN n.source_table='t_app_interactions'

                 AND lower(trim(coalesce(cast(n.attribute_os_name AS STRING),'')))='android'

                    THEN 'Android App'

                WHEN n.source_table='t_app_interactions'

                    THEN 'App'

                WHEN n.source_table='t_web_interactions'

                 AND lower(trim(coalesce(cast(n.page_layout_state AS STRING),'')))='desktop'

                    THEN 'Desktop'

                WHEN n.source_table='t_web_interactions'

                 AND lower(trim(coalesce(cast(n.page_layout_state AS STRING),''))) IN ('mobile','tablet')

                    THEN 'Mobile Web'

                WHEN n.source_table='t_web_interactions'

                    THEN 'Web'

                ELSE 'Unknown'

            END AS device,

            nullif(trim(cast(n.channel AS STRING)),'') AS navigationChannel,

            nullif(trim(cast(n.attribute_channel AS STRING)),'') AS appChannel,

            n.normalizedGeoPostalCode AS geoPostalCode,

            n.normalizedGeoCountry AS geoCountry,

            n.normalizedGeoRegion AS geoRegion,

            n.geoContext AS geoContext,

            nullif(trim(cast(n.site_sub_section AS STRING)),'') AS pageCategory,

            nullif(trim(cast(n.page_name AS STRING)),'') AS pageName,

            cast(n.full_page_name AS STRING) AS fullPageName,

            cast(n.link_name AS STRING) AS linkName,

            cast(n.modal_name AS STRING) AS modalName,

            nullif(trim(cast(n.flow_name AS STRING)),'') AS flowName,

            n.normalizedCustomerType AS customerType,

            n.normalizedCustomerTypeRank AS customerTypeRank,

            n.normalizedAuthState AS authState,

            n.normalizedAuthStateRank AS authStateRank,

            -- MIP Channel breakout remains channel_name. Do not substitute

            -- raw channel/attribute_channel; those are navigation/property fields.

            nullif(trim(cast(n.channel_name AS STRING)),'') AS channelName,

            cast(n.external_campaign_code AS STRING) AS externalCampaignCode,

            n.parsedCampaignCode AS campaignCode,

            m.marketing.campaignName AS campaignName,

            m.marketing.campaignCategory AS campaignCategory,

            m.marketing.campaignIsActive AS campaignIsActive,

            CASE

                WHEN n.site_name='TMO' AND lower(coalesce(n.page_url_path,'')) LIKE '%/home-internet%' THEN 'HSI'

                WHEN n.site_name='TMO' THEN 'Postpaid'

                WHEN n.site_name='TFB' THEN 'TFB'

                WHEN n.site_name IN ('Metro App','MbyT') THEN 'Metro'

                WHEN n.site_name='T-Mo Prepaid' THEN 'Prepaid'

                ELSE 'Other'

            END AS lob,

            CASE

                WHEN n.site_sub_section IN ('Plan','Plans','Change Plan') THEN 'Plan select'

                WHEN n.site_sub_section='Cart' THEN 'Cart'

                WHEN n.site_sub_section IN ('Accessory Detail','Accessories') THEN 'Accessories add-on'

                WHEN n.site_sub_section='Upgrade Trade-In' THEN 'Trade-in'

                WHEN n.site_sub_section IN ('Checkout','Slim Checkout','Choose Payment Method') THEN 'Checkout'

                WHEN n.site_sub_section IN ('Confirmation','One Time Payment Confirmation') THEN 'Confirmation'

                ELSE NULL

            END AS buyFlowStep,

            CASE

                WHEN n.site_sub_section IN ('Plan','Plans','Change Plan') THEN 1

                WHEN n.site_sub_section='Cart' THEN 2

                WHEN n.site_sub_section IN ('Accessory Detail','Accessories') THEN 3

                WHEN n.site_sub_section='Upgrade Trade-In' THEN 4

                WHEN n.site_sub_section IN ('Checkout','Slim Checkout','Choose Payment Method') THEN 5

                WHEN n.site_sub_section IN ('Confirmation','One Time Payment Confirmation') THEN 6

                ELSE NULL

            END AS buyFlowStepOrder,

            cast(n.entry_page_url_path AS STRING) AS sessionEntryPageUrlPath,

            cast(n.entry_page_url_full AS STRING) AS sessionEntryPageUrlFull,

            coalesce(

                nullif(try_parse_url(

                    CASE

                        WHEN lower(coalesce(n.entry_page_url_full,'')) LIKE 'http%' THEN n.entry_page_url_full

                        WHEN nullif(trim(n.entry_page_url_full),'') IS NOT NULL THEN concat('https://',n.entry_page_url_full)

                        ELSE NULL

                    END,

                    'QUERY','utm_source'

                ),''),

                '(not set)'

            ) AS utmSource,

            coalesce(

                nullif(try_parse_url(

                    CASE

                        WHEN lower(coalesce(n.entry_page_url_full,'')) LIKE 'http%' THEN n.entry_page_url_full

                        WHEN nullif(trim(n.entry_page_url_full),'') IS NOT NULL THEN concat('https://',n.entry_page_url_full)

                        ELSE NULL

                    END,

                    'QUERY','utm_medium'

                ),''),

                '(not set)'

            ) AS utmMedium,

            coalesce(

                nullif(try_parse_url(

                    CASE

                        WHEN lower(coalesce(n.entry_page_url_full,'')) LIKE 'http%' THEN n.entry_page_url_full

                        WHEN nullif(trim(n.entry_page_url_full),'') IS NOT NULL THEN concat('https://',n.entry_page_url_full)

                        ELSE NULL

                    END,

                    'QUERY','utm_campaign'

                ),''),

                '(not set)'

            ) AS utmCampaign,

            cast(n.page_url_path AS STRING) AS hitPageUrlPath,

            cast(n.page_url_full AS STRING) AS hitPageUrlFull,

            nullif(trim(cast(n.order_id AS STRING)),'') AS orderId,

            cast(n.product_order_type AS STRING) AS productOrderType,

            CASE WHEN coalesce(try_cast(n.event_page_view AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isPageView,

            CASE WHEN coalesce(try_cast(n.event_purchase AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isOrder,

            CASE WHEN coalesce(try_cast(n.event_click_to_call AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isVrCall,

            CASE WHEN coalesce(try_cast(n.event_chat_engage AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isVrChat,

            CASE WHEN coalesce(try_cast(n.event_store_search AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isStoreLocator,

            CASE WHEN coalesce(try_cast(n.event_cart_add AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isConfigure,

            CASE WHEN coalesce(try_cast(n.event_cart_checkout AS BIGINT),0)>0 THEN 1 ELSE 0 END AS isCheckoutStart,

            CASE

                WHEN nullif(trim(cast(n.flow_name AS STRING)),'') IS NULL THEN 0

                WHEN lower(trim(cast(n.flow_name AS STRING))) IN (

                    'guestpay','otp','otp 31+ days past due','set up auto pay','manage autopay','autopay',

                    'rateplanchange','billing','profile','my-wallet','payment-arrangement','choose-payment-method',

                    'raf-customer','raf-prospect','raf-common','insider-submission','na','pa','0:0','statementcount:0'

                ) THEN 0

                ELSE 1

            END AS isBuyFlow,

            CASE

                WHEN coalesce(try_cast(n.event_purchase AS BIGINT),0)>0

                 AND (

                    lower(coalesce(n.modal_name,'')) LIKE '%buy online while in store is available%'

                    OR lower(coalesce(n.shipping_method,'')) LIKE '%while in store%'

                    OR lower(coalesce(n.shipping_method,'')) LIKE '%comprar por internet desde una tienda%'

                    OR n.external_campaign_code='MGPO_RS_P_PPMGNWLRSU_9FED46B36BD7D485135485'

                    OR lower(coalesce(n.page_shipping_options,'')) LIKE '%online while in store%'

                    OR (

                        n.modal_name IN (

                            'Welcome to T-Mobile - Store In Store - Sam''s Club',

                            'Welcome to T-Mobile - Store In Store - Costco'

                        )

                        AND n.full_page_name LIKE 'TLife App |%'

                    )

                    OR n.alert_message='Message: Screen share in progress'

                    OR (n.page_name='About Screen Share' AND n.link_name='clickToAction')

                    OR lower(coalesce(n.page_url_full,'')) LIKE '%assist.t-mobile%'

                 )

                THEN 1 ELSE 0

            END AS isAssistedOrder,

            CASE WHEN lower(coalesce(n.user_carrier_isp,'')) LIKE 't-mobile%' THEN 1 ELSE 0 END AS isTmoNetwork,

            v_processedAt AS silverProcessedAt

        FROM normalized n

        LEFT JOIN marketingCodeResolved m

          ON n.parsedCampaignCode=m.MKT_CODE;

        SELECT

            'SUCCESS' AS status,

            v_windowStart AS loadedEventWindowStart,

            v_windowEnd AS loadedEventWindowEnd,

            'prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily' AS targetObject;

    END IF;

END;

-- ============================================================================

-- DEVELOPMENT / TEST EXAMPLES

-- Run these statements separately after deploying the procedure.

-- ============================================================================

-- --------------------------------------------------------------------------

-- A. PREFLIGHT ONLY

-- Creates/writes nothing.

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(

--     p_asOfDate        => DATE '2026-10-02',

--     p_eventWindowDays => 1,

--     p_validateOnly    => TRUE

-- );

-- --------------------------------------------------------------------------

-- B. EXECUTE / REBUILD ONE EVENT DAY

-- --------------------------------------------------------------------------

-- CALL prdrzranalytics.lab42.sdi_sp_mip_silver_detailsPerHit_daily(

--     p_asOfDate        => DATE '2026-10-02',

--     p_eventWindowDays => 1,

--     p_validateOnly    => FALSE

-- );

-- --------------------------------------------------------------------------

-- C. VALIDATION 1: BASIC HIT CONTRACT

-- Expected:

--   missingSessionId = 0

--   invalidSessionStatusRows = 0

-- --------------------------------------------------------------------------

-- SELECT

--     eventDate,

--     COUNT(*) AS hitRows,

--     COUNT(DISTINCT sessionId) AS sessions,

--     COUNT_IF(sessionId IS NULL) AS missingSessionId,

--     COUNT_IF(sessionStatus NOT IN ('OPEN','CLOSED')) AS invalidSessionStatusRows,

--     SUM(isPageView) AS pageViews,

--     SUM(isOrder) AS orders

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily

-- WHERE eventDate = DATE '2026-10-02'

-- GROUP BY eventDate;

-- --------------------------------------------------------------------------

-- D. VALIDATION 2: JOIN-GRAIN / ROW-EXPLOSION CHECK

-- Expected: no rows for a source slice that is unique at the composite key.

-- This is a diagnostic check, not a permanent uniqueness contract for UDI.

-- --------------------------------------------------------------------------

-- SELECT

--     rowIdentityHash,

--     eventDate,

--     sourceTable,

--     COUNT(*) AS rowCount

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily

-- WHERE eventDate = DATE '2026-10-02'

-- GROUP BY rowIdentityHash,eventDate,sourceTable

-- HAVING COUNT(*) > 1

-- ORDER BY rowCount DESC

-- LIMIT 100;

-- --------------------------------------------------------------------------

-- E. VALIDATION 3: PLATFORM / DEVICE / GEO SANITY

-- Useful after the device-definition change.

-- --------------------------------------------------------------------------

-- SELECT

--     sourceTable,

--     platform,

--     pageLayoutState,

--     operatingSystem,

--     device,

--     geoPostalCode,

--     geoCountry,

--     geoRegion,

--     COUNT(*) AS hitRows,

--     SUM(isPageView) AS pageViews

-- FROM prdrzranalytics.lab42.sdi_tbl_mip_silver_detailsPerHit_daily

-- WHERE eventDate = DATE '2026-10-02'

-- GROUP BY sourceTable,platform,pageLayoutState,operatingSystem,device,geoPostalCode,geoCountry,geoRegion

-- ORDER BY hitRows DESC;
