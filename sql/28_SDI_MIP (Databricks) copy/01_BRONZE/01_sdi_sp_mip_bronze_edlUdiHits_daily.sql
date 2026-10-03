-- ============================================================================
-- FILE  : 01_sdi_sp_mip_bronze_edlUdiHits_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
-- PURPOSE:
--   Persist the narrow MIP UDI projection for a requested event window.
--
-- DEVICE CONTRACT:
--   page_app_type       = logical property/surface
--   page_layout_state   = Web responsive form factor (desktop/mobile/tablet)
--   attribute_os_name   = App OS (ios/android)
--
-- GEO CONTRACT:
--   geo_* fields are IP-derived hit geography. geo_region is state/province,
--   not an internal T-Mobile sales/marketing region.
--
-- FUTURE CONTEXT:
--   Additional source/user/flow/order/navigation fields are retained in Bronze
--   so future MIP dimensions do not require re-reading the full UDI schema.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze MIP projection of EDL unified_digital_interactions using the validated UDI source/device/geography contract.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1)
    );
    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;
    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate,-(p_eventWindowDays-1));

    IF p_eventWindowDays IS NULL OR p_eventWindowDays<1 THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='p_eventWindowDays must be >= 1.';
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='UDI returned no rows for the requested Bronze window. Bronze was not created or refreshed.';
    END IF;

    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart AS requestedWindowStart,
            v_windowEnd AS requestedWindowEnd,
            'prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions' AS sourceObject,
            'Uses page_app_type + page_layout_state + attribute_os_name for platform/device and geo_region for Region. No Bronze table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        USING DELTA
        CLUSTER BY (event_date,source_table)
        COMMENT 'Bronze: narrow MIP projection of UDI. One row per retained UDI source row.'
        AS
        SELECT
            row_identity_hash,event_date,source_table,event_timestamp_utc,event_timestamp_pst,

            customer_id,profile_uid,encrypted_ban_msisdn,first_party_id,app_instance_id,

            site_name,page_app_type,page_layout_state,attribute_os_name,
            page_language,browser_language,app_launch_type,app_launch_status,

            geo_country,geo_region,geo_city,geo_dma,geo_zip,geo_latitude,geo_longitude,

            channel,attribute_channel,channel_name,
            site_sub_section,page_name,full_page_name,link_name,modal_name,
            page_flow_type,previous_page_name,navigation_intnav,navigation_menu,

            customer_type,user_type,user_auth_state,
            user_account_type,user_account_status,user_account_category,user_role,
            user_credit_class,credit_result,user_engagement_type,customer_indicator,

            carrier_name,attribute_network_device_carrier,attribute_network_connection_type,
            user_carrier_isp,

            flow_name,attribute_flow_name,flow_type,
            external_campaign_code,

            shipping_method,page_shipping_options,payment_method_type,
            alert_message,page_url_path,page_url_full,

            order_id,product_order_type,order_status,trade_in_status,eip_status,
            cart_device_type,service_plan_tier,current_plan,new_plan,

            attribute_event_category,attribute_event_type,attribute_event_action,
            attribute_screen_name,webinteraction_type,link_type,

            event_page_view,event_purchase,event_click_to_call,event_chat_engage,
            event_store_search,event_cart_add,event_cart_checkout,

            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE 1=0;

        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        REPLACE WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        SELECT
            row_identity_hash,event_date,source_table,event_timestamp_utc,event_timestamp_pst,

            customer_id,profile_uid,encrypted_ban_msisdn,first_party_id,app_instance_id,

            site_name,page_app_type,page_layout_state,attribute_os_name,
            page_language,browser_language,app_launch_type,app_launch_status,

            geo_country,geo_region,geo_city,geo_dma,geo_zip,geo_latitude,geo_longitude,

            channel,attribute_channel,channel_name,
            site_sub_section,page_name,full_page_name,link_name,modal_name,
            page_flow_type,previous_page_name,navigation_intnav,navigation_menu,

            customer_type,user_type,user_auth_state,
            user_account_type,user_account_status,user_account_category,user_role,
            user_credit_class,credit_result,user_engagement_type,customer_indicator,

            carrier_name,attribute_network_device_carrier,attribute_network_connection_type,
            user_carrier_isp,

            flow_name,attribute_flow_name,flow_type,
            external_campaign_code,

            shipping_method,page_shipping_options,payment_method_type,
            alert_message,page_url_path,page_url_full,

            order_id,product_order_type,order_status,trade_in_status,eip_status,
            cart_device_type,service_plan_tier,current_plan,new_plan,

            attribute_event_category,attribute_event_type,attribute_event_action,
            attribute_screen_name,webinteraction_type,link_type,

            event_page_view,event_purchase,event_click_to_call,event_chat_engage,
            event_store_search,event_cart_add,event_cart_checkout,

            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd;

        SELECT
            'SUCCESS' AS status,
            v_windowStart AS loadedWindowStart,
            v_windowEnd AS loadedWindowEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily' AS targetObject;
    END IF;
END;

-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--   p_asOfDate=>DATE '2026-09-28',p_eventWindowDays=>1,p_validateOnly=>TRUE);

-- B. EXECUTE
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--   p_asOfDate=>DATE '2026-09-28',p_eventWindowDays=>1,p_validateOnly=>FALSE);

-- C. DEVICE / GEO COVERAGE
-- SELECT
--   source_table,page_app_type,page_layout_state,attribute_os_name,
--   COUNT(*) AS rows,
--   COUNT_IF(nullif(trim(cast(geo_region AS STRING)),'') IS NOT NULL) AS rowsWithGeoRegion,
--   ROUND(100.0*COUNT_IF(nullif(trim(cast(geo_region AS STRING)),'') IS NOT NULL)/COUNT(*),2) AS geoRegionPct
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-09-28'
-- GROUP BY source_table,page_app_type,page_layout_state,attribute_os_name
-- ORDER BY rows DESC;

-- D. GEO RAW COVERAGE
-- SELECT
--   COUNT(*) AS rows,
--   COUNT_IF(geo_country IS NOT NULL) AS rowsWithGeoCountry,
--   COUNT_IF(geo_region IS NOT NULL) AS rowsWithGeoRegion,
--   COUNT_IF(geo_city IS NOT NULL) AS rowsWithGeoCity,
--   COUNT_IF(geo_dma IS NOT NULL) AS rowsWithGeoDma,
--   COUNT_IF(geo_zip IS NOT NULL) AS rowsWithGeoZip,
--   COUNT_IF(geo_latitude IS NOT NULL) AS rowsWithGeoLatitude,
--   COUNT_IF(geo_longitude IS NOT NULL) AS rowsWithGeoLongitude
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-09-28';

-- E. COMPOSITE-GRAIN DIAGNOSTIC
-- SELECT row_identity_hash,event_date,source_table,COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-09-28'
-- GROUP BY row_identity_hash,event_date,source_table
-- HAVING COUNT(*)>1
-- ORDER BY rowCount DESC LIMIT 100;
