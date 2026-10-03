-- ============================================================================
-- FILE  : 01_sdi_sp_mip_bronze_edlUdiHits_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
-- PURPOSE:
--   Persist the narrow MIP-required UDI hit projection for a requested event window.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze MIP projection of EDL unified_digital_interactions. Preserves all MIP-required UDI hits in the requested event-date window.'
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
            'No Bronze table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        USING DELTA
        CLUSTER BY (event_date,source_table)
        COMMENT 'Bronze: narrow MIP projection of EDL unified_digital_interactions. One row per retained UDI source row.'
        AS
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            event_timestamp_pst,
            customer_id,
            profile_uid,
            encrypted_ban_msisdn,
            first_party_id,
            app_instance_id,
            site_name,
            page_app_type,
            device_type,
            device_operating_system,
            -- IP-derived geography; retained raw in Bronze for future MIP use.
            geo_country,
            geo_region,
            geo_city,
            geo_dma,
            geo_zip,
            geo_latitude,
            geo_longitude,
            site_sub_section,
            page_name,
            full_page_name,
            link_name,
            modal_name,
            customer_type,
            user_auth_state,
            channel_name,
            external_campaign_code,
            user_carrier_isp,
            shipping_method,
            page_shipping_options,
            alert_message,
            page_url_path,
            page_url_full,
            order_id,
            product_order_type,
            event_page_view,
            event_purchase,
            event_click_to_call,
            event_chat_engage,
            event_store_search,
            event_cart_add,
            event_cart_checkout,
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE 1=0;
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        REPLACE WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            event_timestamp_pst,
            customer_id,
            profile_uid,
            encrypted_ban_msisdn,
            first_party_id,
            app_instance_id,
            site_name,
            page_app_type,
            device_type,
            device_operating_system,
            -- IP-derived geography; retained raw in Bronze for future MIP use.
            geo_country,
            geo_region,
            geo_city,
            geo_dma,
            geo_zip,
            geo_latitude,
            geo_longitude,
            site_sub_section,
            page_name,
            full_page_name,
            link_name,
            modal_name,
            customer_type,
            user_auth_state,
            channel_name,
            external_campaign_code,
            user_carrier_isp,
            shipping_method,
            page_shipping_options,
            alert_message,
            page_url_path,
            page_url_full,
            order_id,
            product_order_type,
            event_page_view,
            event_purchase,
            event_click_to_call,
            event_chat_engage,
            event_store_search,
            event_cart_add,
            event_cart_checkout,
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
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT ONLY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--   p_asOfDate=>DATE '2026-09-28', p_eventWindowDays=>1, p_validateOnly=>TRUE);
-- B. EXECUTE / REBUILD
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--   p_asOfDate=>DATE '2026-09-28', p_eventWindowDays=>1, p_validateOnly=>FALSE);
-- C. VALIDATION 1: GEO COVERAGE
-- SELECT
--   COUNT(*) AS rows,
--   COUNT_IF(geo_region IS NOT NULL AND trim(cast(geo_region AS STRING))<>'') AS rowsWithGeoRegion,
--   ROUND(100.0*COUNT_IF(geo_region IS NOT NULL AND trim(cast(geo_region AS STRING))<>'')/COUNT(*),2) AS geoRegionPct,
--   COUNT_IF(geo_country IS NOT NULL) AS rowsWithGeoCountry,
--   COUNT_IF(geo_city IS NOT NULL) AS rowsWithGeoCity,
--   COUNT_IF(geo_dma IS NOT NULL) AS rowsWithGeoDma,
--   COUNT_IF(geo_zip IS NOT NULL) AS rowsWithGeoZip
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-09-28';
-- D. VALIDATION 2: COMPOSITE-GRAIN DIAGNOSTIC
-- SELECT row_identity_hash,event_date,source_table,COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-09-28'
-- GROUP BY row_identity_hash,event_date,source_table
-- HAVING COUNT(*)>1
-- ORDER BY rowCount DESC LIMIT 100;


[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `device_type` cannot be resolved. Did you mean one of the following? [`device_info`, `imei_type`, `link_type`, `cart_device_type`, `device_status`]. SQLSTATE: 42703; line 45, pos 12

