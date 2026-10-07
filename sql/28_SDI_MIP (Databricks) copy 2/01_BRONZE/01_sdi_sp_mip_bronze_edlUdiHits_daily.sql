-- ============================================================================
-- FILE  : 01a_sdi_sp_mip_bronze_edlUdiHits_daily.sql
-- LAYER : BRONZE
-- OBJECT: B01
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
-- TARGET: prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
--
-- PURPOSE:
--   Persist the narrow raw UDI slice required by MIP for a requested event-date
--   window.
--
-- RUNTIME MODEL:
--   - Can be called directly with CALL.
--   - Can be called by 01b Runner.
--   - Performs its own hard source-window preflight.
--   - p_validateOnly=TRUE performs preflight only and writes nothing.
--
-- IMPORTANT WRITE DETAIL:
--   REPLACE WHERE cannot resolve procedure-local variables directly in the
--   replacement predicate. The write is therefore executed through
--   EXECUTE IMMEDIATE with typed named parameter markers.
--
-- DEFAULT DATE:
--   Previous Pacific calendar day when p_asOfDate is NULL.
--
-- GRAIN:
--   One row per retained UDI source row.
--
-- JOIN CONTRACT:
--   event_date remains unchanged because UDI <-> SEF uses:
--     row_identity_hash + event_date + source_table
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze01 UDI hits. Preserves source grain and performs hard source-window preflight before writing.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        date_add(
            to_date(
                from_utc_timestamp(
                    current_timestamp(),
                    'America/Los_Angeles'
                )
            ),
            -1
        )
    );

    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;

    DECLARE v_windowStart DATE DEFAULT date_add(
        v_asOfDate,
        -(p_eventWindowDays - 1)
    );

    DECLARE v_sourceDateCount BIGINT DEFAULT 0;
    DECLARE v_replaceSql STRING;

    -- ------------------------------------------------------------------------
    -- 1. Parameter validation
    -- ------------------------------------------------------------------------
    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 2. Hard source preflight
    --    Every requested event_date must exist before Bronze is touched.
    -- ------------------------------------------------------------------------
    SET v_sourceDateCount = (
        SELECT COUNT(DISTINCT event_date)
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
    );

    IF v_sourceDateCount <> p_eventWindowDays THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'UDI does not contain every requested event date. Bronze was not created or refreshed.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 3. Validation-only mode
    -- ------------------------------------------------------------------------
    IF p_validateOnly THEN
        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart     AS requestedWindowStart,
            v_windowEnd       AS requestedWindowEnd,
            v_sourceDateCount AS sourceDateCount,
            'prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions' AS sourceObject,
            'Source-window preflight passed. No Bronze table was created or modified.' AS message;

    ELSE
        -- --------------------------------------------------------------------
        -- 4. Create target only after source preflight passes.
        --    WHERE 1=0 inherits source types without loading source rows.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        USING DELTA
        CLUSTER BY (event_date, source_table)
        COMMENT 'Bronze01: narrow raw UDI copy required by MIP. One row per retained UDI source row.'
        AS
        SELECT
            row_identity_hash,
            event_date,
            source_table,
            event_timestamp_utc,
            event_timestamp_pst,
            load_datetime_pst,
            pipeline_batch_id,
            source_datalakeLoadDate,
            customer_id,
            profile_uid,
            encrypted_ban_msisdn,
            first_party_id,
            app_instance_id,
            attribute_device_id,
            app_session_id,
            site_name,
            page_app_type,
            page_domain,
            page_layout_state,
            page_language,
            channel,
            attribute_channel,
            site_sub_section,
            page_name,
            full_page_name,
            link_name,
            modal_name,
            flow_name,
            customer_type,
            user_auth_state,
            user_account_category,
            imei_type,
            channel_id,
            channel_name,
            external_campaign_code,
            kpi_lob,
            kpi_name,
            user_agent,
            demand_base_details,
            user_carrier_isp,
            attribute_event_type,
            attribute_os_name,
            geo_postal_code,
            attribute_country,
            shipping_method,
            page_shipping_options,
            alert_message,
            page_url_path,
            page_url_full,
            order_id,
            product_order_type,
            cart_is_hint_order,
            event_page_view,
            event_purchase,
            event_click_to_call,
            event_chat_engage,
            event_store_search,
            event_cart_add,
            event_cart_checkout,
            event_product_view,
            event_bopis_selected,
            event_quality_traffic,
            event_engaged_visit,
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE 1 = 0;

        -- --------------------------------------------------------------------
        -- 5. Atomic selective overwrite.
        --
        --    Named parameter markers are bound as DATE values by
        --    EXECUTE IMMEDIATE. The same markers are used in REPLACE WHERE and
        --    in the source filter, guaranteeing identical scope.
        -- --------------------------------------------------------------------
        SET v_replaceSql = '
            INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
            REPLACE WHERE event_date BETWEEN :windowStart AND :windowEnd
            SELECT
                row_identity_hash,
                event_date,
                source_table,
                event_timestamp_utc,
                event_timestamp_pst,
                load_datetime_pst,
                pipeline_batch_id,
                source_datalakeLoadDate,
                customer_id,
                profile_uid,
                encrypted_ban_msisdn,
                first_party_id,
                app_instance_id,
                attribute_device_id,
                app_session_id,
                site_name,
                page_app_type,
                page_domain,
                page_layout_state,
                page_language,
                channel,
                attribute_channel,
                site_sub_section,
                page_name,
                full_page_name,
                link_name,
                modal_name,
                flow_name,
                customer_type,
                user_auth_state,
                user_account_category,
                imei_type,
                channel_id,
                channel_name,
                external_campaign_code,
                kpi_lob,
                kpi_name,
                user_agent,
                demand_base_details,
                user_carrier_isp,
                attribute_event_type,
                attribute_os_name,
                geo_postal_code,
                attribute_country,
                shipping_method,
                page_shipping_options,
                alert_message,
                page_url_path,
                page_url_full,
                order_id,
                product_order_type,
                cart_is_hint_order,
                event_page_view,
                event_purchase,
                event_click_to_call,
                event_chat_engage,
                event_store_search,
                event_cart_add,
                event_cart_checkout,
                event_product_view,
                event_bopis_selected,
                event_quality_traffic,
                event_engaged_visit,
                current_timestamp() AS _ingestedAt
            FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
            WHERE event_date BETWEEN :windowStart AND :windowEnd
        ';

        EXECUTE IMMEDIATE v_replaceSql
            USING (
                v_windowStart AS windowStart,
                v_windowEnd   AS windowEnd
            );

        SELECT
            'SUCCESS'      AS status,
            v_windowStart  AS loadedWindowStart,
            v_windowEnd    AS loadedWindowEnd,
            v_sourceDateCount AS sourceDateCount,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily' AS targetObject;
    END IF;
END;

-- ============================================================================
-- MANUAL TESTS
-- Run separately after deploying the procedure.
-- ============================================================================

-- A. Preflight only:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--     p_asOfDate        => DATE '2026-10-05',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );

-- B. Actual one-day load:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--     p_asOfDate        => DATE '2026-10-05',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );

-- C. Lightweight target sanity:
-- SELECT
--     event_date,
--     COUNT(*) AS rowCount,
--     MAX(_ingestedAt) AS latestIngestedAt
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date = DATE '2026-10-05'
-- GROUP BY event_date;
