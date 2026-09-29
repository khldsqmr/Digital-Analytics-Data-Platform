-- ============================================================================
-- FILE  : 01_sdi_sp_mip_bronze_edlHits_daily.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Persist a narrow raw UDI snapshot for a requested event-date window.
--
-- DESIGN:
--   - One top-level SQL statement per file.
--   - No required run/job ID during development.
--   - Validates inputs/source before creating or writing the Bronze target.
--   - p_validateOnly = TRUE performs preflight only; no table is created/written.
--   - Default as-of date is the previous Pacific calendar day.
-- ============================================================================

CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze UDI hits. Validates first, then creates the target if needed and replaces only the requested event-date window.'
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

    -- ------------------------------------------------------------------------
    -- 1. Parameter validation
    -- ------------------------------------------------------------------------
    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 2. Source preflight
    -- Nothing has been created or written to Bronze at this point.
    -- ------------------------------------------------------------------------
    IF NOT EXISTS (
        SELECT 1
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        LIMIT 1
    ) THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'UDI returned no rows for the requested Bronze window. Bronze was not created or refreshed.';
    END IF;

    -- ------------------------------------------------------------------------
    -- 3. Validation-only mode
    -- ------------------------------------------------------------------------
    IF p_validateOnly THEN

        SELECT
            'VALIDATION_ONLY' AS status,
            v_windowStart     AS requestedWindowStart,
            v_windowEnd       AS requestedWindowEnd,
            'prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions' AS sourceObject,
            'No Bronze table was created or modified.' AS message;

    ELSE

        -- --------------------------------------------------------------------
        -- 4. Create the persisted Bronze target only after preflight passes.
        --    CTAS + WHERE 1=0 inherits source data types without loading data.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
        USING DELTA
        CLUSTER BY (event_date, source_table)
        COMMENT 'Bronze: narrow raw copy of unified_digital_interactions. One row per EDL hit.'
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
        -- 5. Replace only the requested event-date window.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily
        REPLACE WHERE event_date BETWEEN v_windowStart AND v_windowEnd
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
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd;

        SELECT
            'SUCCESS'      AS status,
            v_windowStart  AS loadedWindowStart,
            v_windowEnd    AS loadedWindowEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlHits_daily' AS targetObject;

    END IF;
END;

-- Development examples (run separately after deploying the procedure):
--
-- Preflight only; creates/writes nothing:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );
--
-- Load one explicit completed day:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily(
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );
--
-- Load the default previous Pacific day:
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlHits_daily();
