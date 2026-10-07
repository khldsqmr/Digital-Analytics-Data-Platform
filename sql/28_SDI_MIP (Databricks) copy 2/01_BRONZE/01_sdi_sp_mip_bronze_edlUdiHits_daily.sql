-- ============================================================================
-- FILE  : 01_sdi_sp_mip_bronze_edlUdiHits_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
-- PURPOSE:
--   Persist a narrow raw UDI snapshot for a requested event-date window.
--
-- DESIGN:
--   - One top-level SQL statement per file.
--   - No required run/job ID during development.
--   - Validates inputs/source before creating or writing the Bronze target.
--   - p_validateOnly = TRUE performs preflight only; no table is created/written.
--   - Default as-of date is the previous Pacific calendar day.
--
-- SOURCE CONTRACT:
--   This file intentionally preserves the previously working UDI column contract.
--   Do not rename/remove proven identity/action fields unless Databricks itself
--   returns an unresolved-column error for the exact production source object.
--
-- DEVICE CONTRACT:
--   page_app_type       = logical property/surface.
--   page_layout_state   = Web responsive form factor.
--   attribute_os_name   = App OS.
--
-- CHANNEL CONTRACT:
--   channel_name        = MIP marketing/acquisition Channel. Preserve the exact
--                         source taxonomy as separate values (for example
--                         Paid Search: Brand, Paid Search: PLAs and
--                         Paid Search: Non-Brand). No Paid Search roll-up is
--                         applied in Bronze or Silver.
--   channel             = raw site/top-level navigation context.
--   attribute_channel   = app-side channel grouping/context.
--
-- TEMPORARY GEO CONTRACT:
--   Web geography       = geo_postal_code.
--   App geography       = attribute_country.
--   No ZIP/state/region mapping is applied in Bronze. Source values are kept
--   unchanged, including temporary upstream values such as METRO/RETAIL.
--
-- EVENT-DATE CONTRACT:
--   event_date is retained unchanged because it is part of the validated
--   UDI <-> SEF composite join key:
--     row_identity_hash + event_date + source_table
--
-- PREFLIGHT / VALIDATION CONTRACT:
--   p_validateOnly=TRUE performs all parameter/source-window checks and writes
--   nothing. For multi-day backfills every requested source event_date must be
--   present; a partially available date window fails before REPLACE WHERE runs.
--
-- PEER / IMPACT CONTRACT:
--   Bronze stores source facts only. Peer-set and impact-on-topline calculations
--   are intentionally not performed here; those are downstream analytical/App
--   Gold responsibilities.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    IN p_asOfDate        DATE    DEFAULT NULL,
    IN p_eventWindowDays INT     DEFAULT 1,
    IN p_validateOnly    BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze UDI hits. Preserves the proven source contract and adds current channel/geography context required by MIP.'
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
    SET v_sourceDateCount=(
        SELECT COUNT(DISTINCT event_date)
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
    );
    IF v_sourceDateCount<>p_eventWindowDays THEN
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
            'Proven UDI contract retained. Current additions: attribute_channel and attribute_country. No Bronze table was created or modified.' AS message;
    ELSE
        -- --------------------------------------------------------------------
        -- 4. Create target only after preflight passes.
        --    CTAS + WHERE 1=0 inherits source data types without loading data.
        -- --------------------------------------------------------------------
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        USING DELTA
        CLUSTER BY (event_date, source_table)
        COMMENT 'Bronze: narrow raw UDI copy required by MIP. One row per retained UDI source row.'
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
        -- 5. Replace only requested event-date window.
        -- --------------------------------------------------------------------
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
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
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd;
        SELECT
            'SUCCESS'      AS status,
            v_windowStart  AS loadedWindowStart,
            v_windowEnd    AS loadedWindowEnd,
            'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily' AS targetObject;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- Run separately after deploying the procedure.
-- ============================================================================
-- A. PREFLIGHT ONLY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--     p_asOfDate        => DATE '2026-10-02',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );
-- B. EXECUTE ONE COMPLETED DAY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--     p_asOfDate        => DATE '2026-10-02',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );
-- C. SOURCE-CONTRACT SANITY
-- SELECT
--     source_table,
--     COUNT(*) AS rows,
--     COUNT_IF(attribute_channel IS NOT NULL) AS rowsWithAttributeChannel,
--     COUNT_IF(geo_postal_code IS NOT NULL) AS rowsWithGeoPostalCode,
--     COUNT_IF(attribute_country IS NOT NULL) AS rowsWithAttributeCountry,
--     SUM(COALESCE(event_page_view,0)) AS pageViews,
--     SUM(COALESCE(event_purchase,0)) AS purchases,
--     SUM(COALESCE(event_click_to_call,0)) AS vrCallEvents,
--     SUM(COALESCE(event_chat_engage,0)) AS vrChatEvents,
--     SUM(COALESCE(event_store_search,0)) AS storeSearchEvents
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date = DATE '2026-10-02'
-- GROUP BY source_table
-- ORDER BY source_table;
-- D. COMPOSITE-GRAIN DIAGNOSTIC
-- SELECT
--     row_identity_hash,
--     event_date,
--     source_table,
--     COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date = DATE '2026-10-02'
-- GROUP BY row_identity_hash,event_date,source_table
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC
-- LIMIT 100;
