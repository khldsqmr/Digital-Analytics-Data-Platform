-- ============================================================================
-- FILE  : 01a_sdi_sp_mip_bronze_edlUdiHits_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
-- TARGET: prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- PURPOSE:
--   Persist a narrow raw UDI snapshot for a requested event-date window.
--
-- RUNTIME MODEL:
--   - This procedure remains the Bronze01 transformation engine.
--   - sdi_nb_mip_bronze_edlUdiHitsRunner_daily calls this procedure.
--   - This procedure keeps its own hard source-window preflight.
--   - sdi_nb_mip_bronze_edlUdiHitsValidator_daily performs only lightweight,
--     target-side post-load validation so normal validation does not rescan the
--     large UDI source.
--
-- DESIGN:
--   - One top-level SQL statement per file.
--   - No required run/job ID in the transformation procedure.
--   - Validates parameters/source availability before creating or writing Bronze.
--   - p_validateOnly = TRUE performs preflight only; no table is created/written.
--   - Default as-of date is the previous Pacific calendar day.
--
-- SOURCE CONTRACT:
--   Preserve the proven UDI column contract. Do not rename/remove identity,
--   action, channel or geography fields unless the production source contract
--   itself changes.
--
-- DEVICE CONTRACT:
--   page_app_type       = logical property/surface.
--   page_layout_state   = Web responsive form factor.
--   attribute_os_name   = App OS.
--
-- CHANNEL CONTRACT:
--   channel_name        = MIP marketing/acquisition Channel. Preserve exact
--                         source taxonomy; no Paid Search roll-up in Bronze.
--   channel             = raw site/top-level navigation context.
--   attribute_channel   = app-side channel grouping/context.
--
-- TEMPORARY GEO CONTRACT:
--   Web geography       = geo_postal_code.
--   App geography       = attribute_country.
--   No ZIP/state/region mapping is applied in Bronze.
--
-- EVENT-DATE CONTRACT:
--   event_date remains unchanged because it participates in the validated
--   UDI <-> SEF composite join key:
--     row_identity_hash + event_date + source_table
--
-- PREFLIGHT CONTRACT:
--   For multi-day windows, every requested source event_date must exist.
--   A partially available window fails before REPLACE WHERE executes.
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
    --    Nothing has been created or written to Bronze at this point.
    --    This is the source-side readiness check used in normal execution.
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
            'Source-window preflight passed. No Bronze table was created or modified.' AS message;
    ELSE
        -- --------------------------------------------------------------------
        -- 4. Create target only after source preflight passes.
        --    CTAS + WHERE 1=0 inherits proven source data types without loading.
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
        -- 5. Replace only the requested event-date scope.
        --
        --    IMPORTANT:
        --    Databricks REPLACE WHERE predicates can reference target-table
        --    attributes and literal values, but procedure-local variables are
        --    not resolved inside the replacement predicate itself.
        --
        --    Therefore the two already-validated DATE values are bound through
        --    named parameter markers in EXECUTE IMMEDIATE. This avoids fragile
        --    quote concatenation and preserves DATE typing in both predicates.
        --
        --    This preserves the atomic selective-overwrite behavior of
        --    REPLACE WHERE instead of splitting the operation into DELETE +
        --    INSERT.
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
            USING
                v_windowStart AS windowStart,
                v_windowEnd   AS windowEnd;

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
--     p_asOfDate        => DATE '2026-10-05',
--     p_eventWindowDays => 1,
--     p_validateOnly    => TRUE
-- );

-- B. EXECUTE ONE COMPLETED DAY
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--     p_asOfDate        => DATE '2026-10-05',
--     p_eventWindowDays => 1,
--     p_validateOnly    => FALSE
-- );

-- C. LIGHT TARGET SANITY
-- SELECT
--     event_date,
--     COUNT(*) AS rows,
--     MAX(_ingestedAt) AS latestIngestedAt
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date = DATE '2026-10-05'
-- GROUP BY event_date;

-- D. DEEP COMPOSITE-GRAIN DIAGNOSTIC
--    Run only when investigating a data-quality issue; not part of daily Validator.
-- SELECT
--     row_identity_hash,
--     event_date,
--     source_table,
--     COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date = DATE '2026-10-05'
-- GROUP BY row_identity_hash,event_date,source_table
-- HAVING COUNT(*) > 1
-- ORDER BY rowCount DESC
-- LIMIT 100;
