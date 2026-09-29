-- ============================================================================
-- FILE  : 01_sdi_tbl_mip_bronze_edlHits_daily.sql
-- LAYER : BRONZE
-- PURPOSE:
--   Narrow raw UDI snapshot for the requested event-date window.
-- ============================================================================

USE CATALOG prdrzranalytics;
USE SCHEMA lab42;

-- ----------------------------------------------------------------------------
-- UDI hits
-- ----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS sdi_tbl_mip_bronze_edlHits_daily
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

    CAST(NULL AS STRING) AS _runId,
    current_timestamp() AS _ingestedAt
FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
WHERE 1 = 0;

-- ----------------------------------------------------------------------------
-- Bronze load procedure
-- ----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE sdi_sp_mip_bronze_edlHits_daily(
    p_runId           STRING,
    p_asOfDate        DATE DEFAULT NULL,
    p_eventWindowDays INT  DEFAULT 1
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze UDI hits. Rewrites the requested recent event-date window so source repairs/deletes are reflected.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(
        p_asOfDate,
        to_date(from_utc_timestamp(current_timestamp(), 'America/Los_Angeles'))
    );
    DECLARE v_windowEnd DATE DEFAULT v_asOfDate;
    DECLARE v_windowStart DATE DEFAULT date_add(v_asOfDate, -(p_eventWindowDays - 1));
    DECLARE v_sourceRowCount BIGINT DEFAULT 0;

    IF p_eventWindowDays IS NULL OR p_eventWindowDays < 1 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'p_eventWindowDays must be >= 1';
    END IF;

    SET v_sourceRowCount = (
        SELECT count(*)
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd
    );

    IF v_sourceRowCount = 0 THEN
        SIGNAL SQLSTATE '45000'
            SET MESSAGE_TEXT = 'UDI returned no rows for requested Bronze window; target was not replaced.';
    END IF;

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

        p_runId AS _runId,
        current_timestamp() AS _ingestedAt
    FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
    WHERE event_date BETWEEN v_windowStart AND v_windowEnd;
END;

-- Example:
-- CALL sdi_sp_mip_bronze_edlHits_daily(
--     p_runId           => 'manual_20260929_001',
--     p_asOfDate        => DATE '2026-09-28',
--     p_eventWindowDays => 1
-- );
