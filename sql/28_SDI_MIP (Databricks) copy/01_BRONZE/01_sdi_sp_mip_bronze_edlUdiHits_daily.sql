-- ============================================================================
-- FILE  : 01_sdi_sp_mip_bronze_edlUdiHits_daily.sql
-- LAYER : BRONZE
-- SOURCE: prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
-- PURPOSE:
--   Persist the MIP-required UDI projection for a requested event window.
--
-- DEVICE CONTRACT:
--   page_app_type     = logical property/surface.
--   page_layout_state = Web responsive form factor (desktop/mobile/tablet).
--   attribute_os_name = App OS (ios/android).
--
-- IDENTITY CONTRACT:
--   Live UDI identity fields retained:
--   customer_id, profile_uid, encrypted_ban, encrypted_msisdn,
--   fpid, ecid, visitor_id, attribute_device_id.
--   hit_id is retained as the hit-level source identifier.
--   resolved identity downstream excludes ecid per the manager-approved business
--   definition; ecid is retained only for diagnostics/future validation.
--
-- TEMPORARY GEO CONTRACT:
--   Web geography = geo_postal_code.
--   App geography = attribute_country.
--   No ZIP/state/region mapping is applied yet.
--   Downstream geoRegion is a temporary common field containing Web postal code
--   or App country so the existing Region pipeline remains intact.
--
-- EVENT-DATE CONTRACT:
--   Keep source event_date unchanged because it is part of the validated
--   UDI<->SEF composite join key and source partitioning contract. A tiny known
--   upstream event_date DQ sliver can differ from DATE(event_timestamp_pst);
--   do not rewrite event_date in Bronze because doing so can break session joins.
--
-- ACTION CONTRACT:
--   Raw dedicated VR call/chat/store-search event columns are not physical UDI
--   fields. Silver derives those flags from live event/action/page signals.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze MIP projection of EDL unified_digital_interactions using live-schema identity/device/geo fields; VR action flags are derived in Silver.'
AS
BEGIN
    DECLARE v_asOfDate DATE DEFAULT coalesce(p_asOfDate,date_add(to_date(from_utc_timestamp(current_timestamp(),'America/Los_Angeles')),-1));
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
            'Platform/device use page_app_type + page_layout_state + attribute_os_name. Temporary geography uses geo_postal_code for Web and attribute_country for App. No Bronze table was created or modified.' AS message;
    ELSE
        CREATE TABLE IF NOT EXISTS prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        USING DELTA
        CLUSTER BY (event_date,source_table)
        COMMENT 'Bronze: MIP projection of UDI. One row per retained UDI source row.'
        AS
        SELECT
            row_identity_hash,hit_id,event_date,source_table,event_timestamp_utc,event_timestamp_pst,
            customer_id,profile_uid,encrypted_ban,encrypted_msisdn,fpid,ecid,visitor_id,attribute_device_id,
            site_name,page_app_type,page_layout_state,attribute_os_name,
            page_language,browser_language,app_launch_type,app_launch_status,
            geo_postal_code,attribute_country,
            channel,attribute_channel,channel_name,
            site_sub_section,page_name,full_page_name,link_name,modal_name,
            page_flow_type,previous_page_name,navigation_intnav,navigation_menu,
            customer_type,user_type,user_auth_state,
            user_account_type,user_account_status,user_account_category,user_role,
            user_credit_class,credit_result,user_engagement_type,customer_indicator,
            carrier_name,attribute_network_device_carrier,attribute_network_connection_type,user_carrier_isp,
            flow_name,attribute_flow_name,external_campaign_code,
            shipping_method,payment_method_type,
            alert_message,page_url_path,page_url_full,
            order_id,product_order_type,order_status,trade_in_status,eip_status,
            cart_device_type,service_plan_tier,current_plan,new_plan,
            attribute_event_category,attribute_event_type,attribute_event_action,
            attribute_screen_name,webinteraction_type,link_type,
            event_page_view,event_purchase,event_cart_add,event_cart_checkout,
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE 1=0;
        INSERT INTO TABLE prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
        REPLACE WHERE event_date BETWEEN v_windowStart AND v_windowEnd
        SELECT
            row_identity_hash,hit_id,event_date,source_table,event_timestamp_utc,event_timestamp_pst,
            customer_id,profile_uid,encrypted_ban,encrypted_msisdn,fpid,ecid,visitor_id,attribute_device_id,
            site_name,page_app_type,page_layout_state,attribute_os_name,
            page_language,browser_language,app_launch_type,app_launch_status,
            geo_postal_code,attribute_country,
            channel,attribute_channel,channel_name,
            site_sub_section,page_name,full_page_name,link_name,modal_name,
            page_flow_type,previous_page_name,navigation_intnav,navigation_menu,
            customer_type,user_type,user_auth_state,
            user_account_type,user_account_status,user_account_category,user_role,
            user_credit_class,credit_result,user_engagement_type,customer_indicator,
            carrier_name,attribute_network_device_carrier,attribute_network_connection_type,user_carrier_isp,
            flow_name,attribute_flow_name,external_campaign_code,
            shipping_method,payment_method_type,
            alert_message,page_url_path,page_url_full,
            order_id,product_order_type,order_status,trade_in_status,eip_status,
            cart_device_type,service_plan_tier,current_plan,new_plan,
            attribute_event_category,attribute_event_type,attribute_event_action,
            attribute_screen_name,webinteraction_type,link_type,
            event_page_view,event_purchase,event_cart_add,event_cart_checkout,
            current_timestamp() AS _ingestedAt
        FROM prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions
        WHERE event_date BETWEEN v_windowStart AND v_windowEnd;
        SELECT 'SUCCESS' AS status,v_windowStart AS loadedWindowStart,v_windowEnd AS loadedWindowEnd,'prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily' AS targetObject;
    END IF;
END;
-- ============================================================================
-- DEVELOPMENT / TEST EXAMPLES
-- ============================================================================
-- A. PREFLIGHT
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--   p_asOfDate=>DATE '2026-10-02',p_eventWindowDays=>1,p_validateOnly=>TRUE);
-- B. EXECUTE
-- CALL prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
--   p_asOfDate=>DATE '2026-10-02',p_eventWindowDays=>1,p_validateOnly=>FALSE);
-- C. DEVICE / GEO SOURCE COVERAGE
-- SELECT
--   source_table,page_app_type,page_layout_state,attribute_os_name,
--   COUNT(*) AS rows,
--   COUNT_IF(nullif(trim(cast(geo_postal_code AS STRING)),'') IS NOT NULL) AS rowsWithGeoPostalCode,
--   ROUND(100.0*COUNT_IF(nullif(trim(cast(geo_postal_code AS STRING)),'') IS NOT NULL)/COUNT(*),2) AS geoPostalCodePct,
--   COUNT_IF(nullif(trim(cast(attribute_country AS STRING)),'') IS NOT NULL) AS rowsWithAttributeCountry,
--   ROUND(100.0*COUNT_IF(nullif(trim(cast(attribute_country AS STRING)),'') IS NOT NULL)/COUNT(*),2) AS attributeCountryPct
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-10-02'
-- GROUP BY source_table,page_app_type,page_layout_state,attribute_os_name
-- ORDER BY rows DESC;
-- D. TEMPORARY REGION-PLACEHOLDER COVERAGE
-- SELECT
--   source_table,
--   COUNT(*) AS rows,
--   COUNT_IF(CASE WHEN source_table='t_web_interactions' THEN nullif(trim(cast(geo_postal_code AS STRING)),'') WHEN source_table='t_app_interactions' THEN nullif(trim(cast(attribute_country AS STRING)),'') ELSE NULL END IS NOT NULL) AS rowsWithRegionPlaceholder,
--   ROUND(100.0*COUNT_IF(CASE WHEN source_table='t_web_interactions' THEN nullif(trim(cast(geo_postal_code AS STRING)),'') WHEN source_table='t_app_interactions' THEN nullif(trim(cast(attribute_country AS STRING)),'') ELSE NULL END IS NOT NULL)/COUNT(*),2) AS regionPlaceholderPct
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-10-02'
-- GROUP BY source_table
-- ORDER BY source_table;
-- E. ACTION-SIGNAL COVERAGE
-- SELECT
--   source_table,
--   COUNT(*) AS rows,
--   COUNT_IF(nullif(trim(cast(attribute_event_action AS STRING)),'') IS NOT NULL) AS rowsWithEventAction,
--   COUNT_IF(nullif(trim(cast(attribute_event_category AS STRING)),'') IS NOT NULL) AS rowsWithEventCategory,
--   COUNT_IF(lower(coalesce(attribute_event_action,'')) LIKE '%click to call%'
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%click-to-call%'
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%tap to call%'
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%call us%') AS approxVrCallRows,
--   COUNT_IF(lower(trim(coalesce(attribute_event_action,''))) IN (
--         'chat click','chat entry click','chat message engaged',
--         'live agent chat initiation','chat session ended','chat ended by user')
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%chat with customer care%click%') AS strictVrChatRows,
--   COUNT_IF(lower(coalesce(attribute_event_action,'')) LIKE '%store locator%'
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%find a store%'
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%find store%'
--         OR lower(coalesce(attribute_event_action,'')) LIKE '%store search%'
--         OR lower(coalesce(page_url_path,'')) LIKE '%/stores/%'
--         OR lower(coalesce(page_url_path,'')) LIKE '%/store-locator%'
--         OR lower(coalesce(page_name,'')) LIKE '%store locator%') AS approxStoreLocatorRows
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-10-02'
-- GROUP BY source_table
-- ORDER BY source_table;
-- F. COMPOSITE-GRAIN DIAGNOSTIC
-- SELECT row_identity_hash,event_date,source_table,COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-10-02'
-- GROUP BY row_identity_hash,event_date,source_table
-- HAVING COUNT(*)>1
-- ORDER BY rowCount DESC
-- LIMIT 100;



[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `fpid` cannot be resolved. Did you mean one of the following? [`ecid`, `hit_id`, `cart_id`, `kpi_lob`, `moe_cid`]. SQLSTATE: 42703; line 31, pos 67


Let me search the schema properly for any FPID-related column.# Found it — the column is `attribute_fpid`

Apologies for the back-and-forth. The actual first-party ID column in Silver is **`attribute_fpid`** (not `fpid` or `fpid_id`).

Confirmed from the schema:
- `attribute_fpid` — First-party ID (web browser cookie)
- `ecid` — Adobe Experience Cloud ID
- `visitor_id` — Adobe visitor ID (fallback)
- `attribute_device_id` — App installation device ID

## 🔧 Corrected identity block for your SELECT

```
row_identity_hash, hit_id, event_date, source_table,
event_timestamp_utc, event_timestamp_pst,
customer_id, profile_uid, encrypted_ban, encrypted_msisdn,
attribute_fpid, ecid, visitor_id, attribute_device_id,
```

## 💡 Canonical visitor key (if needed)

```
COALESCE(encrypted_msisdn, profile_uid, attribute_device_id, attribute_fpid) AS canonical_visitor_key
```

That should resolve cleanly against Databricks `prd_dbi_analytics.silver_digital_interactions.unified_digital_interactions`.

[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `fpid` cannot be resolved. Did you mean one of the following? [`ecid`, `hit_id`, `cart_id`, `kpi_lob`, `moe_cid`]. SQLSTATE: 42703; line 31, pos 67