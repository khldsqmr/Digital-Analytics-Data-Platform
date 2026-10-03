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
-- TEMPORARY GEO CONTRACT:
--   Web geography = geo_postal_code.
--   App geography = attribute_country.
--   No ZIP/state/region mapping is applied yet.
--   Downstream geoRegion is a temporary common field containing Web postal code
--   or App country so the existing Region pipeline remains intact.
-- ============================================================================
CREATE OR REPLACE PROCEDURE prdrzranalytics.lab42.sdi_sp_mip_bronze_edlUdiHits_daily(
    IN p_asOfDate DATE DEFAULT NULL,
    IN p_eventWindowDays INT DEFAULT 1,
    IN p_validateOnly BOOLEAN DEFAULT FALSE
)
LANGUAGE SQL
SQL SECURITY INVOKER
MODIFIES SQL DATA
COMMENT 'Bronze MIP projection of EDL unified_digital_interactions using validated device fields and temporary Web-postal/App-country geography.'
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
            row_identity_hash,event_date,source_table,event_timestamp_utc,event_timestamp_pst,
            customer_id,profile_uid,encrypted_ban_msisdn,first_party_id,app_instance_id,
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
            flow_name,attribute_flow_name,flow_type,external_campaign_code,
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
            geo_postal_code,attribute_country,
            channel,attribute_channel,channel_name,
            site_sub_section,page_name,full_page_name,link_name,modal_name,
            page_flow_type,previous_page_name,navigation_intnav,navigation_menu,
            customer_type,user_type,user_auth_state,
            user_account_type,user_account_status,user_account_category,user_role,
            user_credit_class,credit_result,user_engagement_type,customer_indicator,
            carrier_name,attribute_network_device_carrier,attribute_network_connection_type,user_carrier_isp,
            flow_name,attribute_flow_name,flow_type,external_campaign_code,
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
-- E. COMPOSITE-GRAIN DIAGNOSTIC
-- SELECT row_identity_hash,event_date,source_table,COUNT(*) AS rowCount
-- FROM prdrzranalytics.lab42.sdi_tbl_mip_bronze_edlUdiHits_daily
-- WHERE event_date=DATE '2026-10-02'
-- GROUP BY row_identity_hash,event_date,source_table
-- HAVING COUNT(*)>1
-- ORDER BY rowCount DESC
-- LIMIT 100;


[UNRESOLVED_COLUMN.WITH_SUGGESTION] A column, variable, or function parameter with name `flow_type` cannot be resolved. Did you mean one of the following? [`flow_name`, `link_type`, `imei_type`, `user_type`, `moe_cid`]. SQLSTATE: 42703; line 42, pos 42


I'll check FLOW_NAME coverage across UDI and SEF, split by channel, over the last 7 days.# `FLOW_NAME` Daily Coverage — Last 7 Days

## 📊 UDI (hit-level)

| Date | Channel | Total Rows | Rows w/ FLOW_NAME | % Fill |
|---|---|---:|---:|---:|
| 2026-10-02 | WEB | 37.6M | 3.9M | **10.3%** |
| 2026-10-02 | APP | 196.2M | 55.8M | **28.4%** |
| 2026-10-01 | WEB | 49.9M | 5.1M | **10.3%** |
| 2026-10-01 | APP | 261.4M | 73.4M | **28.1%** |
| 2026-09-30 | WEB | 48.3M | 5.0M | **10.4%** |
| 2026-09-30 | APP | 258.5M | 73.9M | **28.6%** |
| 2026-09-29 | WEB | 49.8M | 5.3M | **10.6%** |
| 2026-09-29 | APP | 474.8M | 115.5M | **24.3%** |
| 2026-09-28 | WEB | 53.1M | 6.8M | **12.9%** |
| 2026-09-28 | APP | 315.9M | 74.6M | **23.6%** |
| 2026-09-27 | WEB | 40.8M | 4.9M | **12.0%** |
| 2026-09-27 | APP | 223.9M | 64.4M | **28.8%** |
| 2026-09-26 | WEB | 39.5M | 4.3M | **11.0%** |
| 2026-09-26 | APP | 208.6M | 59.8M | **28.7%** |

**Averages:** Web ~**11%**, App ~**27%**

## 📊 SEF (session-event level)

| Date | Channel | Total Events | Events w/ FLOW_NAME | % Fill |
|---|---|---:|---:|---:|
| 2026-10-02 | WEB | 36.9M | 3.8M | **10.4%** |
| 2026-10-02 | APP | 196.0M | 55.8M | **28.5%** |
| 2026-10-01 | WEB | 49.2M | 5.1M | **10.4%** |
| 2026-10-01 | APP | 261.1M | 73.4M | **28.1%** |
| 2026-09-30 | WEB | 47.5M | 5.0M | **10.5%** |
| 2026-09-30 | APP | 258.2M | 73.9M | **28.6%** |
| 2026-09-29 | WEB | 48.8M | 5.3M | **10.8%** |
| 2026-09-29 | APP | 474.5M | 115.5M | **24.3%** |
| 2026-09-28 | WEB | 52.1M | 6.8M | **13.1%** |
| 2026-09-28 | APP | 315.6M | 74.6M | **23.6%** |
| 2026-09-27 | WEB | 40.0M | 4.9M | **12.2%** |
| 2026-09-27 | APP | 223.6M | 64.4M | **28.8%** |
| 2026-09-26 | WEB | 38.7M | 4.3M | **11.2%** |
| 2026-09-26 | APP | 208.3M | 59.8M | **28.7%** |

Numbers match UDI almost perfectly (SEF is a 1:1 projection of UDI with slightly fewer rows from session-boundary filtering). **Averages:** Web ~**11%**, App ~**27%**.

## 💡 Why fill is low (and that's expected)

`FLOW_NAME` only fires on **shopping/self-service flow pages** — cart, checkout, AAL funnel, upgrade funnel, accessories, OTP payments, etc. It's intentionally NULL for:
- Home page, Discover, Tuesdays, Benefits browsing
- Support/help pages, FAQ, chat
- Billing views that aren't payment flows
- Marketing/content pages
- Login/auth events

So you're seeing roughly **1 in 4 app hits** and **1 in 10 web hits** are inside a tagged commerce/self-service flow. That ratio is **correct behavior**, not a data gap.

## 🎯 Session-level will be much higher

A session only needs **one** hit with a `FLOW_NAME` to be tagged. Rough estimate: **40–55% of sessions** will have at least one flow tag, since most purposeful visits touch a flow page at some point.

Let me check both the Silver schema and the session tables for flow-related columns.# Flow Columns Across UDI, SEF, and SSF

## 📍 Where flow lives in each table

| Table | Flow columns | Notes |
|---|---|---|
| **UNIFIED_DIGITAL_INTERACTIONS** | `FLOW_NAME`, `ATTRIBUTE_FLOW_NAME` | Hit-level |
| **SESSION_EVENT_FACT** | `FLOW_NAME` | Hit-level (one per event) |
| **SESSION_SUMMARY_FACT** | ❌ none | No flow column — would need to aggregate from SEF |

## 🧾 Column definitions

### `FLOW_NAME` (UDI + SEF)
**Primary shopping/self-service flow bucket.** Governed, cross-channel. This is the canonical flow column.

**Common values:**
- `AAL Intent` — Add-A-Line
- `UPGRADE Intent` — device upgrade
- `ACTIVATION Intent` — new activation (prospect)
- `DEFERRED Intent` — deferred activation (prospect, pre-checkout)
- `ACCESSORIES Intent` — accessories-only cart
- `AAL Intent|UPGRADE Intent` — **mixed cart** (one concatenated value, not two rows)
- `PROSPECT`, `Port-In`, `Satellite Sign Up`
- `AuthPay`, `GuestPay`, `OTP` (One Time Payment)
- `benefits`, `checkout`, `my-wallet`

### `ATTRIBUTE_FLOW_NAME` (UDI only)
**App-side raw flow label** from the app analytics attribute payload. Mirrors `FLOW_NAME` for app rows, often NULL on web.

## ⚠️ What's **not** there

- **No `FLOW_TYPE` column** in any of the three tables. The dictionary mentions `FLOW_TYPE` conceptually but it's not a physical column — `FLOW_NAME` is what you query.
- **No flow column on `SESSION_SUMMARY_FACT`.** If you want "the dominant flow of a session," you have to derive it from SEF — e.g.:

```
-- Pattern: tag each session with its flows
SELECT
  session_id,
  LISTAGG(DISTINCT flow_name, '|') WITHIN GROUP (ORDER BY flow_name) AS session_flows,
  COUNT(DISTINCT flow_name) AS flow_count
FROM BDM_DBI_DB.SILVER_DIGITAL_INTERACTIONS.SESSION_EVENT_FACT
WHERE event_timestamp_pst >= DATEADD('day', -1, CURRENT_TIMESTAMP())
  AND flow_name IS NOT NULL
GROUP BY session_id
```

## 🎯 Rule of thumb

- **Filtering/analytics** → use `FLOW_NAME` (works in UDI and SEF, same values).
- **Session-level flow attribution** → aggregate SEF's `FLOW_NAME` up to `session_id`, then join to SSF.
- **Mixed-cart logic** → match the literal concatenated value `'AAL Intent|UPGRADE Intent'`, not Upgrade-only + AAL-only.

