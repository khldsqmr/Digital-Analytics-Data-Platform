/* =================================================================================================
FILE: 01_vw_sdi_clickstream_gold_entryPages_daily.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_clickstream_gold_entryPages_daily

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_silver_entryPagesSession_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesMetrics_daily

PURPOSE:
  Canonical daily Gold fact view for the Entry Pages tab using clickstream-only metrics, with
  TY and LY values side by side, including:
    - raw channel
    - channel hierarchy
    - page grouping
    - tactic levels

BUSINESS GRAIN:
  session_day
  + lob
  + entry_page_group
  + session_channel_name
  + TYPE
  + SUB_TYPE
  + MEDIA_TYPE
  + TACTIC_LEVEL_1
  + TACTIC_LEVEL_2
  + TACTIC_LEVEL_3

WHY CHANNEL IS INCLUDED:
  Entry page behavior is often analyzed by traffic source/channel. Including session_channel_name
  in Gold gives the dashboard flexibility for:
  - filtering by channel
  - breaking entry page groups down by channel
  - later bridging more easily to Adobe/channel-level facts

WHY COUNT(*) IS USED:
  The Silver session view already contains one row per session_id + session_day.
  Therefore COUNT(*) is the correct and cheaper way to count entry sessions at this level.

LY LOGIC:
  For each current-day row, LY is pulled from the same:
    - lob
    - entry_page_group
    - session_channel_name
  on:
    DATE_SUB(session_day, INTERVAL 1 YEAR)

NOTES:
  - If no matching prior-year row exists, LY values default to 0.
  - This is same-calendar-date last year logic, not same-weekday or same-QGP-week logic.
  - Silver contains one row per session_id + session_day
  - Bronze metrics contains one row per session_id + session_day
  - Join is 1:1 on session_id + session_day + lob
  - All additional dimensions are deterministic attributes of the Silver row
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_gold_entryPages_daily`
AS

WITH daily_base AS (
  SELECT
    s.session_day,
    s.lob,
    s.entry_page_group,
    s.session_channel_name,
    s.TYPE,
    s.SUB_TYPE,
    s.MEDIA_TYPE,
    s.TACTIC_LEVEL_1,
    s.TACTIC_LEVEL_2,
    s.TACTIC_LEVEL_3,

    COUNT(*) AS entry_sessions,
    SUM(COALESCE(m.has_pspv, 0)) AS pspv_sessions,
    SUM(COALESCE(m.has_cart_start, 0)) AS cart_start_sessions,
    SUM(COALESCE(m.has_checkout, 0)) AS checkout_sessions,
    SUM(COALESCE(m.has_order, 0)) AS order_sessions

  FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_silver_entryPagesSession_daily` s
  LEFT JOIN `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.sdi_clickstream_bronze_entryPagesMetrics_daily` m
    ON s.session_id = m.session_id
   AND s.session_day = m.session_day
   AND s.lob = m.lob
  GROUP BY
    s.session_day,
    s.lob,
    s.entry_page_group,
    s.session_channel_name,
    s.TYPE,
    s.SUB_TYPE,
    s.MEDIA_TYPE,
    s.TACTIC_LEVEL_1,
    s.TACTIC_LEVEL_2,
    s.TACTIC_LEVEL_3
)

SELECT
  cur.session_day,
  cur.lob,
  cur.entry_page_group,
  cur.session_channel_name,
  cur.TYPE,
  cur.SUB_TYPE,
  cur.MEDIA_TYPE,
  cur.TACTIC_LEVEL_1,
  cur.TACTIC_LEVEL_2,
  cur.TACTIC_LEVEL_3,

  cur.entry_sessions,
  COALESCE(ly.entry_sessions, 0) AS entry_sessions_LY,

  cur.pspv_sessions,
  COALESCE(ly.pspv_sessions, 0) AS pspv_sessions_LY,

  cur.cart_start_sessions,
  COALESCE(ly.cart_start_sessions, 0) AS cart_start_sessions_LY,

  cur.checkout_sessions,
  COALESCE(ly.checkout_sessions, 0) AS checkout_sessions_LY,

  cur.order_sessions,
  COALESCE(ly.order_sessions, 0) AS order_sessions_LY

FROM daily_base cur
LEFT JOIN daily_base ly
  ON ly.session_day = DATE_SUB(cur.session_day, INTERVAL 1 YEAR)
 AND ly.lob = cur.lob
 AND ly.entry_page_group = cur.entry_page_group
 AND ly.session_channel_name = cur.session_channel_name
 AND ly.TYPE = cur.TYPE
 AND ly.SUB_TYPE = cur.SUB_TYPE
 AND ly.MEDIA_TYPE = cur.MEDIA_TYPE
 AND ly.TACTIC_LEVEL_1 = cur.TACTIC_LEVEL_1
 AND ly.TACTIC_LEVEL_2 = cur.TACTIC_LEVEL_2
 AND ly.TACTIC_LEVEL_3 = cur.TACTIC_LEVEL_3;