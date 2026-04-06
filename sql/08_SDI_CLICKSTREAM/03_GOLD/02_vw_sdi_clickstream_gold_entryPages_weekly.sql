/* =================================================================================================
FILE: 02_vw_sdi_clickstream_gold_entryPages_weekly.sql
LAYER: Gold View
DATASET: prj-dbi-prd-1.ds_dbi_digitalmedia_automation
VIEW: vw_sdi_clickstream_gold_entryPages_weekly

SOURCE:
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_gold_entryPages_daily
  prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week


PURPOSE:
  Weekly Gold fact view for the Entry Pages tab using QGP week rollup, with TY and LY values,
  including:
    - raw channel
    - channel hierarchy
    - page grouping
    - tactic levels

BUSINESS GRAIN:
  qgp_week
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
  This supports:
  - channel filtering in the Entry Pages tab
  - entry page group by channel analysis
  - later comparison/bridging to Adobe channel-level data

QGP WEEK LOGIC:
  qgp_week =
    `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(session_day)

WEEKLY LY LOGIC:
  Weekly LY is calculated by summing the daily same-calendar-date LY values that are already attached
  to TY daily rows. This preserves the daily TY vs LY alignment before rolling into QGP week.

NOTES:
  - qgp_week is the final reporting period end date.
  - This is the final weekly output for the initial clickstream-only Entry Pages tab.
================================================================================================= */

CREATE OR REPLACE VIEW
`prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_gold_entryPages_weekly`
AS

SELECT
  `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.fn_qgp_week`(session_day) AS qgp_week,
  lob,
  entry_page_group,
  session_channel_name,
  TYPE,
  SUB_TYPE,
  MEDIA_TYPE,
  TACTIC_LEVEL_1,
  TACTIC_LEVEL_2,
  TACTIC_LEVEL_3,

  SUM(entry_sessions) AS entry_sessions,
  SUM(entry_sessions_LY) AS entry_sessions_LY,

  SUM(pspv_sessions) AS pspv_sessions,
  SUM(pspv_sessions_LY) AS pspv_sessions_LY,

  SUM(cart_start_sessions) AS cart_start_sessions,
  SUM(cart_start_sessions_LY) AS cart_start_sessions_LY,

  SUM(checkout_sessions) AS checkout_sessions,
  SUM(checkout_sessions_LY) AS checkout_sessions_LY,

  SUM(order_sessions) AS order_sessions,
  SUM(order_sessions_LY) AS order_sessions_LY

FROM `prj-dbi-prd-1.ds_dbi_digitalmedia_automation.vw_sdi_clickstream_gold_entryPages_daily`
GROUP BY
  qgp_week,
  lob,
  entry_page_group,
  session_channel_name,
  TYPE,
  SUB_TYPE,
  MEDIA_TYPE,
  TACTIC_LEVEL_1,
  TACTIC_LEVEL_2,
  TACTIC_LEVEL_3;