/* =================================================================================================
FILE:         01_sdi_vw_dashboardPulseTms_bronze_appendix_wide.sql
LAYER:        Dimension View - Bronze
VIEW NAME:    sdi_vw_dashboardPulseTms_bronze_appendix_wide

PURPOSE:
  Hand-curated wide appendix table, one row per apx_id, sourced from the team's "Pulse 1.2 -
  Metric Appendix" reference document. Feeds the dashboard's expandable metric-definition cards
  and, via the bridge table + annotated view built on top of this stack, gives an agentic
  Genie/SQL-querying LLM the "what does this metric mean and how was it built" context that
  gold_unified_long's fact rows don't carry on their own.

  This is a dimension/reference table, not a fact table -- it is deliberately NOT stacked into
  gold_unified_long (different grain: one row per metric/glossary definition, not one row per
  qgp_date x lob x channel_group x metric_name). See sdi_tbl_dashboardPulseTms_gold_appendixMetricBridge_weekly
  for how the two connect via a live join instead.

GRAIN:
  One row per apx_id. 54 rows total: 29 Metric, 25 Glossary.

CONTENT CONVENTION (read this before editing apx_build_detail_raw / apx_key_exclusions_raw):
  Every atomic fact -- each individual filter, each formula/calc definition, each exclusion
  clause -- is its own "[Label]\nDetail text" block, separated from the next by a blank line.
  Silver's unpivot (split on blank lines, regexp_extract the bracket as the label) turns each
  block into its own row, one label + one detail per row, matching the confirmed dashboard
  mockup pattern: a metric's "How Is It Built" panel should show one row per individual filter
  (Layer 1, Layer 2, Layer 3, Layer 4, ...), not one row per group of filters bundled together.
  Every block MUST have real detail text below its bracket line -- a bracket with nothing
  following it produces an empty detail cell downstream (this was a real bug found and fixed
  in this version, not a hypothetical one -- see CHANGE LOG).

  Universal Filters and the four standard prospect-qualifier filters (Layer 1: Exclude TMO
  Mobile Network Carrier, Layer 2: Exclude Authenticated/Network Authenticated, Layer 3:
  Exclude Visit Login Page, Layer 4: Exclude Single Page Visits) are repeated in full on every
  ADOBE-sourced metric row that uses them (via the with_prospect_layers() helper in the
  generator script), rather than abbreviated as "(same as UPV Actuals)" the way the original
  source spreadsheet did -- this keeps every card self-contained without needing to open a
  different metric's card to see what Layer 1-4 actually say. Metric-specific filters continue
  the numbering from Layer 5 onward.

KEY COLUMNS:
  apx_id                    - stable slug, matches gold_unified_long's camelCase metric_name
                              wherever a 1:1 relationship exists (see the bridge table for the
                              cases that need more than one gold_unified_long row per apx_id)
  apx_record_type           - 'Metric' | 'Glossary'
  apx_funnel_stage          - 'Top Funnel' | 'Mid Funnel' | 'Bottom Funnel' | NULL (Glossary)
  apx_is_subflow            - TRUE for rows marked (dagger) in the source doc
  apx_parent_id             - apx_id of the row this sums to, if any
  apx_summable_to_parent    - TRUE if this row's value actually adds up to its parent (cart and
                              orders flows do; UPV flows do NOT, despite also being marked as
                              subflows -- Adobe deduplicates unique visitors at metric scope, so
                              upvPostpaid + upvHsi + upvByod overcounts upvTotalAdobe)
  apx_referenced_glossary_ids - explicit array of glossary apx_ids this Metric row's build logic
                              depends on, so an LLM crawling this table gets a structured edge
                              instead of having to re-derive the connection from prose

CHANGE LOG:
  - Ported from the team's "Pulse 1.2 - Metric Appendix" spreadsheet (Khalid, this session).
  - Added apx_id 'ordersTotal' / "Orders (Overall)" as a new top-level row, parent of both
    ordersUnassistedTotal and ordersAssistedTotal, mirroring how cartstartTotal already parents
    the 3 Cart flows. Corresponds to metric_name = 'ordersTotal', already computed in
    gold_unified_long's ADOBE CTE as ordersUnassistedTotal + ordersAssistedTotal -- no new
    upstream SQL needed, this just gives it an appendix card.
  - Renamed the 3 Orders Unassisted sub-flow apx_metric_name values to say "Unassisted"
    explicitly ('Orders - Postpaid Flow' -> 'Orders Unassisted - Postpaid Flow', etc.), matching
    the parallel Assisted rows which already said "Assisted".
  - Added apx_warning_message -- populated for the 11 rows whose source text already carried an
    IMPORTANT/Note-style caveat, NULL everywhere else.
  - REWRITTEN this version: every apx_build_detail_raw and apx_key_exclusions_raw across all 29
    Metric rows, atomized to one bracketed block per individual filter/formula/exclusion clause,
    against a real dashboard mockup screenshot Khalid provided. Previously, multiple filters were
    bundled under one "[Layer N]" bracket (e.g. all four prospect qualifiers under one
    "[Layer 2 - Prospect qualifiers]" block), which produced one crowded row instead of four
    clean ones once unpivoted -- confirmed by actually running Silver's split/regexp logic
    against the prior content and inspecting the output rows. Also fixed one dead block whose
    bracket had no detail text following it (a "[No flow filter - ...]" line that was pure
    description with nothing left over once the bracket was stripped) -- now
    "[No Flow Filter]" / "Broadest universe across all product areas." like every other block.
  - eligibilityChecksCompleted intentionally carries no apx_data_source_label pointer beyond
    "PENDING" -- this metric has no live data_source/metric_name in gold_unified_long yet.
  - Refresh-cadence text for the 16 Adobe-sourced rows is carried through as "Daily at 9 AM PT"
    from the source doc, which is QGP's cadence -- a dashboard screenshot Khalid provided shows
    "Daily at 7 AM PST" for upvTotalAdobe specifically, suggesting this needs correcting before
    it ships; flagged inline on each affected row rather than silently changed, since the correct
    real-world cadence hasn't been confirmed yet.
================================================================================================= */

CREATE OR REPLACE VIEW
  prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_bronze_appendix_wide
AS
SELECT * FROM (
  SELECT 'mfcSpendPostpaid', 'Metric', 'Top Funnel', 'Media Spend', 'MFC Spend - Postpaid', 'Postpaid LOB', FALSE, NULL, NULL, 'Media Flow Chart (MFC)', 'prdrzranalytics.lab42.raw_media_flowchart', 'Amy Pritchett / Will Butler', 'Total dollars invested in Postpaid media each week, as tracked and finalized by LOB teams in the Media Flow Chart.', NULL, '[Source]
Media Flow Chart (MFC) - T-Mobile''s centralized media spend source of truth, stored in Databricks.

[Contribution Cadence]
LOB teams contribute Postpaid spend data each Thursday. QA and finalization by 3 PM PT each Friday.

[Mid-Week Corrections]
Will Butler''s team performs a twice-weekly update for mid-week corrections.

[Aggregation]
Aggregates weekly spend across all buying channels and platforms at the Postpaid LOB level.

[MFC Tactic Filter]
Global selector affecting all spend rows.

[MFC Channel Filter]
Applies to MFC Spend only - does not affect any other metric.

[Forecasted Spend$]
Planned/budgeted amount from MFC.

[Act vs Fcst %]
(Actuals - Forecast) / Forecast.

[QTD Sum]
Running total of weekly actuals from Q2 start.', '[Message Type Exclusion]
Excludes Message_Type = MICRO.

[Message Value Exclusion]
Excludes Message values ''SEM Postpaid/Micro'' and ''Micro Postpaid Offers'' until channel mapping is finalized.', 'Weekly (Thursday contribution, Friday 3 PM PT finalization, twice-weekly mid-week updates)', NULL, NULL, array('mfc', 'tfb'), 1
 UNION ALL
  SELECT 'mfcSpendBroadband', 'Metric', 'Top Funnel', 'Media Spend', 'MFC Spend - Broadband', 'Broadband / HSI LOB', FALSE, NULL, NULL, 'Media Flow Chart (MFC)', 'prdrzranalytics.lab42.raw_media_flowchart', 'Amy Pritchett / Will Butler', 'Total dollars invested in Broadband (Home Internet) media each week, as tracked and finalized in the Media Flow Chart.', NULL, '[Source]
Same source and build logic as MFC Spend - Postpaid, filtered to the Broadband LOB.

[Contribution Cadence]
Same LOB contribution, QA, and refresh cadence as Postpaid spend.

[Aggregation]
Aggregates weekly spend across all channels for the Broadband / Home Internet (HSI) line of business.

[Forecasted Spend$]
Planned/budgeted Broadband amount.

[Act vs Fcst %]
(Actuals - Forecast) / Forecast.', '[Message Type Exclusion]
Same MICRO message type exclusions as Postpaid MFC Spend.', 'Weekly (Thursday contribution, Friday 3 PM PT finalization, twice-weekly mid-week updates)', NULL, NULL, array('mfc', 'hsi'), 2
 UNION ALL
  SELECT 'upvTotalAdobe', 'Metric', 'Top Funnel', 'UPV', 'UPV Actuals (Overall)', 'All Flows Combined', FALSE, NULL, NULL, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Count of distinct website visitors who landed on T-Mobile.com and engaged beyond their first page - the broadest measure of qualified prospect traffic entering the funnel.', 'Flows (Postpaid, HSI, BYOD) do NOT sum to this total. Adobe deduplicates unique visitors at the scope of the metric, so summing the three flow columns re-introduces visitors counted in more than one flow.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[No Flow Filter]
Broadest universe across all product areas.

[WoW %]
(Current week - Prior week) / Prior week.

[Avg of QTD]
Running average of weekly actuals from Q2 start.

[Refresh]
Weekly, each time the UPV forecast notebook is run.', '[T-Mobile Network Visitors]
Visitors on the T-Mobile cellular network.

[Authenticated/LOA Sessions]
All sessions where User State = Authenticated or any LOA token present.

[Login & Guest-Pay Pages]
All visits touching sign-in or guest-pay pages.

[Single-Page Bounces]
Sessions with only one page view.

[Native App Traffic]
iOS/Android T-Life, MyT-Mobile, Metro app sessions.

[TFB Pages]
b2b, Atwork, /business, /t-priority URL paths.', 'Weekly, each time the UPV forecast notebook is run', NULL, NULL, array('prospect', 'nonBounced', 'uniqueVisitors', 'visit', 'hit', 'tfb', 'loa'), 3
 UNION ALL
  SELECT 'upvForecast', 'Metric', 'Top Funnel', 'UPV', 'UPV Forecasts', 'All Flows', FALSE, NULL, NULL, 'Silver procedure: sdi_sp_dashboardPulseTms_silver_upvForecast_weekly', 'prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_silver_upvForecast_weekly', 'Khalid / Ben', 'A quarterly-uploaded weekly estimate of expected UPV, used to benchmark whether actual traffic is tracking ahead or behind plan.', NULL, '[Bronze Upload]
Populated via a manual CSV upload through a Databricks notebook (upvForecast_bronze_upload.py), ad hoc, no fixed schedule, until the forecasting team has a proper feed.

[Channel Allocation]
All Channels receives the full Bronze value (allocation_ratio = 1.0), passthrough. Every other channel_group is NULL (allocation_ratio = NULL) until per-channel forecasts are available.

[Act vs Fcst %]
(Actuals - Forecast) / Forecast.

[Act vs Fcst (Absolute)]
Actuals minus Forecast in raw visitor count.', '[Channel Allocation Status]
Channel-level allocation is not yet live - only the All Channels rollup carries a value today.', 'Manual / ad hoc - after each quarterly CSV upload', NULL, NULL, array('mfc'), 4
 UNION ALL
  SELECT 'upvPostpaid', 'Metric', 'Top Funnel', 'UPV', 'UPV - Postpaid Flow', 'Postpaid', TRUE, 'upvTotalAdobe', FALSE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Distinct non-bounced visitors who browsed the core Postpaid phone/plan shopping experience, with HSI pages excluded.', 'Cannot be summed with the HSI and BYOD flows to equal UPV Actuals (Overall) - see that row''s warning for why.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Flow Filter]
[sdi] Postpaid Flow (Exclude HSI + BYOD). HIT-level exclusion, removes any hit matching HSI identifiers: Page Name contains ''TLife App | Shop : HINT'', ''TMO | HINT'', or ''TMO:UNO | HINT''; Page URL contains www.t-mobile.com/isp, es.t-mobile.com/home-internet, www.t-mobile.com/home-internet, or www.t-mobile.com/stores/bd/home-internet; Site Name = TMO AND Site Section (v1) = HINT; Page Name contains ''TLife App | Shop'' AND Product Type (v100) = ISP; Page Name contains ''TMO | Shop'' AND Product Type (v100) = ISP. Remaining hits scope the visit to the Postpaid phone/plan experience.

[BYOD Note]
BYOD SIM pages are not excluded at the UPV level - they are excluded at cart and orders level only.

[CVR Denominator]
Used as denominator for Cart Postpaid CVR% and Orders Postpaid CVR%.', '[HSI Page Hits]
Excludes all HSI page hits (HINT page names, /isp/ and /home-internet/ URLs, Site Section = HINT, Product Type = ISP).

[Prospect Filters]
All prospect qualifier filters (Layers 1-4) also applied.', 'Weekly, each time the UPV forecast notebook is run', NULL, NULL, array('prospect', 'nonBounced', 'hsi', 'productType'), 5
 UNION ALL
  SELECT 'upvHsi', 'Metric', 'Top Funnel', 'UPV', 'UPV - HSI Flow', 'Home Internet (HSI)', TRUE, 'upvTotalAdobe', FALSE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Distinct non-bounced visitors who browsed T-Mobile Home Internet pages - the prospect audience for the internet service offering.', 'Cannot be summed with the Postpaid and BYOD flows to equal UPV Actuals (Overall) - see that row''s warning for why.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Flow Filter]
[DA] HSI Any HSI Site Page, Hits. HIT-level inclusion, includes visits where any hit matched HSI page identifiers: Page Name contains ''TLife App | Shop : HINT'', ''TMO | HINT'', or ''TMO:UNO | HINT''; Page URL contains www.t-mobile.com/isp, es.t-mobile.com/home-internet, www.t-mobile.com/home-internet, or www.t-mobile.com/stores/bd/home-internet; Site Name = TMO AND Site Section (v1) = HINT; Page Name contains ''TLife App | Shop'' AND Product Type (v100) = ISP; Page Name contains ''TMO | Shop'' AND Product Type (v100) = ISP.

[CVR Denominator]
Used as denominator for Cart HSI CVR% and Orders HSI CVR%.', '[HSI Page Hit Scope]
Scoped to visits containing any HSI page hit.

[Prospect Filters]
All prospect filters (Layers 1-4) applied.

[Overlap Note]
Not mutually exclusive from Postpaid or BYOD at visit level.', 'Weekly, each time the UPV forecast notebook is run', NULL, NULL, array('prospect', 'nonBounced', 'hsi', 'productType'), 6
 UNION ALL
  SELECT 'upvByod', 'Metric', 'Top Funnel', 'UPV', 'UPV - BYOD Flow', 'BYOD', TRUE, 'upvTotalAdobe', FALSE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Distinct non-bounced visitors who browsed BYOD or device-switching pages - prospects exploring how to bring an existing device to T-Mobile.', 'Cannot be summed with the Postpaid and HSI flows to equal UPV Actuals (Overall) - see that row''s warning for why.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Flow Filter]
BYOD Pages, Visits. Includes visits where any page name or hit matched BYOD identifiers: ''TMO | Shop : Browse : Bring Your Own Phone''; ''TMO | Support : Resources : Switch from Current Carrier''; ''TMO | Shop : Browse : Bring your own phone eligibility checker''; page names containing ''TMO | Shop : switch :''; ''TMO | Marketing : Landing Page : Quick Prospect Switching''; ''TMO | Shop : Cell Phone Detail : T-Mobile SIM Card'' (HITS); ''TLife App | Shop : Cell Phone Detail : T-Mobile SIM Card'' (HITS); ''TLife App | Shop : Tablet Detail : Mobile Internet SIM Card'' (HITS); ''TMO | Shop : Tablet Detail : T-Mobile Mobile Internet SIM Card'' (HITS).

[CVR Denominator]
Used as denominator for Cart BYOD CVR% and Orders BYOD CVR%.', '[BYOD/SIM Page Scope]
Scoped to visits containing any BYOD or SIM card page hit.

[Prospect Filters]
All prospect filters (Layers 1-4) applied.

[Overlap Note]
Not mutually exclusive from other flows at visit level.', 'Weekly, each time the UPV forecast notebook is run', NULL, NULL, array('prospect', 'nonBounced', 'byod'), 7
 UNION ALL
  SELECT 'vrCalls', 'Metric', 'Mid Funnel', 'Actions', 'VR Calls', 'All / Postpaid', FALSE, NULL, NULL, 'Quarterly Game Plan (QGP)', 'prdrzrlakehouse.qgp_restricted.qgpweeklyview', 'Preeti Laharwani / Bharat Kavuru', 'Phone calls placed by customers clicking a T-Mobile.com phone number to get live sales help - a direct signal of high purchase intent.', NULL, '[Source]
QGP operational data - entirely separate from Adobe Analytics. No Adobe segment filtering applies.

[Counting Logic]
Counts customer calls handled by T-Mobile''s Virtual Retail (VR) sales teams.

[Trigger]
Triggered when a customer clicks a phone number on T-Mobile.com to get assistance purchasing a device or plan.

[Forecast Availability]
QGP provides both weekly actuals and a weekly forecast (VR Calls QGP).

[Act vs Fcst %]
(Actuals - QGP Forecast) / QGP Forecast.

[QTD Sum]
Running total from Q2 start.

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
No Adobe segment filtering. QGP operational data only.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('qgp', 'vr'), 8
 UNION ALL
  SELECT 'vrChats', 'Metric', 'Mid Funnel', 'Actions', 'VR Chats', 'All / Postpaid', FALSE, NULL, NULL, 'Quarterly Game Plan (QGP)', 'prdrzrlakehouse.qgp_restricted.qgpweeklyview', 'Preeti Laharwani / Bharat Kavuru', 'Chat conversations initiated by customers on T-Mobile.com with Virtual Retail sales agents - indicating purchase-ready engagement.', NULL, '[Source]
QGP operational data - no Adobe segment filtering.

[Counting Logic]
Counts customer chat conversations handled by T-Mobile''s VR sales teams.

[Trigger]
Triggered when a customer clicks to chat on T-Mobile.com to get help purchasing a device or plan.

[Forecast Availability]
QGP provides both actuals and a weekly forecast.

[Act vs Fcst %]
(Actuals - QGP Forecast) / QGP Forecast.

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
No Adobe segment filtering. QGP operational data only.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('qgp', 'vr'), 9
 UNION ALL
  SELECT 'storeTraffic', 'Metric', 'Mid Funnel', 'Actions', 'Exit Traffic (Door Swings)', 'Retail', FALSE, NULL, NULL, 'Quarterly Game Plan (QGP)', 'prdrzrlakehouse.qgp_restricted.qgpweeklyview', 'Preeti Laharwani / Bharat Kavuru', 'Total customer entries/exits at T-Mobile retail stores each week, measured by door-swing sensors - a proxy for offline foot traffic driven by digital awareness.', NULL, '[Source]
QGP operational data sourced from retail door-swing sensor hardware - no Adobe segment filtering.

[Counting Logic]
Counts physical customer exits from T-Mobile retail store locations.

[Data Source Detail]
Data is sourced from door-swing sensor readings at store entrances/exits, reported through QGP.

[Forecast Availability]
QGP provides both actuals and a weekly forecast.

[Act vs Fcst %]
(Actuals - QGP Forecast) / QGP Forecast.

[Refresh]
Daily at 9 AM PT.', '[Sensor Scope]
Retail sensor data - no Adobe filtering.

[Outcome Scope]
Counts all store visits regardless of purchase outcome.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('qgp'), 10
 UNION ALL
  SELECT 'eligibilityChecksCompleted', 'Metric', 'Mid Funnel', 'Actions', 'Eligibility Checks Completed', 'Home Internet (HSI)', FALSE, NULL, NULL, 'Adobe Analytics -> Databricks (PENDING - not yet in the pipeline)', NULL, 'khalid.qamar1 (pipeline)', 'Prospects who ran a T-Mobile Home Internet availability check and got a result back - the first hard signal of HSI purchase intent, ahead of any cart activity.', 'PENDING - not yet live in gold_unified_long. This card documents the intended design only; no data currently backs it.', '[Status]
PENDING - this metric is not yet live anywhere in gold_unified_long. No data_source/metric_name exists for it today.

Metric definition (as designed): Eligibility Checks Completed, counted as occurrences (hits) in Adobe Analytics.

[Universal + Prospect Filters (as designed)]
Same as UPV Actuals (Overall) -- Layers 1-4.

[Flow Filter (as designed)]
[DA] HSI Eligibility Check Completed. HIT-level inclusion, includes HITS where BOTH are true: Tool Name (v11) = ''HINT Availability Search'' OR Tool Name (v11) = ''#''; AND Tool Result (v96) contains ''ISP:TMO|Eligibility:''.', '[Filter Scope]
All Layer 1-4 prospect filters applied (as designed).

[Counting Note]
Counted as occurrences - a visit running several availability checks contributes several, so this does not compare like-for-like with the visit-level UPV and cart metrics.

[Distinction]
NOT the QGP ''Eligibility Check'' metric.', 'PENDING - not yet built', NULL, NULL, array('hsi', 'prospect'), 11
 UNION ALL
  SELECT 'cartstartTotal', 'Metric', 'Mid Funnel', 'Actions', 'Add to Cart (Overall)', 'Postpaid + HSI + BYOD', FALSE, NULL, NULL, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Total visits in which a prospect added any T-Mobile product to cart - the strongest digital signal of purchase intent before checkout.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Derived Total]
Add to Cart = Cart Postpaid Flow + Cart HSI Flow + Cart BYOD Flow. Cart flows CAN be summed - each uses mutually exclusive product-type and page-scope filtering.

[Base Metric]
Count of VISITS where a prospect performs a cart start action (scAdd or scOpen) on a recognized cart page.

[Layer 5 - Cart Start Filter]
[sdi] cart start. Requires a hit where scOpen OR scAdd exists AND page is a recognized cart page: www.t-mobile.com/shopping-cart | www.t-mobile.com/cart | es.t-mobile.com/cart | es.t-mobile.com/shopping-cart; Page Name = ''TMO:UNO | Shop : Cart'' | ''TMO | Shop : Cart'' | ''TLife App | Shop : Cart : Active Cart'' | ''TMO | Shop : Cart : Active Cart''; Page Name = ''TMO | Shop : Switch : Checkout Review'' AND Cart Views (scView) exists AND Product Name (v35) = ''BYOS SIM or eSIM''.

[Flow-Specific Filters]
Applied differently per flow - see Cart flow rows (Postpaid/HSI/BYOD).

[CVR %]
Add to Cart / UPV Actuals.', '[Prospect Filters]
All prospect filters applied.

[Flow Exclusions]
Flow-specific exclusions detailed in each sub-flow row.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('prospect', 'scOpenScAdd', 'cvr'), 12
 UNION ALL
  SELECT 'cartstartPostpaid', 'Metric', 'Mid Funnel', 'Actions', 'Cart - Postpaid Flow', 'Postpaid', TRUE, 'cartstartTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Visits in which a prospect started or added to a cart on the Postpaid phone/plan path, with all HSI and BYOD cart activity removed.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Cart Start Filter]
[sdi] cart start (same as Add to Cart Overall).

[Layer 6 - HSI Cart Exclusions]
[sdi] Exclude HSI OR BYOD Cart Start. HSI portion: Cart pages where Product Type (v100) = ISP; Flow Name (v154) = ''AAL Intent'' (HSI checkout initiation); URLs containing .t-mobile.com/home-internet/eligibility/base or /commerce/checkout/hint-order.

[Layer 7 - BYOD Cart Exclusions]
Cart pages where Product Type (v100) = SIMCARDS; ''TMO | Shop : Switch : Checkout Review'' AND Cart Views (scView) exists AND Product Name (v35) = ''BYOS SIM or eSIM''; Flow Name (v154) starts with ''DEFERRED Intent'', ''Prospect Activation'', or ''Activation Intent''.

[Layer 8 - Existing Customer Scrub]
Additional scrub beyond Layer 2: Mobile Carrier Network = T-Mobile | Encrypted MSISDN (v121) exists; Tracking Code (v0) contains ''_TMT_'', ''_C_'', or ''_CUST_''; Successful Logins (e4) exists | Page contains web2go | Site Section = ma; Page URL (c3) contains my.t-mobile.com (excluding login page); Site Name contains Sprint | Customer Indicators (v155) contains m:1 or b:1.

[CVR %]
Cart Postpaid Flow / UPV Postpaid Flow.

[Summability]
Sums with Cart HSI + Cart BYOD = Add to Cart total.', '[HSI/BYOD Removal]
Removes HSI and BYOD cart actions.

[Existing Customer Scrub]
Additional existing-customer signal scrub beyond standard prospect filters (MSISDN, tracking codes, Sprint site, Customer Indicators).', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('flowName', 'productType', 'scOpenScAdd', 'cvr'), 13
 UNION ALL
  SELECT 'cartstartHsi', 'Metric', 'Mid Funnel', 'Actions', 'Cart - HSI Flow', 'Home Internet (HSI)', TRUE, 'cartstartTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Visits in which a prospect engaged with the Home Internet cart - indicating intent to purchase T-Mobile''s internet service.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Flow Filter]
[DA] HSI Prospect Cart Visits. Includes HITS where ALL three conditions are true: 1. Cart page is an HSI cart identifier (Page URL (v6) contains .t-mobile.com/buy/cart or .t-mobile.com/cart; Page Name contains ''TMO | HINT : Shop : Active Cart'' or ''TMO | Shop : Cart : Active Cart''). 2. Product Type (v100) = ISP. 3. Visit includes an active HSI prospect flow signal (any of: Flow Name (v154) = ''AAL Intent''; Page URL contains .t-mobile.com/home-internet/eligibility/base; Page URL contains .t-mobile.com/commerce/checkout/hint-order).

[CVR %]
Cart HSI Flow / UPV HSI Flow.

[Summability]
Part of Add to Cart total.', '[Scope Requirement]
Requires ISP product type AND active HSI prospect flow signal.

[Fire Condition]
Will not fire for general HSI browsing without checkout intent.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('hsi', 'flowName', 'productType', 'cvr'), 14
 UNION ALL
  SELECT 'cartstartByod', 'Metric', 'Mid Funnel', 'Actions', 'Cart - BYOD Flow', 'BYOD', TRUE, 'cartstartTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Visits in which a prospect started a cart on the BYOD/SIM path - bringing an existing device to T-Mobile.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Flow Filter]
[sdi] postpaid prospect byod cart start hit. Includes HITS where ALL three conditions are true: 1. Cart start event: scOpen OR scAdd on a recognized cart page. 2. Product scope, either: Product Type (v100) = SIMCARDS; or Page Name = ''TMO | Shop : Switch : Checkout Review'' AND Cart Views (scView) exists AND Product Name (v35) = ''BYOS SIM or eSIM''. 3. Flow Name (v154) = active prospect activation intent (any of: starts with ''DEFERRED Intent''; = ''Prospect Activation Intent''; = ''Intencion de activacion de prospecto Intent''; = ''Activacion de prospecto Intent''; starts with ''Prospect Activation''; starts with ''Activation Intent'').

[CVR %]
Cart BYOD Flow / UPV BYOD Flow.

[Summability]
Part of Add to Cart total.', '[Product Scope]
Scoped to SIM card product type with active prospect activation flow.

[Exclusion Scope]
Excludes Postpaid device and HSI cart actions.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('byod', 'flowName', 'productType', 'scOpenScAdd', 'cvr'), 15
 UNION ALL
  SELECT 'ordersUnassistedTotal', 'Metric', 'Bottom Funnel', 'Orders (Unassisted)', 'Orders (Unassisted)', 'Postpaid + HSI + BYOD', TRUE, 'ordersTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Total orders completed entirely online with no T-Mobile employee involvement - the purest measure of self-serve digital conversion.', NULL, '[Derived Total]
Orders (Unassisted) = Orders Unassisted - Postpaid Flow + Orders Unassisted - HSI Flow + Orders Unassisted - BYOD Flow. Orders flows CAN be summed - each uses mutually exclusive product/flow filtering.

[Unassisted Flag Source]
Sourced directly from Adobe Analytics via [DA] Unassisted Proxy (No In-Store or Screen Share) - NOT a join against QGP.

[In-Store Signals ([DA] Unassisted Proxy)]
Modal Name (v82) contains ''Buy online while in store is available :''; Shipping Method (v27) contains ''while in store'' or ''Comprar por Internet desde una tienda''; Tracking Code - Visit Level Expiration (v45) = MGPO_RS_P_PPMGNWLRSU_9FED46B36BD7D485135485; Shipping Methods Displayed (v9) contains ''online while in store''.

[Screen-Share Signals ([DA] Unassisted Proxy)]
Alert Message (v94) = ''Message: Screen share in progress''; Page Name = ''TLife App | Support : Screen Share : Share This Code'' AND Action Name = ''Button Click : Allow''; Page URL (v6) contains ''assist.t-mobile''.

[Proxy Logic]
Unassisted Proxy EXCLUDES visits where any in-store or screen-share signal above is present.

[CVR %]
Orders (Unassisted) / UPV Actuals.

[Refresh]
Daily at 9 AM PT.', '[In-Store Orders]
Removes in-store-assisted orders (modal, shipping method, tracking code).

[Screen-Share Sessions]
Removes screen-share sessions.

[Android Ghost Hits]
Removes Android native app ghost order hits (no Order ID).', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('unassisted', 'androidGhostHit', 'cvr'), 16
 UNION ALL
  SELECT 'ordersUnassistedPostpaid', 'Metric', 'Bottom Funnel', 'Orders (Unassisted)', 'Orders Unassisted - Postpaid Flow', 'Postpaid', TRUE, 'ordersUnassistedTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Orders completed on the Postpaid phone/plan path with no employee assistance - the core digital new-customer acquisition conversion.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Unassisted Proxy]
[DA] Unassisted Proxy. Removes in-store and screen-share sessions (full signal list on the Orders (Unassisted) card).

[Layer 6 - Confirmation + Activation Intent]
[sdi] postpaid prospect orders visits. Includes VISITS containing a hit where Orders exists on a recognized Postpaid order confirmation page: ''TMO:UNO | Shop : Checkout : Submit Order Confirmation''; ''TMO | Shop : Checkout : Submit Order Confirmation''; ''TLife App | Shop : Checkout : Order Confirmation''; Page URL (c3) = www.t-mobile.com/checkout/finish or es.t-mobile.com/checkout/finish; Page URL (v6) = www.t-mobile.com/shop/checkout/confirmation or es.t-mobile.com/shop/checkout/confirmation. AND Flow Name (v154) = activation intent via [sdi] Activation Intent: ''ACTIVATION Intent'' | ''DEFERRED Intent'' | ''Prospect Activation Intent'' | ''Activacion de prospecto Intent''.

[Layer 7 - Exclude HSI/BYOD Orders]
[sdi] Postpaid (Exclude HSI BYOD Orders). Strips HSI and BYOD orders from the Postpaid count: HSI - Site = TLife App or TMO AND Flow = ACTIVATION/Prospect Activation Intent AND Product Type = ISP AND Orders exists; BYOD SIM - confirmation page with Orders AND Product Name = ''BYOS SIM or eSIM'', ''SIM Card'', ''Tarjeta SIM'', ''Mobile Internet SIM Card'', or ''Tarjeta SIM para Internet movil'' AND Flow = activation intent.

[Layer 8 - Android Ghost Hit Exclusion]
Removes HITS where OS = Google Android AND Order ID (v14) does not exist AND Page Layout State (v86) = Native App.

[CVR %]
Orders Unassisted Postpaid Flow / UPV Postpaid Flow.

[Summability]
Sums with Orders Unassisted HSI + Orders Unassisted BYOD = Orders (Unassisted) total.', '[HSI/BYOD Orders]
Excludes HSI and BYOD orders.

[In-Store/Screen-Share]
Excludes in-store/screen-share sessions.

[Prospect Filters]
All prospect filters applied.

[Android Ghost Hits]
Excludes Android native app ghost hits (missing Order ID).', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('sdiActivationIntent', 'flowName', 'productType', 'androidGhostHit', 'cvr'), 17
 UNION ALL
  SELECT 'ordersUnassistedHsi', 'Metric', 'Bottom Funnel', 'Orders (Unassisted)', 'Orders Unassisted - HSI Flow', 'Home Internet (HSI)', TRUE, 'ordersUnassistedTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Home Internet orders completed fully online by a prospect - confirming a new HSI subscriber acquired through the digital channel.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Unassisted Proxy]
[DA] Unassisted Proxy. Removes in-store and screen-share sessions.

[Layer 6 - Flow Filter]
[DA] HSI Prospect Orders. Includes HITS where ALL four conditions are true simultaneously: Site Name (v18) = TLife App OR TMO; Flow Name (v154) = ''ACTIVATION Intent'' OR ''Prospect Activation Intent'' OR ''Intencion de activacion de prospecto Intent'' (via [sdi] Activation Intent); Product Type (v100) = ISP; Orders exists.

[CVR %]
Orders Unassisted HSI Flow / UPV HSI Flow.

[Summability]
Part of Orders (Unassisted) total.', '[Scope Requirement]
Requires ISP product type AND HSI activation flow - will not count Postpaid or BYOD orders.

[Proxy Applied]
Unassisted proxy also applied.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('hsi', 'sdiActivationIntent', 'flowName', 'productType', 'cvr'), 18
 UNION ALL
  SELECT 'ordersUnassistedByod', 'Metric', 'Bottom Funnel', 'Orders (Unassisted)', 'Orders Unassisted - BYOD Flow', 'BYOD', TRUE, 'ordersUnassistedTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'SIM card/eSIM activations completed fully online by prospects bringing their own device to T-Mobile.', NULL, '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Unassisted Proxy]
[DA] Unassisted Proxy. Removes in-store and screen-share sessions.

[Layer 6 - Flow Filter]
[sdi] Postpaid Prospect BYOD Order Hit. Includes HITS where ALL conditions are true: Orders exists on a recognized order confirmation page (same confirmation page list as Postpaid orders); Product Name (v35) = any of: ''BYOS SIM or eSIM'', ''SIM Card'', ''Tarjeta SIM'', ''Mobile Internet SIM Card'', ''Tarjeta SIM para Internet movil''; Flow Name (v154) = activation intent (DEFERRED Intent, Prospect Activation Intent, Activacion de prospecto Intent, Prospect Activation, Activation Intent - via [sdi] Activation Intent).

[Layer 7 - Android Ghost Hit Exclusion]
Removes HITS where OS = Google Android AND Order ID (v14) does not exist AND Page Layout State (v86) = Native App.

[CVR %]
Orders Unassisted BYOD Flow / UPV BYOD Flow.

[Summability]
Part of Orders (Unassisted) total.', '[Product Scope]
Scoped to SIM/eSIM product names with activation intent.

[Android Ghost Hits]
Android ghost hit exclusion applied.

[Proxy Applied]
Unassisted proxy also applied.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('byod', 'sdiActivationIntent', 'flowName', 'androidGhostHit', 'cvr'), 19
 UNION ALL
  SELECT 'ordersAssistedTotal', 'Metric', 'Bottom Funnel', 'Orders (Assisted)', 'Orders (Assisted)', 'Postpaid + HSI + BYOD', TRUE, 'ordersTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Total orders completed with T-Mobile in-store staff or screen-share agent assistance - measuring the digital-to-assisted channel conversion where an employee helped close the sale.', NULL, '[Derived Total]
Orders (Assisted) = Orders Assisted - Postpaid Flow + Orders Assisted - HSI Flow + Orders Assisted - BYOD Flow. Assisted flows CAN be summed - same mutually exclusive product/flow scoping as unassisted flows.

[Assisted Flag Source]
Sourced directly from Adobe Analytics via [DA] Assisted Proxy (In-Store or Screen Share) - the mirror of the Unassisted Proxy.

[In-Store Signals ([DA] Assisted Proxy)]
Same four signals as Unassisted, but INCLUDED instead of excluded: Modal Name (v82) contains ''Buy online while in store is available :''; Shipping Method (v27) contains ''while in store'' or ''Comprar por Internet desde una tienda''; Tracking Code - Visit Level Expiration (v45) = MGPO_RS_P_PPMGNWLRSU_9FED46B36BD7D485135485; Shipping Methods Displayed (v9) contains ''online while in store''.

[Screen-Share Signals ([DA] Assisted Proxy)]
Alert Message (v94) = ''Message: Screen share in progress''; Page Name = ''TLife App | Support : Screen Share : Share This Code'' AND Action Name = ''Button Click : Allow''; Page URL (v6) contains ''assist.t-mobile''.

[Proxy Logic]
Assisted Proxy INCLUDES visits where any in-store or screen-share signal above is present (the mirror of Unassisted Proxy, which excludes these same signals).

[CVR %]
Orders (Assisted) / UPV Actuals.

[Refresh]
Daily at 9 AM PT.', '[Scope]
Scoped exclusively to sessions with confirmed in-store or screen-share assist signals.

[Prospect Filters]
Universal and prospect filters still applied.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('assisted', 'cvr'), 20
 UNION ALL
  SELECT 'ordersAssistedPostpaid', 'Metric', 'Bottom Funnel', 'Orders (Assisted)', 'Orders Assisted - Postpaid Flow', 'Postpaid', TRUE, 'ordersAssistedTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Postpaid orders completed with in-store or screen-share employee assistance - showing how much of Postpaid conversion volume flows through the assisted digital channel.', 'REQUIRES the assisted proxy signal (in-store or screen-share). Unassisted sessions are excluded entirely from this metric, not merely deprioritized.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Assisted Proxy]
[DA] Assisted Proxy (In-Store or Screen Share), Visit. Retains only VISITS with in-store or screen-share signals (full signal list on the Orders (Assisted) card).

[Layer 6 - Confirmation + Activation Intent]
[sdi] postpaid prospect orders visits. Includes VISITS containing a hit where Orders exists on a recognized Postpaid order confirmation page (same list as Orders Unassisted Postpaid) AND Flow Name (v154) = activation intent via [sdi] Activation Intent: ''ACTIVATION Intent'' | ''DEFERRED Intent'' | ''Prospect Activation Intent'' | ''Activacion de prospecto Intent''.

[Layer 7 - Exclude HSI/BYOD Orders]
[sdi] Postpaid (Exclude HSI BYOD Orders), same logic as Orders Unassisted Postpaid: removes ISP product type orders in activation flows; removes SIM/BYOD product name orders in activation flows.

[Layer 8 - Android Ghost Hit Exclusion]
Removes HITS where OS = Google Android AND Order ID (v14) does not exist AND Page Layout State = Native App.

[CVR %]
Orders Assisted Postpaid Flow / UPV Postpaid Flow.

[Summability]
Sums with Orders Assisted HSI + Orders Assisted BYOD = Orders (Assisted) total.', '[HSI/BYOD Orders]
Excludes HSI and BYOD orders.

[Android Ghost Hits]
Excludes Android ghost hits.

[Proxy Requirement]
REQUIRES assisted proxy signal - unassisted sessions are excluded from this metric.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('assisted', 'sdiActivationIntent', 'flowName', 'androidGhostHit', 'cvr'), 21
 UNION ALL
  SELECT 'ordersAssistedHsi', 'Metric', 'Bottom Funnel', 'Orders (Assisted)', 'Orders Assisted - HSI Flow', 'Home Internet (HSI)', TRUE, 'ordersAssistedTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Home Internet orders completed with in-store or screen-share employee assistance - confirming HSI conversions driven through the assisted digital channel.', 'REQUIRES the assisted proxy signal (in-store or screen-share). Unassisted sessions are excluded entirely from this metric, not merely deprioritized.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Assisted Proxy]
[DA] Assisted Proxy (In-Store or Screen Share), Visit. Retains only VISITS with in-store or screen-share signals.

[Layer 6 - Flow Filter]
[DA] HSI Prospect Orders. Includes HITS where ALL four conditions are true simultaneously: Site Name (v18) = TLife App OR TMO; Flow Name (v154) = ''ACTIVATION Intent'' OR ''Prospect Activation Intent'' OR ''Intencion de activacion de prospecto Intent''; Product Type (v100) = ISP; Orders exists.

[CVR %]
Orders Assisted HSI Flow / UPV HSI Flow.

[Summability]
Part of Orders (Assisted) total.', '[Scope Requirement]
Requires ISP product type AND HSI activation flow.

[Proxy Requirement]
REQUIRES assisted proxy signal - unassisted sessions excluded.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('hsi', 'assisted', 'flowName', 'productType', 'cvr'), 22
 UNION ALL
  SELECT 'ordersAssistedByod', 'Metric', 'Bottom Funnel', 'Orders (Assisted)', 'Orders Assisted - BYOD Flow', 'BYOD', TRUE, 'ordersAssistedTotal', TRUE, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'BYOD/SIM activations completed with in-store or screen-share employee assistance - showing assisted conversion on the device-switching path.', 'REQUIRES the assisted proxy signal (in-store or screen-share). Unassisted sessions are excluded entirely from this metric, not merely deprioritized.', '[Universal Filters]
DDM - Exclude Data Issue + [DA] Web Visits + [sdi] Postpaid (Exclude TFB).

[Layer 1]
Exclude TMO Mobile Network Carrier - removes VISITS where Mobile Carrier Network = T-Mobile.

[Layer 2]
[sdi] Exclude Authenticated or Network Authenticated VISITS - removes sessions where User State = Authenticated or Network Authenticated, or any LOA token (LOA0.5-LOA3) is present.

[Layer 3]
[sdi] Exclude Visit Login Page - removes VISITS where Page URL - Full (v103) contains associated_billing_accounts, t-mobile.com/guest-pay, or account.t-mobile.com/signin/v2/.

[Layer 4]
[DA] Exclude Single Page Visits - removes VISITS where Single Page Visits exists. Enforces the ''Non-Bounced'' qualifier.

[Layer 5 - Assisted Proxy]
[DA] Assisted Proxy (In-Store or Screen Share), Visit. Retains only VISITS with in-store or screen-share signals.

[Layer 6 - Flow Filter]
[sdi] Postpaid Prospect BYOD Order Hit. Includes HITS where ALL conditions are true: Orders exists on a recognized order confirmation page; Product Name (v35) = any of: ''BYOS SIM or eSIM'', ''SIM Card'', ''Tarjeta SIM'', ''Mobile Internet SIM Card'', ''Tarjeta SIM para Internet movil''; Flow Name (v154) = activation intent (DEFERRED Intent, Prospect Activation Intent, Activacion de prospecto Intent, Prospect Activation, Activation Intent).

[Layer 7 - Android Ghost Hit Exclusion]
Removes HITS where OS = Google Android AND Order ID (v14) does not exist AND Page Layout State = Native App.

[CVR %]
Orders Assisted BYOD Flow / UPV BYOD Flow.

[Summability]
Part of Orders (Assisted) total.', '[Product Scope]
Scoped to SIM/eSIM product names with activation intent.

[Android Ghost Hits]
Android ghost hit exclusion applied.

[Proxy Requirement]
REQUIRES assisted proxy signal.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('byod', 'assisted', 'flowName', 'androidGhostHit', 'cvr'), 23
 UNION ALL
  SELECT 'ordersTotal', 'Metric', 'Bottom Funnel', 'Orders', 'Orders (Overall)', 'Postpaid + HSI + BYOD, Unassisted + Assisted', FALSE, NULL, NULL, 'Adobe Analytics -> Databricks', 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long', 'Khalid / Ben', 'Total completed digital prospect orders across every purchase path, self-serve and employee-assisted combined - the single top-line conversion number for the funnel.', NULL, '[Derived Total]
Orders (Overall) = Orders (Unassisted) + Orders (Assisted). Corresponds directly to metric_name = ordersTotal in gold_unified_long, already computed there as ordersUnassistedTotal + ordersAssistedTotal.

[Base Metric]
The base Orders-confirmation population is the union of what Unassisted and Assisted split via their respective proxy segments ([DA] Unassisted Proxy vs [DA] Assisted Proxy) - every completed order lands in exactly one side, so the two are complementary, not overlapping.

[CVR %]
Orders (Overall) / UPV Actuals.', '[Composite Scope]
Sums Orders (Unassisted) and Orders (Assisted) - see each row''s own exclusions.

[Independent Logic]
No independent filtering logic beyond what those two already apply.', 'Daily at 9 AM PT (source cadence, see notes on this doc''s Adobe-cadence text vs deployed Silver schedule)', NULL, NULL, array('unassisted', 'assisted', 'cvr'), 24
 UNION ALL
  SELECT 'activationsBopis', 'Metric', 'Bottom Funnel', 'New BANs & VR Conversions', 'New BANs - Digital Unassisted', 'Consumer Postpaid', FALSE, NULL, NULL, 'Quarterly Game Plan (QGP)', 'prdrzrlakehouse.qgp_restricted.qgpweeklyview', 'Preeti Laharwani / Bharat Kavuru; Forecast: Sheenu Thakran''s team', 'New billing accounts activated through digital self-serve - operational confirmation that a digital order resulted in a new T-Mobile Postpaid line being turned on.', NULL, '[Source]
QGP operational data - no Adobe segment filtering.

[Counting Logic]
Metric: Consumer Postpaid Digital No Assistance New BAN activations.

[BAN Definition]
A BAN (Billing Account Number) = a unique T-Mobile customer account ID. A ''New BAN'' = a brand-new account.

[Operational Meaning]
Downstream operational confirmation that a completed digital order translated into an active line.

[Forecast Availability]
QGP provides actuals and a weekly forecast (Sheenu Thakran''s team).

[Act vs Fcst %]
(Actuals - QGP Forecast) / QGP Forecast.

[% of Consumer]
New BANs Dig. Unassist / total Consumer Postpaid new BANs (digital + assisted combined).

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
QGP operational metric - no Adobe filtering.

[Scope]
Counts Digital No Assistance (self-serve) activations only.', 'Daily at 9 AM PT', NULL, NULL, array('qgp', 'ban'), 25
 UNION ALL
  SELECT 'vrPostpaidActivations', 'Metric', 'Bottom Funnel', 'New BANs & VR Conversions', 'VR New BANs', 'Consumer Postpaid', FALSE, NULL, NULL, 'Quarterly Game Plan (QGP)', 'prdrzrlakehouse.qgp_restricted.qgpweeklyview', 'Preeti Laharwani / Bharat Kavuru', 'New billing accounts activated with help from a T-Mobile Virtual Retail agent - measuring the conversion output of the assisted digital channel.', 'VR New BANs typically far exceed Digital Unassisted BANs. This reflects VR channel outperformance, not a data quality issue.', '[Source]
QGP operational data - no Adobe segment filtering.

[Counting Logic]
Metric: Consumer Postpaid Digital Assistance New BAN activations.

[Activation Trigger]
Counts activations handled by VR sales teams initiated when a customer engaged via chat on T-Mobile.com.

[Forecast Availability]
QGP provides actuals and a weekly forecast.

[Act vs Fcst %]
(Actuals - QGP Forecast) / QGP Forecast.

[Outperformance Note]
VR New BANs typically far exceed Digital Unassisted BANs - VR-assisted journeys have a higher close rate. Strong positive variance reflects VR outperformance, not a data error.

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
QGP operational metric - no Adobe filtering.

[Scope]
Counts only VR-assisted (chat/call) activations.', 'Daily at 9 AM PT', NULL, NULL, array('qgp', 'ban', 'vr'), 26
 UNION ALL
  SELECT 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'Metric', 'Bottom Funnel', 'New BANs & VR Conversions', 'Digital % of Phone New Account Activations - Total (No Assistance + Assistance)', 'Consumer Postpaid - Phone, New Accounts', FALSE, NULL, NULL, 'Quarterly Game Plan (QGP), curated via QGP Archive Gold', 'prdrzranalytics.lab42.sdi_tbl_qgparchive_gold_curated_weekly', 'Preeti Laharwani / Bharat Kavuru', 'The share of all new Consumer Postpaid phone accounts activated through digital - self-serve and rep-assisted combined. The headline digital-penetration number, and the only one of the three carrying a QGP target.', NULL, '[Source]
QGP operational data, curated through the QGP Archive pipeline - no Adobe segment filtering. The ratio arrives pre-calculated from QGP; this table only carries it through.

[Numerator]
Digital new-account phone activations, No Assistance + Assistance, TM1-mapped.

[Denominator]
All Consumer Postpaid new-account phone activations, every sales channel, excl. National Retail Indirect (~145.9K, WE 8/15/26).

[Composition]
Equals No Assistance % + Assistance % exactly - both share this denominator.

[Act vs QGP %]
(Actuals - Target) / Target. Future weeks carry an Outlook equal to the Target.

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
No Adobe segment filtering. QGP operational data only.

[Denominator Scope]
Denominator excludes National Retail Indirect.

[Activation Scope]
New-account phone activations only - excludes AALs, upgrades, BTS, Broadband, Fiber.

[Ratio Caveat]
A ratio - do not sum across weeks.', 'Daily at 9 AM PT', NULL, NULL, array('qgp'), 27
 UNION ALL
  SELECT 'digitalPctNoAssistanceActivations', 'Metric', 'Bottom Funnel', 'New BANs & VR Conversions', 'Digital % of Phone New Account Activations - No Assistance', 'Consumer Postpaid - Phone, New Accounts', TRUE, 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', TRUE, 'Quarterly Game Plan (QGP), curated via QGP Archive Gold', 'prdrzranalytics.lab42.sdi_tbl_qgparchive_gold_curated_weekly', 'Preeti Laharwani / Bharat Kavuru', 'The share of new Consumer Postpaid phone accounts customers activated digitally with no rep involvement.', 'BOPIS counts as No Assistance here - the opposite of how [DA] Unassisted Proxy treats in-store signals elsewhere on this dashboard. No QGP target exists for this metric.', '[Source]
QGP operational data, curated via QGP Archive - no Adobe segment filtering. The ratio arrives pre-calculated from QGP.

[Numerator]
ConsumerPostpaidNewPhoneBANActivationsTM1MappedDigital - new-account phone activations with no rep involvement. Splits into BOPIS + Non-BOPIS (4,304 + 11,002 = 15,306, WE 8/15/26).

[Denominator]
Same as the Total row (~145.9K).

[Example Calculation]
WE 8/15/26: 15,306 / 145,900 = 10.5%.

[Target Availability]
No QGP target or Outlook - actuals only.

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
No Adobe segment filtering. QGP operational data only.

[BOPIS Treatment]
BOPIS counts as No Assistance here - the opposite of how Adobe''s [DA] Unassisted Proxy treats in-store signals in the Orders rows.

[Activation Scope]
New-account phone activations only.

[Future Weeks]
Blank on future weeks.', 'Daily at 9 AM PT', NULL, NULL, array('qgp'), 28
 UNION ALL
  SELECT 'digitalPctAssistanceActivations', 'Metric', 'Bottom Funnel', 'New BANs & VR Conversions', 'Digital % of Phone New Account Activations - Assistance', 'Consumer Postpaid - Phone, New Accounts', TRUE, 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', TRUE, 'Quarterly Game Plan (QGP), curated via QGP Archive Gold', 'prdrzranalytics.lab42.sdi_tbl_qgparchive_gold_curated_weekly', 'Preeti Laharwani / Bharat Kavuru', 'The share of new Consumer Postpaid phone accounts activated in digital with a rep assisting.', 'No QGP target exists for this metric - actuals only, blank on future weeks.', '[Source]
QGP operational data, curated via QGP Archive - no Adobe segment filtering. The ratio arrives pre-calculated from QGP.

[Numerator]
ConsumerPostpaidNewPhoneBANAssistedActivationsTM1MappedDigital - new-account phone activations transacted in digital with rep assistance, TM1-mapped (45,332, WE 8/15/26).

[Denominator]
Same as the Total row (~145.9K).

[Example Calculation]
WE 8/15/26: 45,332 / 145,900 = 31.1%.

[Target Availability]
No QGP target or Outlook - actuals only.

[Refresh]
Daily at 9 AM PT.', '[No Adobe Filtering]
No Adobe segment filtering. QGP operational data only.

[Reconciliation Note]
Counts activations, not Adobe orders - will not reconcile to Orders (Assisted).

[Activation Scope]
New-account phone activations only.

[Future Weeks]
Blank on future weeks.', 'Daily at 9 AM PT', NULL, NULL, array('qgp'), 29
 UNION ALL
  SELECT 'postpaid', 'Glossary', NULL, 'Product / LOB', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Postpaid', 'T-Mobile''s core consumer wireless service (pay monthly after usage). In Pulse: visitors and transactions on phone/plan/device pages - excluding HSI, BYOD, T-Mobile for Business, and B2B. Defined as Site Name (v18) = TMO, with TFB/b2b/Atwork/business/t-priority paths removed.', CAST(array() AS ARRAY<STRING>), 30
 UNION ALL
  SELECT 'hsi', 'Glossary', NULL, 'Product / LOB', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'HSI / Home Internet (HINT)', 'T-Mobile''s Home Internet service. ''HINT'' is the Adobe Site Section tag. Identified by /isp or /home-internet URL patterns, page names containing ''HINT'', Site Section = HINT, or Product Type (v100) = ISP.', CAST(array() AS ARRAY<STRING>), 31
 UNION ALL
  SELECT 'byod', 'Glossary', NULL, 'Product / LOB', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'BYOD (Bring Your Own Device)', 'The path for customers switching to T-Mobile with an existing device. Identified by Bring Your Own Phone pages, SIM card detail pages, carrier-switching pages, and the ''TMO | Shop : switch :'' page name pattern.', CAST(array() AS ARRAY<STRING>), 32
 UNION ALL
  SELECT 'prospect', 'Glossary', NULL, 'Audience', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Prospect', 'A visitor who is NOT a current T-Mobile customer. Defined by absence of: Authenticated/Network Authenticated User State, LOA tokens (LOA0.5-LOA3), T-Mobile mobile carrier network signal, and login/account management page visits.', CAST(array() AS ARRAY<STRING>), 33
 UNION ALL
  SELECT 'nonBounced', 'Glossary', NULL, 'Adobe Concept', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Non-Bounced', 'A visit where the visitor viewed more than one page. Enforced by [DA] Exclude Single Page Visits, which removes sessions where ''Single Page Visits exists'' in Adobe.', CAST(array() AS ARRAY<STRING>), 34
 UNION ALL
  SELECT 'uniqueVisitors', 'Glossary', NULL, 'Adobe Concept', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Unique Visitors (UV)', 'Adobe''s deduplicated count of distinct individuals (browser/device cookies). Deduplication happens at the scope of the metric - so a visitor browsing both Postpaid and HSI pages counts as 1 UV in UPV Actuals, but also as 1 UV in both UPV Postpaid Flow and UPV HSI Flow. This is why UPV flows cannot be summed.', CAST(array() AS ARRAY<STRING>), 35
 UNION ALL
  SELECT 'visit', 'Glossary', NULL, 'Adobe Concept', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'VISIT (Visit-Level Scope)', 'A single continuous session - starts on arrival, ends after 30 min of inactivity or at midnight. Visit-level segments evaluate the full session: if any hit meets the criteria, the whole visit is included or excluded.', CAST(array() AS ARRAY<STRING>), 36
 UNION ALL
  SELECT 'hit', 'Glossary', NULL, 'Adobe Concept', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'HIT (Hit-Level Scope)', 'A single page view, link click, or event within a visit. Hit-level segments filter on individual actions. Flow segments use hit-level logic to detect whether any page in a visit touched the relevant product area.', CAST(array() AS ARRAY<STRING>), 37
 UNION ALL
  SELECT 'sdiActivationIntent', 'Glossary', NULL, 'Adobe Segment', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, '[sdi] Activation Intent', 'A hit-level segment introduced in the updated funnel setup that consolidates all prospect activation flow signals into a single reusable segment. Includes HITS where Flow Name (v154) = ''ACTIVATION Intent'', ''DEFERRED Intent'', ''Prospect Activation Intent'', or ''Activacion de prospecto Intent''. Used as the intent qualifier in Orders Postpaid, Orders HSI, Orders BYOD, and their Assisted equivalents - replacing inline Flow Name logic from earlier configurations.', CAST(array() AS ARRAY<STRING>), 38
 UNION ALL
  SELECT 'flowName', 'Glossary', NULL, 'Adobe Dimension', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Flow Name (v154)', 'Adobe eVar capturing purchase intent flow. Key values in Pulse: ''ACTIVATION Intent'', ''DEFERRED Intent'', ''Prospect Activation Intent'', ''Activacion de prospecto Intent'' (new prospect flows); ''AAL Intent'' (HSI checkout). Critical for separating new prospect purchase flows from upgrade or existing-customer flows.', CAST(array() AS ARRAY<STRING>), 39
 UNION ALL
  SELECT 'productType', 'Glossary', NULL, 'Adobe Dimension', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Product Type (v100)', 'Adobe eVar tagging each hit by product type. Key values: ISP = HSI/Home Internet; SIMCARDS = SIM card/BYOD activation products. Used to isolate HSI and BYOD flows in cart and orders metrics.', CAST(array() AS ARRAY<STRING>), 40
 UNION ALL
  SELECT 'scOpenScAdd', 'Glossary', NULL, 'Adobe Event', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'scOpen / scAdd (Cart Events)', 'Adobe Analytics shopping cart events. scOpen = cart opened; scAdd = product added to cart. Together form the ''cart start'' signal for all Add to Cart metrics. Both require the hit to occur on a recognized cart page URL or page name.', CAST(array() AS ARRAY<STRING>), 41
 UNION ALL
  SELECT 'ban', 'Glossary', NULL, 'T-Mobile Operational', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'BAN (Billing Account Number)', 'Unique ID for a T-Mobile customer account. A ''New BAN'' = a brand new account activation. New BAN metrics confirm a completed digital order translated into an active line.', CAST(array() AS ARRAY<STRING>), 42
 UNION ALL
  SELECT 'vr', 'Glossary', NULL, 'T-Mobile Operational', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'VR (Virtual Retail)', 'T-Mobile''s human sales agents who assist customers via live chat and phone from T-Mobile.com. Sits between fully self-serve (Unassisted) and in-store purchase paths.', CAST(array() AS ARRAY<STRING>), 43
 UNION ALL
  SELECT 'mfc', 'Glossary', NULL, 'Data Source', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'MFC (Media Flow Chart)', 'T-Mobile''s centralized media spend source of truth in Databricks. LOB teams update Thursday; finalized by 3 PM PT Friday. Will Butler''s team does twice-weekly updates.', CAST(array() AS ARRAY<STRING>), 44
 UNION ALL
  SELECT 'qgp', 'Glossary', NULL, 'Data Source', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'QGP (Quarterly Game Plan)', 'T-Mobile''s internal system tracking weekly performance vs quarterly targets. Provides actuals and forecasts for VR Calls, VR Chats, Door Swings, New BANs, and VR New BANs. Daily refresh at 9 AM PT.', CAST(array() AS ARRAY<STRING>), 45
 UNION ALL
  SELECT 'unassisted', 'Glossary', NULL, 'Purchase Path', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Unassisted', 'Order completed with zero T-Mobile employee involvement. In Pulse, the unassisted flag comes from Adobe Analytics ([DA] Unassisted Proxy segment) - not a QGP join. Removes in-store modal signals, screen-share indicators, and specific shipping method/tracking code tags.', CAST(array() AS ARRAY<STRING>), 46
 UNION ALL
  SELECT 'assisted', 'Glossary', NULL, 'Purchase Path', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Assisted', 'Order completed WITH T-Mobile in-store staff or screen-share agent involvement. In Pulse, the assisted flag comes from [DA] Assisted Proxy (In-Store or Screen Share) - the mirror of the Unassisted Proxy. Captures the same signals (in-store modal, screen-share page, shipping method, tracking code) but includes these sessions instead of excluding them. Unassisted + Assisted orders together represent total digital prospect orders.', CAST(array() AS ARRAY<STRING>), 47
 UNION ALL
  SELECT 'actVsFcst', 'Glossary', NULL, 'Dashboard Metric', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Act vs Fcst D%', '(Actuals - Forecast) / Forecast. Up = actuals beat forecast; Down = underperformance. Used across MFC Spend, UPV, VR Calls/Chats, Exit Traffic, and New BANs.', CAST(array() AS ARRAY<STRING>), 48
 UNION ALL
  SELECT 'cvr', 'Glossary', NULL, 'Dashboard Metric', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'CVR% (Conversion Rate)', 'How effectively traffic converts to the next funnel stage. Cart CVR% = Cart Flow / UPV Flow (matched by product). Orders CVR% = Orders Flow / UPV Flow (matched by product). Each flow''s CVR uses the corresponding UPV flow as its denominator.', CAST(array() AS ARRAY<STRING>), 49
 UNION ALL
  SELECT 'wow', 'Glossary', NULL, 'Dashboard Metric', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'WoW% (Week-over-Week)', '(Current Week - Prior Week) / Prior Week. Tracks weekly directional momentum on UPV Actuals, UPV flows, and Cart/Orders flows.', CAST(array() AS ARRAY<STRING>), 50
 UNION ALL
  SELECT 'qtd', 'Glossary', NULL, 'Dashboard Metric', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'QTD (Quarter-to-Date)', 'Cumulative total or average from Q2 start to the most recent data date. Used for Spend, UPV, and conversion metrics to provide running quarter context alongside weekly point-in-time values.', CAST(array() AS ARRAY<STRING>), 51
 UNION ALL
  SELECT 'tfb', 'Glossary', NULL, 'Exclusion', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'TFB (T-Mobile for Business)', 'T-Mobile''s business/enterprise product line. Excluded from all Pulse metrics via [sdi] Postpaid (Exclude TFB): Site Name = TFB/b2b/Atwork, or URL contains /business or /t-priority.', CAST(array() AS ARRAY<STRING>), 52
 UNION ALL
  SELECT 'loa', 'Glossary', NULL, 'Exclusion', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'LOA (Level of Assurance)', 'Authentication confidence levels LOA0.5 (network device token) through LOA3 (high security ID). All authenticated sessions - any LOA level - are excluded from Pulse to maintain prospect-only scope.', CAST(array() AS ARRAY<STRING>), 53
 UNION ALL
  SELECT 'androidGhostHit', 'Glossary', NULL, 'Data Quality', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'Android Ghost Hit', 'A false order confirmation event that fires on native Android app views without an associated Order ID. Excluded via: OS = Google Android AND Order ID (v14) does not exist AND Page Layout State (v86) = Native App. Applied to all Orders Postpaid, HSI, and BYOD flows - both Unassisted and Assisted.', CAST(array() AS ARRAY<STRING>), 54
) AS t(apx_id, apx_record_type, apx_funnel_stage, apx_category, apx_metric_name, apx_flow_scope, apx_is_subflow, apx_parent_id, apx_summable_to_parent, apx_data_source_label, apx_source_table, apx_source_owner, apx_vp_one_liner, apx_warning_message, apx_build_detail_raw, apx_key_exclusions_raw, apx_refresh_cadence, apx_glossary_term, apx_glossary_definition, apx_referenced_glossary_ids, apx_sort_order)
;