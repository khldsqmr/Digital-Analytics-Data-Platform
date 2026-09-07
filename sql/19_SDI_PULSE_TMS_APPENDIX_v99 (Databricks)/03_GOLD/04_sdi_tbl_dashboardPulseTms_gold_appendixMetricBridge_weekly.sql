/* =================================================================================================
FILE:         04_sdi_tbl_dashboardPulseTms_gold_appendixMetricBridge_weekly.sql
LAYER:        Bridge / Mapping Table - Gold
TABLE NAME:   sdi_tbl_dashboardPulseTms_gold_appendixMetricBridge_weekly

PURPOSE:
  Hand-curated bridge between the appendix (sdi_vw_dashboardPulseTms_gold_appendix_long, keyed
  on apx_id) and the fact table (sdi_vw_dashboardPulseTms_gold_unified_long, keyed on
  data_source x metric_name x lob x channel_group x ...). This is what lets the two stay
  separate objects (different grain: dimension/reference vs. fact) while still supporting a
  live join, rather than forcing the appendix into gold_unified_long's UNION ALL.

GRAIN:
  One row per (apx_id, gul_data_source, gul_metric_name, gul_lob) combination that legitimately
  applies. Most apx_ids need exactly one row. MFC Spend rows need four (Channel + Granular grain,
  each x Actual + Forecast) - a genuine one-to-many relationship, since the appendix card
  describes the same underlying spend at two different fact-table grains.

WILDCARD DESIGN (this is the part that matters most):
  gul_lob is nullable. NULL means wildcard - match every lob value on the gold_unified_long
  side, not just a literal NULL. This is deliberate: whether a marketing-channel dimension or a
  LOB varies for a given metric is a property of the metric's definition, not something the
  appendix card describes differently per slice.

  channel_group, channel, tactic, message_type, and agency are NOT columns on this table at
  all - their absence is what makes them wildcard through every join. A definition card for
  "UPV Postpaid Flow" describes the same segment-stacking logic whether you're looking at the
  Paid Search slice, the Programmatic slice, or the All Channels rollup; nothing in the
  appendix text varies by channel_group, so there is nothing to match against.

  The join predicate this table is built for:

    SELECT g.*
    FROM gold_unified_long g
    JOIN appendixMetricBridge b
      ON b.gul_data_source = g.data_source
     AND b.gul_metric_name = g.metric_name
     AND (b.gul_lob = g.lob OR b.gul_lob IS NULL)
    WHERE b.apx_id = '<some apx_id>'

  Wildcarding lob this way is exactly the kind of pattern a text-to-SQL agent is likely to get
  wrong on its own (naive equality drops every wildcarded row silently) - see
  sdi_vw_dashboardPulseTms_gold_metricAnnotated_long, which bakes this join in once so an
  agentic Genie/SQL query never has to reconstruct it.

CONTENT NOTES:
  - eligibilityChecksCompleted has ZERO rows here on purpose - it has no live data_source in
    gold_unified_long yet (confirmed PENDING in the appendix itself). Do not add a row for it
    until the metric actually ships.
  - ADOBE and UPV_FORECAST rows use gul_lob = NULL, not the literal string
    'Postpaid + Broadband' - that string is a cosmetic static label on those two sources in
    gold_unified_long, not a real filterable dimension, so it is never something to match
    against.
  - New BANs - Digital Unassisted maps to activationsBopis, not activationsNewAalNoAssistance -
    this reverses an earlier Medium-confidence guess from a prior mapping pass, based on the
    appendix's own metric-ID description (Phone New BAN activations, same QGP target ID as
    activationsBopis). Worth a confirm with Preeti/Bharat before treating as final.
  - Digital % Total/No Assistance/Assistance all map to QGP_SCORECARD, referencing the same
    metric_name values Silver already produces from sdi_tbl_qgparchive_gold_curated_weekly
    (the QGP Archive's curated Gold layer) - this bridge does not care which raw source fed
    Silver, only the resulting data_source/metric_name/lob triple gold_unified_long exposes.
  - Content gaps in the source spreadsheet, not bridge gaps: no appendix row exists yet for
    MFC's TFB lob, and Platform Spend / Biddable Spend have zero appendix coverage at all.
    Nothing to bridge until those cards get written.

CHANGE LOG:
  - Built this session alongside Bronze/Silver/Gold appendix and the annotated view, resolving
    the wildcard-lob and channel/granular-grain join questions worked through with Khalid.
================================================================================================= */

CREATE OR REPLACE TABLE
  prdrzranalytics.lab42.sdi_tbl_dashboardPulseTms_gold_appendixMetricBridge_weekly
USING DELTA
CLUSTER BY (apx_id)
COMMENT 'PulseTMS appendix bridge - maps apx_id to the (data_source, metric_name, lob) triple that identifies its rows in gold_unified_long. gul_lob NULL = wildcard, matches every lob. channel_group/channel/tactic/message_type/agency are not columns here; their absence is what wildcards them through every join. See file header for the full join pattern and content notes.'
AS
SELECT * FROM VALUES
  -- MFC Spend - Postpaid (Channel + Granular grain, Actual + Forecast)
  ('mfcSpendPostpaid',   'MFC_SPEND_CHANNEL',  'mfcSpendActual',   'POSTPAID'),
  ('mfcSpendPostpaid',   'MFC_SPEND_CHANNEL',  'mfcSpendForecast', 'POSTPAID'),
  ('mfcSpendPostpaid',   'MFC_SPEND_GRANULAR', 'mfcSpendActual',   'POSTPAID'),
  ('mfcSpendPostpaid',   'MFC_SPEND_GRANULAR', 'mfcSpendForecast', 'POSTPAID'),

  -- MFC Spend - Broadband (Channel + Granular grain, Actual + Forecast)
  ('mfcSpendBroadband',  'MFC_SPEND_CHANNEL',  'mfcSpendActual',   'BROADBAND'),
  ('mfcSpendBroadband',  'MFC_SPEND_CHANNEL',  'mfcSpendForecast', 'BROADBAND'),
  ('mfcSpendBroadband',  'MFC_SPEND_GRANULAR', 'mfcSpendActual',   'BROADBAND'),
  ('mfcSpendBroadband',  'MFC_SPEND_GRANULAR', 'mfcSpendForecast', 'BROADBAND'),

  -- UPV (ADOBE, lob wildcarded - 'Postpaid + Broadband' is a static label, not a real filter)
  ('upvTotalAdobe',      'ADOBE',        'upvTotalAdobe', CAST(NULL AS STRING)),
  ('upvForecast',        'UPV_FORECAST', 'upvForecast',   CAST(NULL AS STRING)),
  ('upvPostpaid',        'ADOBE',        'upvPostpaid',   CAST(NULL AS STRING)),
  ('upvHsi',              'ADOBE',        'upvHsi',        CAST(NULL AS STRING)),
  ('upvByod',             'ADOBE',        'upvByod',       CAST(NULL AS STRING)),

  -- QGP Scorecard (no lob dimension on this source at all - always NULL)
  ('vrCalls',            'QGP_SCORECARD', 'vrCalls',      CAST(NULL AS STRING)),
  ('vrChats',             'QGP_SCORECARD', 'vrChats',      CAST(NULL AS STRING)),
  ('storeTraffic',        'QGP_SCORECARD', 'storeTraffic', CAST(NULL AS STRING)),

  -- Add to Cart (ADOBE, lob wildcarded)
  ('cartstartTotal',      'ADOBE', 'cartstartTotal',    CAST(NULL AS STRING)),
  ('cartstartPostpaid',   'ADOBE', 'cartstartPostpaid', CAST(NULL AS STRING)),
  ('cartstartHsi',        'ADOBE', 'cartstartHsi',      CAST(NULL AS STRING)),
  ('cartstartByod',       'ADOBE', 'cartstartByod',     CAST(NULL AS STRING)),

  -- Orders Unassisted (ADOBE, lob wildcarded)
  ('ordersUnassistedTotal',    'ADOBE', 'ordersUnassistedTotal',    CAST(NULL AS STRING)),
  ('ordersUnassistedPostpaid', 'ADOBE', 'ordersUnassistedPostpaid', CAST(NULL AS STRING)),
  ('ordersUnassistedHsi',      'ADOBE', 'ordersUnassistedHsi',      CAST(NULL AS STRING)),
  ('ordersUnassistedByod',     'ADOBE', 'ordersUnassistedByod',     CAST(NULL AS STRING)),

  -- Orders Assisted (ADOBE, lob wildcarded)
  ('ordersAssistedTotal',    'ADOBE', 'ordersAssistedTotal',    CAST(NULL AS STRING)),
  ('ordersAssistedPostpaid', 'ADOBE', 'ordersAssistedPostpaid', CAST(NULL AS STRING)),
  ('ordersAssistedHsi',      'ADOBE', 'ordersAssistedHsi',      CAST(NULL AS STRING)),
  ('ordersAssistedByod',     'ADOBE', 'ordersAssistedByod',     CAST(NULL AS STRING)),

  -- Orders (Overall) - new apx_id, maps to the already-computed ordersTotal metric
  ('ordersTotal',         'ADOBE', 'ordersTotal', CAST(NULL AS STRING)),

  -- New BANs & VR Conversions (QGP_SCORECARD, lob always NULL)
  -- NOTE: activationsBopis, not activationsNewAalNoAssistance - see CONTENT NOTES above
  ('activationsBopis',        'QGP_SCORECARD', 'activationsBopis',        CAST(NULL AS STRING)),
  ('vrPostpaidActivations',   'QGP_SCORECARD', 'vrPostpaidActivations',   CAST(NULL AS STRING)),
  ('digitalPctConsumerPostpaidActivationsTotalInclAssisted', 'QGP_SCORECARD', 'digitalPctConsumerPostpaidActivationsTotalInclAssisted', CAST(NULL AS STRING)),
  ('digitalPctNoAssistanceActivations', 'QGP_SCORECARD', 'digitalPctNoAssistanceActivations', CAST(NULL AS STRING)),
  ('digitalPctAssistanceActivations',   'QGP_SCORECARD', 'digitalPctAssistanceActivations',   CAST(NULL AS STRING))

  -- eligibilityChecksCompleted intentionally has NO rows - PENDING, no live gold_unified_long
  -- data_source/metric_name to bridge to yet.

AS t(apx_id, gul_data_source, gul_metric_name, gul_lob)
;