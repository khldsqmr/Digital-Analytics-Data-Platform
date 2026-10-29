/* =================================================================================================
FILE:     04_sdi_weeklyNarrative_validation_checks_adhoc.sql
PURPOSE:  Run these after deploying files 01-03. Every check except 4 and 5 should return 0 rows.
================================================================================================= */


/* -------------------------------------------------------------------------------------------------
   1. Registry: duplicate metric_key (a view cannot enforce a primary key)
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, COUNT(*) AS n
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
GROUP BY metric_key
HAVING COUNT(*) > 1;


/* -------------------------------------------------------------------------------------------------
   2. Registry: target_metric_key points at a key that does not exist
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, r.target_metric_key
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
LEFT JOIN prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static t
  ON t.metric_key = r.target_metric_key
WHERE r.target_metric_key IS NOT NULL
  AND t.metric_key IS NULL;


/* -------------------------------------------------------------------------------------------------
   3. Registry vs Gold: active slices that match NOTHING in Gold (typo in name / type / lob)
      Checked across all weeks, so a slice that is simply empty this week is not flagged.
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, r.data_source, r.metric_name, r.metric_type, r.source_lob, r.channel_group
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
WHERE  r.is_active = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g
    WHERE g.data_source   =   r.data_source
      AND g.metric_name   =   r.metric_name
      AND g.metric_type   <=> r.metric_type
      AND ((r.source_lob IS NULL AND g.true_lob IS NULL) OR array_contains(r.source_lob, g.true_lob))
      AND g.channel_group <=> r.channel_group
  );


/* -------------------------------------------------------------------------------------------------
   4. Take a snapshot (latest completed Saturday), then a backfill example
   ------------------------------------------------------------------------------------------------- */
CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(NULL, 'MANUAL', NULL);

-- CALL prdrzranalytics.lab42.sdi_sp_weeklyNarrative_gold_factsHistory_long(DATE'2026-10-24', 'MANUAL', NULL);


/* -------------------------------------------------------------------------------------------------
   5. Look at what the AI will see: headline rows for the latest week
   ------------------------------------------------------------------------------------------------- */
SELECT
  week_end, metric_lob, display_name, unit,
  week_value, wow_pct, yoy_pct,
  target_week_value, vs_target_abs, vs_target_pct, is_target_same_as_actual,
  avg_prior_4wk, avg_prior_13wk, avg_prior_52wk, zscore_prior_13wk, prior_52wk_weeks,
  mtd_avg_weekly, qtd_avg_weekly, ytd_avg_weekly,
  qtd_total, qtd_target_total, qtd_vs_target,
  is_data_current, source_validation_status
FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE row_role = 'ACTUAL'
  AND week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long)
ORDER BY metric_lob, sort_order;

output:
week_end	metric_lob	display_name	unit	week_value	wow_pct	yoy_pct	target_week_value	vs_target_abs	vs_target_pct	is_target_same_as_actual	avg_prior_4wk	avg_prior_13wk	avg_prior_52wk	zscore_prior_13wk	prior_52wk_weeks	mtd_avg_weekly	qtd_avg_weekly	ytd_avg_weekly	qtd_total	qtd_target_total	qtd_vs_target	is_data_current	source_validation_status
2026-10-24	null	Store traffic	COUNT	3494555	-0.0010222601726465053	null	3543422.999998	-48867.999997999985	-0.013791184399386536	false	3658922.25	3668458.5384615385	3462931.0714285714	-0.7095284778572473	42	3563577.9454472433	3563577.9454472433	3510315.254312949	12217980	12139352	0.0064771167357202675	true	Warning
2026-10-24	null	Virtual retail calls	COUNT	96683.000001	0.010725822501452598	null	100541.733103	-3858.733102000013	-0.03837941701330061	false	95939.9999995	91899.53846153847	88141.40476173809	1.0914507022917157	42	95605.13695005879	95605.13695005879	89529.81235201158	327788.999998	348485.213699	-0.059389072727992764	true	Warning
2026-10-24	null	Virtual retail chats	COUNT	17698.999997	-0.09745028048405668	null	27081.525898	-9382.525901	-0.3464548466116125	false	18905.24999925	20587.999999846157	28400.07142814286	-1.5407506254336911	42	18979.335704291963	18979.335704291963	28530.350456040574	65071.999995	95047.8289	-0.3153762611088953	true	Warning
2026-10-24	BROADBAND	UPV HSI	COUNT	339715	-0.21531375669563968	-0.32303415567334903	null	null	null	null	433291.5	367958.6923076923	445951.73076923075	-0.5426400671021924	52	400683.0084187094	400683.0084187094	430542.9168068308	1373770.1428571427	null	null	true	Healthy
2026-10-24	BROADBAND	Cart starts HSI	COUNT	31598	0.1492271322058556	34.78482446206115	null	null	null	null	27055.5	26099.153846153848	13545.865384615385	3.93551705297424	52	28656.961915453576	28656.961915453576	17052.38064362674	98252.42857142858	null	null	true	Healthy
2026-10-24	BROADBAND	Orders unassisted HSI	COUNT	13665	0.22501120573733746	1.2706879361914258	null	null	null	null	12060	12662.307692307691	9901.192307692309	0.9770894888859388	52	12316.959872953317	12316.959872953317	10763.902465628644	42229.57142857143	null	null	true	Healthy
2026-10-24	BROADBAND	Orders assisted HSI	COUNT	726	0.32723948811700176	null	null	null	null	null	573.25	685.2307692307693	439.5576923076923	0.33134739075293557	52	618.0000772500098	618.0000772500098	540.9966384612455	2118.857142857143	null	null	true	Healthy
2026-10-24	BROADBAND	MFC spend, Broadband	USD	null	null	null	10996756.158813477	null	null	null	9621034.131655062	8956687.713455299	8835292.294822989	null	52	11208178.77015058	11208178.77015058	8961259.372719355	27219857.924003363	32142252.408569336	-0.1531440429872808	false	Warning
2026-10-24	BROADBAND	Biddable spend, Broadband	USD	3745560.2021109154	0.11572969311173376	0.20471869502561413	null	null	null	null	2944374.702879662	3351146.667833879	4272889.55968889	0.43606582288105905	52	3325851.2639865014	3325851.2639865014	4610779.686676826	11402917.194017462	null	null	true	Warning
2026-10-24	BYOD	UPV BYOD	COUNT	83793	0.0273534243888085	0.499248523886205	null	null	null	null	84118.75	90467.69230769231	73538.40384615384	-0.5996783060925999	52	81132.17680818877	81132.17680818877	78242.75163208165	278167.4285714286	null	null	true	Healthy
2026-10-24	BYOD	Cart starts BYOD	COUNT	66583	0.04434092477570739	1.737900407089107	null	null	null	null	69041	62259.307692307695	53133	0.25560469155203797	52	62314.59112265723	62314.59112265723	52843.16888393773	213650	null	null	true	Healthy
2026-10-24	BYOD	Orders unassisted BYOD	COUNT	443	-0.08091286307053946	-0.7485811577752555	null	null	null	null	1253.25	2729.5384615384614	2849.519230769231	-1.9865672878754461	52	700.875087609386	700.875087609386	3037.710468394382	2403	null	null	true	Healthy
2026-10-24	BYOD	Orders assisted BYOD	COUNT	141	-0.12422360248447206	46	null	null	null	null	358.5	635.9230769230769	632.5576923076923	-2.1555729751217525	52	226.95836170312856	226.95836170312856	723.9225662349081	778.1428571428571	null	null	true	Healthy
2026-10-24	FIBER	Biddable spend, Fiber	USD	null	null	null	null	null	null	null	0	32781.2958055	255008.38828953193	null	47	0	0	247058.73141712171	0	null	null	false	Warning
2026-10-24	POSTPAID	UPV Postpaid	COUNT	616080	-0.03257870661890905	-0.041034054436042466	null	null	null	null	650398.75	615465.7692307692	586902.1538461539	0.005522976853486275	52	613239.2016549002	613239.2016549002	570272.2986896193	2102534.1428571427	null	null	true	Healthy
2026-10-24	POSTPAID	Cart starts Postpaid	COUNT	81547	0.05667785365347977	-0.2406038143485063	null	null	null	null	74450.75	70802.61538461539	61624.07692307692	2.188364419565353	52	77392.05134067309	77392.05134067309	60389.737983734725	265344.14285714284	null	null	true	Healthy
2026-10-24	POSTPAID	Orders unassisted Postpaid	COUNT	716	-0.11276332094175956	-0.7576988155668358	null	null	null	null	1739	3124	3503.2884615384614	-2.105838897850292	52	1122.7501403437677	1122.7501403437677	3305.841784234092	3849.4285714285716	null	null	true	Healthy
2026-10-24	POSTPAID	Orders assisted Postpaid	COUNT	246	-0.2038834951456311	21.363636363636363	null	null	null	null	584.25	838.4615384615385	752.7692307692307	-2.3105727700603564	52	372.2500465312558	372.2500465312558	857.8686955340272	1276.2857142857142	null	null	true	Healthy
2026-10-24	POSTPAID	MFC spend, Postpaid	USD	null	null	null	23134676.580932617	null	null	null	25131218.860746384	23026044.32263169	22432040.965223663	null	52	25391408.037251152	25391408.037251152	21505742.660887692	61664837.20843506	61825271.81500244	-0.0025949680746644166	false	Warning
2026-10-24	POSTPAID	Biddable spend, Postpaid	USD	12293027.66792212	0.10541325465976747	2.97459149607783	null	null	null	null	13536959.920841739	12464469.937181856	8817200.746540667	-0.062440972589867215	52	11455535.128401278	11455535.128401278	9901118.970262144	39276115.530717894	null	null	true	Warning
2026-10-24	POSTPAID	Digital phone activations (BOPIS + non-BOPIS)	COUNT	13567	0.08588122298703382	null	11832.26849	1734.7315099999996	0.1466102219930272	false	15769.25	15307.461538461539	13734.166666666666	-0.8057666889280741	42	13491.626686453337	13491.626686453337	13915.198793756217	46257	40871.66515	0.1317620613262438	true	Warning
2026-10-24	POSTPAID	Activations, New + AAL, no assistance	COUNT	16154	0.10719671007539411	null	13325.590516	2828.409484	0.2122539695786041	false	16675.75	15882.076923076924	13616.238095238095	0.09785048466891152	42	15472.626934078367	15472.626934078367	13859.434483428631	53049	46094.213285000005	0.15088199188906048	true	Warning
2026-10-24	POSTPAID	Digital % of postpaid activations	PCT	null	null	null	null	null	null	null	0.53632	0.5188016999999999	0.5118298461538463	null	26	null	null	0.4945774699453551	null	null	null	false	Warning
2026-10-24	POSTPAID	Virtual retail postpaid activations	COUNT	70782.000002	-0.034970755409696075	null	79009.100975	-8227.100972999993	-0.10412852280907747	false	73852.9999985	72715.92307746153	65269.571428333344	-0.6018037184587939	42	73040.63412891261	73040.63412891261	66278.54612383716	250424.99999600003	273323.665147	-0.08377856757732449	true	Warning
2026-10-24	POSTPAID + BROADBAND	UPV (total Adobe)	COUNT	1732595	-0.0694268843273762	-0.034021756038217776	1678687	53908	0.03211319322780248	false	1755644.25	1665282.2307692308	1632174.1538461538	0.38988903174117506	52	1728975.8411219805	1728975.8411219805	1579169.642213835	5927916.428571429	5684831	0.042760361490329135	true	Healthy
2026-10-24	POSTPAID + BROADBAND	UPV in flow	COUNT	807690	-0.07144458431771539	-0.03475844761136504	null	null	null	null	900332	841049.6153846154	790980.6346153846	-0.2736858449909922	52	849043.6894637946	849043.6894637946	781555.9674904643	2911006.5714285714	null	null	true	Healthy
2026-10-24	POSTPAID + BROADBAND	Cart starts	COUNT	179728	0.06711632546430435	0.3555579020409394	null	null	null	null	170547.25	159161.07692307694	128302.94230769231	1.006945100297627	52	168363.60437878387	168363.60437878387	130285.28751129922	577246.5714285714	null	null	true	Healthy
2026-10-24	POSTPAID + BROADBAND	Orders (total)	COUNT	15937	0.1839387861228734	0.4826495487952367	null	null	null	null	16568.25	20675.46153846154	18078.884615384617	-1.3566654041019264	52	15357.793586390866	15357.793586390866	19230.242618487297	52655.28571428572	null	null	true	Healthy
2026-10-24	POSTPAID + BROADBAND	Orders unassisted	COUNT	14824	0.1912568306010929	0.3809035863996273	null	null	null	null	15052.25	18515.846153846152	16254	-1.2518228697835112	52	14140.585100906472	14140.585100906472	17107.45471825712	48482	null	null	true	Healthy
2026-10-24	POSTPAID + BROADBAND	Orders assisted	COUNT	1113	0.0943952802359882	78.5	null	null	null	null	1516	2159.6153846153848	1824.8846153846155	-1.8008208882130026	52	1217.2084854843943	1217.2084854843943	2122.7879002301806	4173.285714285715	null	null	true	Healthy
2026-10-24	POSTPAID + BROADBAND	MFC spend, Postpaid + Broadband	USD	null	null	null	34131432.739746094	null	null	null	34752252.99240144	31982732.03608699	31267333.260046642	null	52	36599586.80740173	36599586.80740173	30467002.03360705	88884695.13243842	93967524.22357178	-0.054091337758777835	false	Warning
2026-10-24	POSTPAID + BROADBAND + FIBER	Biddable spend, all LOBs	USD	16038587.870033035	0.10780538547933238	1.5406820574093674	null	null	null	null	16481334.623721402	15835789.710126814	13320578.65718356	0.0740531328308096	52	14781386.39238778	14781386.39238778	14718196.856498448	50679032.72473536	null	null	true	Warning


/* -------------------------------------------------------------------------------------------------
   6. Registry slices missing from the latest snapshot (expected on weeks where a source lags,
      e.g. QGP actuals reporting lag; investigate anything else)
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, r.display_name, r.metric_role
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
WHERE  r.is_active = TRUE
  AND NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long f
    WHERE f.metric_key = r.metric_key
      AND f.row_role IN ('ACTUAL', 'TARGET')
      AND f.week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long)
  );


/* -------------------------------------------------------------------------------------------------
   7. Reconcile with the PulseTMS wide sense-check view (should differ only on BOUNDARY_FIRST
      weeks, where the snapshot uses the full natural week)
   ------------------------------------------------------------------------------------------------- */
SELECT
  f.week_end,
  f.week_type,
  f.week_value       AS snapshot_upv_total,
  w.upvTotalAdobe    AS wide_upv_total,
  f.week_value - w.upvTotalAdobe AS diff
FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long f
JOIN prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide w
  ON  w.qgp_date      = f.week_end
  AND w.channel_group = 'All Channels'
WHERE f.metric_key = 'pulseTms.upvTotalAdobe'
  AND f.row_role   = 'ACTUAL'
  AND ABS(COALESCE(f.week_value - w.upvTotalAdobe, 0)) > 0.01;

output:
week_end	week_type	snapshot_upv_total	wide_upv_total	diff
2026-10-03	BOUNDARY_FIRST	1683543	721518.4285714285	962024.5714285715
2026-07-04	BOUNDARY_FIRST	1510963	863407.4285714285	647555.5714285715

/* -------------------------------------------------------------------------------------------------
   8. Lineage: active registry rows pointing at an upstream the snapshot procedure does not read
      (only PulseTMS Gold is wired today; any row here needs a new adapter before it will snapshot)
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, source_project, source_object
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
WHERE  is_active = TRUE
  AND  NOT (source_project = 'PULSE_TMS'
            AND source_object = 'prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long');


/* -------------------------------------------------------------------------------------------------
   9. LOB: metric_lob with an unknown LOB code, or several LOBs not in the fixed order
      (POSTPAID, BROADBAND, BYOD, FIBER, TFB, METRO). NULL is allowed (metric has no LOB).
   ------------------------------------------------------------------------------------------------- */
WITH Lobs AS (
  SELECT metric_key, metric_lob,
         transform(split(metric_lob, ' \\+ '),
                   x -> array_position(array('POSTPAID', 'BROADBAND', 'BYOD', 'FIBER', 'TFB', 'METRO'), x)) AS pos
  FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
  WHERE metric_lob IS NOT NULL
)
SELECT metric_key, metric_lob
FROM   Lobs
WHERE  array_contains(pos, 0)
   OR  pos <> array_sort(pos)
   OR  size(pos) <> size(array_distinct(pos));


/* -------------------------------------------------------------------------------------------------
   10. LOB: source_lob that sums several LOBs on a non-additive unit (PCT), or an empty array
       (use NULL, not [], for a source with no LOB)
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, unit, source_lob
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
WHERE  size(source_lob) = 0
   OR  (size(source_lob) > 1 AND unit = 'PCT');


/* -------------------------------------------------------------------------------------------------
   11. LOB: source_lob values that never appear in PulseTMS Gold for that metric (typo or
       a LOB PulseTMS does not carry yet, e.g. METRO)
   ------------------------------------------------------------------------------------------------- */
SELECT r.metric_key, m.member_lob
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static r
LATERAL VIEW explode(r.source_lob) m AS member_lob
WHERE  r.is_active = TRUE
  AND  NOT EXISTS (
    SELECT 1
    FROM prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long g
    WHERE g.data_source = r.data_source
      AND g.metric_name = r.metric_name
      AND g.metric_type <=> r.metric_type
      AND g.true_lob    = m.member_lob
  );


/* -------------------------------------------------------------------------------------------------
   12. LOB: partial summed totals in the latest snapshot (one of the source_lob values missing this week)
   ------------------------------------------------------------------------------------------------- */
SELECT week_end, metric_key, channel_group, source_lob
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE  is_lob_rollup_complete = FALSE;
output:
week_end	metric_key	channel_group	source_lob
2026-10-24	pulseTms.mfcSpendActualTotal	All Channels	POSTPAID + BROADBAND
2026-10-24	pulseTms.mfcSpendActualTotal	Other	POSTPAID + BROADBAND
2026-10-24	pulseTms.mfcSpendActualTotal	Paid Search	POSTPAID + BROADBAND
2026-10-24	pulseTms.mfcSpendActualTotal	Paid Social	POSTPAID + BROADBAND
2026-10-24	pulseTms.mfcSpendActualTotal	Programmatic	POSTPAID + BROADBAND
2026-10-03	pulseTms.mfcSpendForecastTotal	Paid Search	POSTPAID + BROADBAND
2026-10-03	pulseTms.mfcSpendForecastTotal	Paid Social	POSTPAID + BROADBAND
2026-10-03	pulseTms.mfcSpendForecastTotal	Programmatic	POSTPAID + BROADBAND

/* -------------------------------------------------------------------------------------------------
   13. LOB: MFC Postpaid + Broadband rollup must equal the PulseTMS wide view total
   ------------------------------------------------------------------------------------------------- */
SELECT f.week_end, f.channel_group, f.metric_value AS snapshot_total, w.mfcSpendActualTotal AS wide_total
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long f
JOIN   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_wide w
  ON   w.qgp_date      = f.week_end
 AND   w.channel_group = f.channel_group
WHERE  f.metric_key = 'pulseTms.mfcSpendActualTotal'
  AND  ABS(COALESCE(f.metric_value, 0) - COALESCE(w.mfcSpendActualTotal, 0)) > 0.01;


/* -------------------------------------------------------------------------------------------------
   14. Target: weeks where target equals actual (PulseTMS MFC forecast fallback). Expected
       for weeks with no MFC forecast; the narrative must say "no target available".
   ------------------------------------------------------------------------------------------------- */
SELECT week_end, metric_key, channel_group, week_value, target_week_value
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE  is_target_same_as_actual = TRUE
  AND  row_role = 'ACTUAL'
ORDER BY week_end DESC, metric_key;


/* -------------------------------------------------------------------------------------------------
   15. Naming convention: codes must be UPPER_SNAKE, metric_role must be ACTUAL or TARGET
   ------------------------------------------------------------------------------------------------- */
SELECT metric_key, metric_lob, metric_role, unit, source_project
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_control_metricRegistry_static
WHERE  NOT COALESCE(metric_lob, 'NULL') RLIKE '^[A-Z][A-Z0-9_]*( \\+ [A-Z][A-Z0-9_]*)*$'
   OR  metric_role IS NULL OR metric_role NOT IN ('ACTUAL', 'TARGET')
   OR  NOT unit           RLIKE '^[A-Z][A-Z0-9_]*$'
   OR  NOT source_project RLIKE '^[A-Z][A-Z0-9_]*$';


/* -------------------------------------------------------------------------------------------------
   16. Backfill coverage: Saturdays in the PulseTMS calendar with no snapshot (after running 05).
       Expect rows only before p_start_week_end or for weeks PulseTMS has no data.
   ------------------------------------------------------------------------------------------------- */
SELECT cal.qgp_date AS missing_week_end
FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_dim_qgp_calendar cal
WHERE  DAYOFWEEK(cal.qgp_date) = 7
  AND  cal.is_complete_period = TRUE
  AND  cal.qgp_date >= (SELECT MIN(week_end) FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long)
  AND  NOT EXISTS (
    SELECT 1 FROM prdrzranalytics.lab42.sdi_tbl_weeklyNarrative_gold_factsHistory_long h
    WHERE h.week_end = cal.qgp_date
  )
ORDER BY cal.qgp_date;


/* -------------------------------------------------------------------------------------------------
   17. What moved: one row per headline metric per week (should return 0 rows)
   ------------------------------------------------------------------------------------------------- */
SELECT week_end, metric_key, COUNT(*) AS n
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long
GROUP BY week_end, metric_key
HAVING COUNT(*) > 1;


/* -------------------------------------------------------------------------------------------------
   18. What moved: highlights for the latest week, in rank order (eyeball against the report)
   ------------------------------------------------------------------------------------------------- */
SELECT
  highlight_rank, metric_lob, display_name,
  highlight_reasons, ROUND(highlight_score, 2) AS score,
  direction, wow_sentiment, target_position, target_sentiment,
  top_driver_channel, ROUND(top_driver_share_of_change, 2) AS driver_share,
  source_validation_status
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long
WHERE  week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long)
  AND  is_highlight = TRUE
ORDER BY highlight_rank;
output:
highlight_rank	metric_lob	display_name	highlight_reasons	score	direction	wow_sentiment	target_position	target_sentiment	top_driver_channel	driver_share	source_validation_status
1	null	Virtual retail chats	["OFF_TARGET","QTD_OFF_PACE","NEW_13WK_LOW"]	10.51	DOWN	NEUTRAL	BELOW_TARGET	NEUTRAL	null	null	Warning
2	POSTPAID	Activations, New + AAL, no assistance	["BIG_WOW","OFF_TARGET","QTD_OFF_PACE"]	5.03	UP	GOOD	ABOVE_TARGET	GOOD	null	null	Warning
3	POSTPAID	Digital phone activations (BOPIS + non-BOPIS)	["OFF_TARGET","QTD_OFF_PACE"]	4.39	UP	GOOD	ABOVE_TARGET	GOOD	null	null	Warning
4	BROADBAND	Orders assisted HSI	["BIG_WOW"]	3.27	UP	GOOD	null	NEUTRAL	Direct	0.54	Healthy
5	POSTPAID	Virtual retail postpaid activations	["OFF_TARGET","QTD_OFF_PACE"]	2.79	DOWN	BAD	BELOW_TARGET	BAD	null	null	Warning
6	BROADBAND	Orders unassisted HSI	["BIG_WOW"]	2.25	UP	GOOD	null	NEUTRAL	Paid Search	0.46	Healthy
7	BROADBAND	UPV HSI	["BIG_WOW"]	2.15	DOWN	BAD	null	NEUTRAL	Direct	0.94	Healthy
8	POSTPAID	Orders assisted Postpaid	["UNUSUAL_MOVE","BIG_WOW","NEW_13WK_LOW"]	2.04	DOWN	BAD	null	NEUTRAL	Other	0.87	Healthy
9	null	Virtual retail calls	["QTD_OFF_PACE"]	1.98	UP	NEUTRAL	BELOW_TARGET	NEUTRAL	null	null	Warning
10	BROADBAND	Cart starts HSI	["UNUSUAL_MOVE","BIG_WOW","NEW_13WK_HIGH"]	1.97	UP	GOOD	null	NEUTRAL	Paid Search	0.43	Healthy
11	POSTPAID + BROADBAND	Orders unassisted	["BIG_WOW"]	1.91	UP	GOOD	null	NEUTRAL	Paid Search	0.48	Healthy
12	POSTPAID + BROADBAND	Orders (total)	["BIG_WOW"]	1.84	UP	GOOD	null	NEUTRAL	Paid Search	0.47	Healthy
13	POSTPAID + BROADBAND	UPV (total Adobe)	["QTD_OFF_PACE"]	1.43	DOWN	BAD	ABOVE_TARGET	GOOD	Direct	1.05	Healthy
14	BYOD	Orders assisted BYOD	["UNUSUAL_MOVE","BIG_WOW","NEW_13WK_LOW"]	1.24	DOWN	BAD	null	NEUTRAL	Other	0.8	Healthy
15	BROADBAND	Biddable spend, Broadband	["BIG_WOW"]	1.16	UP	NEUTRAL	null	NEUTRAL	Programmatic - All	0.51	Warning
16	POSTPAID	Orders unassisted Postpaid	["UNUSUAL_MOVE","BIG_WOW","NEW_13WK_LOW"]	1.13	DOWN	BAD	null	NEUTRAL	Organic Search	0.84	Healthy
17	POSTPAID	Cart starts Postpaid	["UNUSUAL_MOVE","NEW_13WK_HIGH"]	1.09	UP	GOOD	null	NEUTRAL	Paid Search	0.92	Healthy
18	POSTPAID + BROADBAND + FIBER	Biddable spend, all LOBs	["BIG_WOW"]	1.08	UP	NEUTRAL	null	NEUTRAL	Paid Search - All	0.5	Warning
19	POSTPAID	Biddable spend, Postpaid	["BIG_WOW"]	1.05	UP	NEUTRAL	null	NEUTRAL	Paid Search - All	0.67	Warning
20	BYOD	Orders unassisted BYOD	["NEW_13WK_LOW"]	1	DOWN	BAD	null	NEUTRAL	Organic Search	0.79	Healthy

/* -------------------------------------------------------------------------------------------------
   19. What moved: rows blocked by the data quality gate in the latest week
   ------------------------------------------------------------------------------------------------- */
SELECT display_name, metric_lob, data_quality_issue, highlight_reasons
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long
WHERE  week_end = (SELECT MAX(week_end) FROM prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_whatMoved_long)
  AND  data_quality_issue IS NOT NULL
ORDER BY sort_order;
output:
display_name	metric_lob	data_quality_issue	highlight_reasons
MFC spend, Postpaid + Broadband	POSTPAID + BROADBAND	NO_VALUE	["QTD_OFF_PACE"]
MFC spend, Postpaid	POSTPAID	NO_VALUE	[]
MFC spend, Broadband	BROADBAND	NO_VALUE	["QTD_OFF_PACE"]
Biddable spend, Fiber	FIBER	NO_VALUE	[]
Digital % of postpaid activations	POSTPAID	NO_VALUE	[]


SELECT week_end, week_type, metric_key, week_value, target_week_value, vs_target_pct
FROM   prdrzranalytics.lab42.sdi_vw_weeklyNarrative_gold_factsLatest_long
WHERE  week_type = 'BOUNDARY_FIRST'
  AND  row_role  = 'ACTUAL'
  AND  target_week_value IS NOT NULL
ORDER BY week_end DESC, metric_key;
output:
week_end	week_type	metric_key	week_value	target_week_value	vs_target_pct
2026-10-03	BOUNDARY_FIRST	pulseTms.mfcSpendActualBroadband	10069747.640986279	16379848.931280285	-0.38523562193810756
2026-10-03	BOUNDARY_FIRST	pulseTms.mfcSpendActualPostpaid	24034685.26335907	29557764.896835327	-0.1868571474451236
2026-10-03	BOUNDARY_FIRST	pulseTms.mfcSpendActualTotal	34104432.90434535	45937613.82811561	-0.25759241583697357
2026-10-03	BOUNDARY_FIRST	pulseTms.qgpActivationsBopisActual	16250	17003.667681	-0.04432383031351239
2026-10-03	BOUNDARY_FIRST	pulseTms.qgpActivationsNewAalNoAssistanceActual	16318	14690.852818	0.11075920521144522
2026-10-03	BOUNDARY_FIRST	pulseTms.qgpStoreTrafficActual	3700347	3582063.9999989998	0.03302090638275401
2026-10-03	BOUNDARY_FIRST	pulseTms.qgpVrCallsActual	95392.00000100001	92215.560199	0.03444581147851067
2026-10-03	BOUNDARY_FIRST	pulseTms.qgpVrChatsActual	19002.999999	27624.636399	-0.3120995431567779
2026-10-03	BOUNDARY_FIRST	pulseTms.qgpVrPostpaidActivationsActual	67781.999998	72928.51977099999	-0.0705693710658104
2026-10-03	BOUNDARY_FIRST	pulseTms.upvTotalAdobe	1683543	1597531	0.05384058274925496
2026-07-04	BOUNDARY_FIRST	pulseTms.mfcSpendActualBroadband	9471715.791402817	9430434.320379257	0.004377472937206139
2026-07-04	BOUNDARY_FIRST	pulseTms.mfcSpendActualPostpaid	14707314.606905222	14312680.881673813	0.027572313565427553
2026-07-04	BOUNDARY_FIRST	pulseTms.mfcSpendActualTotal	24179030.39830804	23743115.20205307	0.018359646261467688
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpActivationsBopisActual	16199	22481.275090000003	-0.27944478526462446
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpActivationsNewAalNoAssistanceActual	13334	14697.063111	-0.09274391085521139
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpDigitalPctConsumerPostpaidActivationsTotalInclAssistedActual	0.9600409999999999	0.900954	null
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpStoreTrafficActual	3490143	3501659.492999	-0.003288867184837785
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpVrCallsActual	89509.99999899999	82535.8528	0.08449839630178269
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpVrChatsActual	24561	32562.958499	-0.2457380676650046
2026-07-04	BOUNDARY_FIRST	pulseTms.qgpVrPostpaidActivationsActual	76607.00000100001	55144.815642	0.38919677415067366
2026-07-04	BOUNDARY_FIRST	pulseTms.upvTotalAdobe	1510963	876000	0.7248436073059361


SELECT data_source, metric_name, metric_type, true_lob, qgp_date, week_type, days_in_period,
       metric_value, wow_numerator, wow_denominator
FROM   prdrzranalytics.lab42.sdi_vw_dashboardPulseTms_gold_unified_long
WHERE  channel_group <=> CASE WHEN data_source = 'QGP_SCORECARD' THEN NULL ELSE 'All Channels' END
  AND  (   qgp_date BETWEEN DATE'2026-06-27' AND DATE'2026-07-11'
        OR qgp_date BETWEEN DATE'2026-09-26' AND DATE'2026-10-10')
  AND  (   (data_source = 'ADOBE'        AND metric_name = 'upvTotalAdobe')
        OR (data_source = 'UPV_FORECAST')
        OR (metric_name IN ('mfcSpendActual', 'mfcSpendForecast') AND true_lob = 'POSTPAID')
        OR (data_source = 'QGP_SCORECARD' AND metric_name = 'digitalPctConsumerPostpaidActivationsTotalInclAssisted'))
ORDER BY data_source, metric_name, metric_type, qgp_date;
outpu:
data_source	metric_name	metric_type	true_lob	qgp_date	week_type	days_in_period	metric_value	wow_numerator	wow_denominator
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-06-27	NORMAL	7	1496566	1496566	1564176
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-06-30	BOUNDARY_STUB	3	647555.5714285715	null	null
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-07-04	BOUNDARY_FIRST	4	863407.4285714285	1510963	1496566
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-07-11	NORMAL	7	1460310	1460310	1510963
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-09-26	NORMAL	7	1865231	1865231	1884557
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-09-30	BOUNDARY_STUB	4	962024.5714285715	null	null
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-10-03	BOUNDARY_FIRST	3	721518.4285714285	1683543	1865231
ADOBE	upvTotalAdobe	ADOBE_VOLUME	null	2026-10-10	NORMAL	7	1611945	1611945	1683543
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-06-27	NORMAL	7	28727732.43356079	28727732.43356079	30227374.481093545
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-06-30	BOUNDARY_STUB	3	4017127.9395792484	null	null
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-07-04	BOUNDARY_FIRST	4	10690186.667325974	14707314.606905222	28727732.43356079
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-07-11	NORMAL	7	19602534.71313095	19602534.71313095	14707314.606905222
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-09-26	NORMAL	7	31979462.727539062	31979462.727539062	26025565.865753174
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-09-30	BOUNDARY_STUB	4	6880575.507011414	null	null
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-10-03	BOUNDARY_FIRST	3	17154109.756347656	24034685.26335907	31979462.727539062
MFC_SPEND_CHANNEL	mfcSpendActual	MFC_SPEND_ACTUAL	POSTPAID	2026-10-10	NORMAL	7	22648377.274780273	22648377.274780273	24034685.26335907
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-06-27	NORMAL	7	26714434.48070918	26714434.48070918	26888063.709953863
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-06-30	BOUNDARY_STUB	3	3622494.2143478394	null	null
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-07-04	BOUNDARY_FIRST	4	10690186.667325974	14312680.881673813	26714434.48070918
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-07-11	NORMAL	7	20340122.250782013	20340122.250782013	14312680.881673813
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-09-26	NORMAL	7	63343867.876953125	63343867.876953125	28994450.19416809
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-09-30	BOUNDARY_STUB	4	13497554.896835327	null	null
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-10-03	BOUNDARY_FIRST	3	16060210	29557764.896835327	63343867.876953125
MFC_SPEND_CHANNEL	mfcSpendForecast	MFC_SPEND_FORECAST	POSTPAID	2026-10-10	NORMAL	7	23122251.07421875	23122251.07421875	29557764.896835327
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-06-27	NORMAL	7	0.476254	0.476254	0.479893
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-06-30	BOUNDARY_STUB	3	0.467037	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-07-04	BOUNDARY_FIRST	4	0.493004	0.9600409999999999	0.476254
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-07-11	NORMAL	7	0.467811	0.467811	0.9600409999999999
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-09-26	NORMAL	7	0.53632	0.53632	0.547238
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-09-30	BOUNDARY_STUB	4	0.530301	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-10-03	BOUNDARY_FIRST	3	null	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_ACTUAL	POSTPAID	2026-10-10	NORMAL	7	null	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-06-27	NORMAL	7	0.552433	0.552433	0.54992
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-06-30	BOUNDARY_STUB	3	0.562352	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-07-04	BOUNDARY_FIRST	4	0.338602	0.900954	0.552433
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-07-11	NORMAL	7	0.321692	0.321692	0.900954
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-09-26	NORMAL	7	0.337051	0.337051	0.322195
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-09-30	BOUNDARY_STUB	4	0.309334	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-10-03	BOUNDARY_FIRST	3	null	null	null
QGP_SCORECARD	digitalPctConsumerPostpaidActivationsTotalInclAssisted	QGP_TARGET	POSTPAID	2026-10-10	NORMAL	7	null	null	null
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-06-27	NORMAL	7	null	null	null
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-06-30	BOUNDARY_STUB	3	null	null	null
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-07-04	BOUNDARY_FIRST	4	876000	876000	0
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-07-11	NORMAL	7	1460000	1460000	876000
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-09-26	NORMAL	7	1793592	1793592	1837371
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-09-30	BOUNDARY_STUB	4	841159	null	null
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-10-03	BOUNDARY_FIRST	3	756372	1597531	1793592
UPV_FORECAST	upvForecast	UPV_FORECAST	null	2026-10-10	NORMAL	7	1609303	1609303	1597531
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-06-27	NORMAL	7	null	null	null
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-06-30	BOUNDARY_STUB	3	null	null	null
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-07-04	BOUNDARY_FIRST	4	1027791.21	1027791.21	0
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-07-11	NORMAL	7	1919477.83	1919477.83	1027791.21
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-09-26	NORMAL	7	2395283.69	2395283.69	2333388.59
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-09-30	BOUNDARY_STUB	4	1025241.11	null	null
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-10-03	BOUNDARY_FIRST	3	151500	1176741.1099999999	2395283.69
UPV_FORECAST	upvWebAppForecast	UPV_FORECAST	null	2026-10-10	NORMAL	7	322400	322400	1176741.1099999999