-- Diagnostic only while the OLD analytical Gold02/03 rows are still present.
-- For counts the TEMP App hotfix converts the corrupted analytical ingredient back to the intended SUM:
-- correctedFourWeekSum = storedFourWeekTrendNumerator / fourWeekTrendWeekCount
-- final App average     = correctedFourWeekSum / fourWeekTrendWeekCount
SELECT
    targetWeekStartDate,metricName,breakoutType,breakoutValue,fourWeekTrendWeekCount,
    fourWeekTrendNumerator AS storedInflatedIngredient,
    try_divide(fourWeekTrendNumerator,cast(fourWeekTrendWeekCount AS DOUBLE)) AS correctedFourWeekSum,
    try_divide(fourWeekTrendNumerator,
               cast(fourWeekTrendWeekCount AS DOUBLE)*cast(fourWeekTrendWeekCount AS DOUBLE)) AS expectedCountFourWeekAverage
FROM prdrzranalytics.lab42.sdi_tbl_mip_gold_breakoutMetricIngredientsByWeek_long
WHERE targetWeekStartDate IN (DATE '2026-09-20',DATE '2026-09-27')
  AND metricKind='count'
  AND fourWeekTrendWeekCount>0
LIMIT 100;

output:
targetWeekStartDate	metricName	breakoutType	breakoutValue	fourWeekTrendWeekCount	storedInflatedIngredient	correctedFourWeekSum	expectedCountFourWeekAverage
2026-09-27	vrChats	authState	Ambiguous	3	656175	218725	72908.33333333333
2026-09-20	vrChats	authState	Ambiguous	2	290016	145008	72504
2026-09-27	ordersBase	authState	Network Authenticated	3	9582	3194	1064.6666666666667
2026-09-20	ordersBase	authState	Network Authenticated	2	3900	1950	975
2026-09-20	nbv	buyFlowStep	Checkout	2	2678862	1339431	669715.5
2026-09-27	nbv	buyFlowStep	Checkout	3	6065433	2021811	673937
2026-09-20	ordersAssisted	buyFlowStep	Did not enter buy flow	2	0	0	0
2026-09-27	ordersAssisted	buyFlowStep	Did not enter buy flow	3	6	2	0.6666666666666666
2026-09-27	storeLocator	buyFlowStep	Did not enter buy flow	3	170613	56871	18957
2026-09-20	storeLocator	buyFlowStep	Did not enter buy flow	2	73580	36790	18395
2026-09-20	storeLocator	campaign	0	2	0	0	0
2026-09-27	storeLocator	campaign	0	3	0	0	0
2026-09-27	pageViews	campaign	1.22E+13	3	5028	1676	558.6666666666666
2026-09-20	pageViews	campaign	1.22E+13	2	2670	1335	667.5
2026-09-20	vrCalls	campaign	1.22E+13	2	2	1	0.5
2026-09-27	vrCalls	campaign	1.22E+13	3	3	1	0.3333333333333333
2026-09-20	orders	campaign	19ADDALINE · 2019 Add a Line campaign	2	0	0	0
2026-09-27	orders	campaign	19ADDALINE · 2019 Add a Line campaign	3	0	0	0
2026-09-20	vrChats	campaign	19HOLMGTFR · 2019 Holiday Magenta Friday	2	0	0	0
2026-09-27	vrChats	campaign	19HOLMGTFR · 2019 Holiday Magenta Friday	3	0	0	0
2026-09-20	ordersBase	campaign	19IHEART5G · 2019 iHeart TFB Podcast 5G Network	2	0	0	0
2026-09-27	ordersBase	campaign	19IHEART5G · 2019 iHeart TFB Podcast 5G Network	3	0	0	0
2026-09-27	pageViews	campaign	19JDPOWER · 2019 JD POWER	3	2832	944	314.6666666666667
2026-09-20	pageViews	campaign	19JDPOWER · 2019 JD POWER	2	0	0	0
2026-09-20	sessionCount	campaign	1RU76COWBU42353	2	4	2	1
2026-09-27	sessionCount	campaign	1RU76COWBU42353	3	9	3	1
2026-09-20	vrChats	campaign	20COVID19 · 2020 Covid-19 Response	2	2	1	0.5
2026-09-27	vrChats	campaign	20COVID19 · 2020 Covid-19 Response	3	3	1	0.3333333333333333
2026-09-27	nbvBuyFlow	campaign	20FBPG · 2020 TFB Facebook Page	3	66	22	7.333333333333333
2026-09-20	nbvBuyFlow	campaign	20FBPG · 2020 TFB Facebook Page	2	26	13	6.5
2026-09-27	nbvConfigure	campaign	20FRBSMKT · 2020 TFB Forbes Content Marketing	3	0	0	0
2026-09-20	nbvConfigure	campaign	20FRBSMKT · 2020 TFB Forbes Content Marketing	2	0	0	0
2026-09-27	sessionCount	campaign	20NEWTMO · 2020 New TMO media/communication	3	81	27	9
2026-09-20	sessionCount	campaign	20NEWTMO · 2020 New TMO media/communication	2	46	23	11.5
2026-09-27	pageViews	campaign	20NURTURE · 2020 TFB Nurture	3	990	330	110
2026-09-20	pageViews	campaign	20NURTURE · 2020 TFB Nurture	2	584	292	146
2026-09-20	nbvCheckoutStart	campaign	212781859	2	0	0	0
2026-09-27	nbvCheckoutStart	campaign	212781859	3	0	0	0
2026-09-27	orderCount	campaign	21APPLNPI · 2021 Apple NPI	3	0	0	0
2026-09-20	orderCount	campaign	21APPLNPI · 2021 Apple NPI	2	0	0	0
2026-09-27	sessionCount	campaign	21IPH12OUS · 2021 iPhone 12 on us	3	3	1	0.3333333333333333
2026-09-20	sessionCount	campaign	21IPH12OUS · 2021 iPhone 12 on us	2	0	0	0
2026-09-27	vrCalls	campaign	22SG22FTMX · 2022 Samsung Galaxy S22 Free w/ Trade on Magenta Max	3	0	0	0
2026-09-27	ordersBase	campaign	23APPLNPI · 2023 Apple NPI	3	0	0	0
2026-09-20	ordersBase	campaign	23APPLNPI · 2023 Apple NPI	2	0	0	0
2026-09-20	orders	campaign	23FXWLBYPM · 2023 FoxWallaby Prospect Email Campaign	2	0	0	0
2026-09-27	orders	campaign	23FXWLBYPM · 2023 FoxWallaby Prospect Email Campaign	3	0	0	0
2026-09-27	nbv	campaign	23IP15PROT · 2023 iPhone 15 Pro On Us w/ Trade (On Go5G Plans)	3	9	3	1
2026-09-20	nbv	campaign	23IP15PROT · 2023 iPhone 15 Pro On Us w/ Trade (On Go5G Plans)	2	6	3	1.5
2026-09-27	vrCalls	campaign	23MBTA14FR · 2023 Metro Samsung Galaxy A14 5G Free w/ AAL	3	0	0	0
2026-09-20	vrCalls	campaign	23MBTA14FR · 2023 Metro Samsung Galaxy A14 5G Free w/ AAL	2	0	0	0
2026-09-20	nbv	campaign	23MTACPSCH · 2023 Metro ACP Awareness Back To School Campaign	2	4	2	1
2026-09-27	nbv	campaign	23MTACPSCH · 2023 Metro ACP Awareness Back To School Campaign	3	6	2	0.6666666666666666
2026-09-27	vrCalls	campaign	23MTCLACTR · 2023 Matricula Creators: International Talk Rate Reductions to LATAM	3	0	0	0
2026-09-20	vrCalls	campaign	23MTCLACTR · 2023 Matricula Creators: International Talk Rate Reductions to LATAM	2	0	0	0
2026-09-27	pageViews	campaign	23MTHINT24 · 2023 Metro HINT $24.99 Device at POS	3	96	32	10.666666666666666
2026-09-20	pageViews	campaign	23MTHINT24 · 2023 Metro HINT $24.99 Device at POS	2	34	17	8.5
2026-09-20	vrChats	campaign	23MTHINT24 · 2023 Metro HINT $24.99 Device at POS	2	0	0	0
2026-09-27	vrChats	campaign	23MTHINT24 · 2023 Metro HINT $24.99 Device at POS	3	0	0	0
2026-09-27	nbvCheckoutStart	campaign	23MTHINT49 · 2023 Metro HINT $49.99 Device at POS	3	0	0	0
2026-09-20	nbvCheckoutStart	campaign	23MTHINT49 · 2023 Metro HINT $49.99 Device at POS	2	0	0	0
2026-09-20	nbvConfigure	campaign	23SMBUPSLL · 2023 SMB Base Upsell Program	2	0	0	0
2026-09-27	nbvConfigure	campaign	23SMBUPSLL · 2023 SMB Base Upsell Program	3	0	0	0
2026-09-27	ordersUnassisted	campaign	23SMRAWIN · 2023 SMRA Win back - DM Campaign	3	0	0	0
2026-09-20	ordersUnassisted	campaign	23SMRAWIN · 2023 SMRA Win back - DM Campaign	2	0	0	0
2026-09-20	ordersAssisted	campaign	23SPRNTMIG · 2023 Sprint Migrator Nurture Campaign	2	0	0	0
2026-09-27	ordersAssisted	campaign	23SPRNTMIG · 2023 Sprint Migrator Nurture Campaign	3	0	0	0
2026-09-27	sessionCount	campaign	2497735954955	3	1371	457	152.33333333333334
2026-09-20	sessionCount	campaign	2497735954955	2	492	246	123
2026-09-27	pageViews	campaign	24APLNPIAV · 2024 Apple NPI Launch Alternative Value Prop	3	45	15	5
2026-09-20	pageViews	campaign	24APLNPIAV · 2024 Apple NPI Launch Alternative Value Prop	2	12	6	3
2026-09-20	ordersUnassisted	campaign	24HLIP4F25 · 2024 Q4 Holiday SOEP iPhone 16 4 for 25	2	0	0	0
2026-09-27	ordersUnassisted	campaign	24HLIP4F25 · 2024 Q4 Holiday SOEP iPhone 16 4 for 25	3	0	0	0
2026-09-20	ordersAssisted	campaign	24IPAD9P50 · 2024 50% Off iPad 9th Gen	2	0	0	0
2026-09-27	ordersAssisted	campaign	24IPAD9P50 · 2024 50% Off iPad 9th Gen	3	0	0	0
2026-09-20	nbv	campaign	24IPHONE16 · 2024 Q4 iPhone 16 Demand Gen	2	4600	2300	1150
2026-09-27	nbv	campaign	24IPHONE16 · 2024 Q4 iPhone 16 Demand Gen	3	10035	3345	1115
2026-09-20	nbv	campaign	24MTRAPWLC9	2	10	5	2.5
2026-09-27	nbv	campaign	24MTRAPWLC9	3	27	9	3
2026-09-27	orders	campaign	24NYYFLXIL · 2024 Nada Yada Yada Campaign: Metro Flex Island Longform	3	0	0	0
2026-09-20	nbvBuyFlow	campaign	24NYYP12NF · 2024 Nada Yada Yada Campaign: iPhone 12 $99.99 (no act fees)	2	4	2	1
2026-09-27	nbvBuyFlow	campaign	24NYYP12NF · 2024 Nada Yada Yada Campaign: iPhone 12 $99.99 (no act fees)	3	6	2	0.6666666666666666
2026-09-27	pageViews	campaign	24PRMDCVS · 2024 Premium Devices Direct Mail	3	60	20	6.666666666666667
2026-09-20	pageViews	campaign	24PRMDCVS · 2024 Premium Devices Direct Mail	2	0	0	0
2026-09-27	nbvBuyFlow	campaign	24RVVLTBUS · 2024 Revvl Tab 5G On Us w/ New or AAL	3	0	0	0
2026-09-20	orders	campaign	24SMGXYNPI · 2024 Samsung Galaxy NPI (S24 Eureka)	2	0	0	0
2026-09-27	orders	campaign	24SMGXYNPI · 2024 Samsung Galaxy NPI (S24 Eureka)	3	0	0	0
2026-09-20	ordersBase	campaign	24SMGXYNPI · 2024 Samsung Galaxy NPI (S24 Eureka)	2	0	0	0
2026-09-27	ordersBase	campaign	24SMGXYNPI · 2024 Samsung Galaxy NPI (S24 Eureka)	3	0	0	0
2026-09-27	ordersAcquisition	campaign	24WINBCKDM · 2024 Winback Direct Mail Regular and Cable Cos	3	0	0	0
2026-09-20	ordersAcquisition	campaign	24WINBCKDM · 2024 Winback Direct Mail Regular and Cable Cos	2	0	0	0
2026-09-27	vrChats	campaign	2500752552438	3	6	2	0.6666666666666666
2026-09-20	vrChats	campaign	2500752552438	2	4	2	1
2026-09-27	pageViews	campaign	251PI6PEIP	3	0	0	0
2026-09-27	ordersAcquisition	campaign	252STALNCH	3	0	0	0
2026-09-20	ordersAcquisition	campaign	252STALNCH	2	0	0	0
2026-09-20	storeLocator	campaign	252STALNCH	2	0	0	0
2026-09-27	storeLocator	campaign	252STALNCH	3	0	0	0
2026-09-20	nbv	campaign	253494331	2	2	1	0.5
2026-09-27	nbv	campaign	253494331	3	6	2	0.6666666666666666