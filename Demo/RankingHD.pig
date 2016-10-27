-------------------------------------------------------------------------------------------------------------------
-- Use this script to rank wifi hotspot temperature according to 'WifiStatus_20K.csv'.
-- Data downloaded from 'data.gov.au'.
--
-- Date:	2016/3/1
-- Author:	Trams Wang
-- Version:	1.1
-------------------------------------------------------------------------------------------------------------------
register DemoUDF.jar;

DEFINE Clean		test.CleanByRep('19');
DEFINE ConvertTime	test.ConvertTime();
DEFINE CalDensity	test.CalculateDensity();

--set default_parallel 16;

raw = LOAD 'Data/WifiStatusLarge_50.csv' USING PigStorage(',');

cleaned = FILTER (FOREACH raw GENERATE FLATTEN(Clean(*))) BY NOT ($0 MATCHES '');

named = FOREACH cleaned GENERATE (chararray)$1 AS LocationID:chararray,
				 (chararray)$3 AS FirstAccess:chararray,
				 (chararray)$4 AS LastAccess:chararray,
				 (int)$5 AS AccessCount:int
				 ;

timed = FOREACH named GENERATE LocationID, ConvertTime(*) AS Duration;

timed_grouped = GROUP timed BY LocationID;

timed_summed = FOREACH timed_grouped GENERATE group AS LocationID, SUM(timed.Duration) AS TotalDuration;

timed_ordered = ORDER timed_summed BY TotalDuration DESC;
STORE timed_ordered INTO 'timed_ordered' USING PigStorage();
---------------------------------------------------------------------------------------------------
access_grouped = GROUP named BY LocationID;

access_summed = FOREACH access_grouped GENERATE group AS LocationID, SUM(named.AccessCount) AS TotalAccesses;

access_ordered = ORDER access_summed BY TotalAccesses DESC;
STORE access_ordered INTO 'access_ordered' USING PigStorage();

---------------------------------------------------------------------------------------------------
density = JOIN timed_summed BY LocationID, access_summed BY LocationID;

density_scored = FOREACH density GENERATE timed_summed::LocationID AS LocationID,
	CalDensity(access_summed::TotalAccesses, timed_summed::TotalDuration) AS Density:double;

density_ordered = ORDER density_scored BY Density DESC;
STORE density_ordered INTO 'density_ordered' USING PigStorage();
