SET enable_nereids_planner=TRUE;
SET enable_fallback_to_original_planner=FALSE;
SELECT CounterID, count(distinct UserID) FROM hits WHERE CAST(0 AS BOOLEAN) AND CounterID = 1704509 GROUP BY CounterID
