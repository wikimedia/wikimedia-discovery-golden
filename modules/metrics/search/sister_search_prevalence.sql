SELECT
  DATE('{from_timestamp}') AS date, wiki_id,
  SUM(has_iw) AS has_sister_results,
  SUM(IF(has_iw, 0, 1)) AS no_sister_results
FROM (
  SELECT DISTINCT
    wiki_id, session_id, query_hash, has_iw
  FROM (
    SELECT DISTINCT
      wiki AS wiki_id,
      event_uniqueId AS event_id,
      event_searchSessionId AS session_id,
      MD5(LOWER(TRIM(event_query))) AS query_hash,
      INSTR(event_extraParams, '"iw":') > 0 AS has_iw -- sister project results shown
    FROM TestSearchSatisfaction2_16909631
    WHERE timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
      AND event_source = 'fulltext'
      AND event_action = 'searchResultPage'
      AND event_subTest IS NULL
  ) AS events
) AS searches
GROUP BY date, wiki_id
HAVING wiki_id RLIKE 'wiki$'
   AND has_sister_results > 0
   AND no_sister_results > 0
ORDER BY date, wiki_id;
