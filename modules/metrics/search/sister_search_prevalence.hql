USE event;
WITH deduplicated AS (
  SELECT DISTINCT
    wiki AS wiki_id,
    event.uniqueId AS event_id,
    event.searchSessionId AS session_id,
    MD5(LOWER(TRIM(event.query))) AS query_hash,
    INSTR(event.extraParams, '"iw":') > 0 AS has_iw -- sister project results shown
  FROM TestSearchSatisfaction2
  WHERE CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= '${hiveconf:start_date}'
    AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < '${hiveconf:end_date}'
    AND event.source = 'fulltext'
    AND event.action = 'searchResultPage'
    AND event.subTest IS NULL
    AND wiki RLIKE 'wiki$'
)
SELECT
  '${hiveconf:start_date}' AS date, wiki_id,
  SUM(IF(has_iw, 1, 0)) AS has_sister_results,
  SUM(IF(has_iw, 0, 1)) AS no_sister_results
FROM deduplicated
GROUP BY '${hiveconf:start_date}', wiki_id
HAVING has_sister_results > 0 AND no_sister_results > 0
ORDER BY '${hiveconf:end_date}', wiki_id
LIMIT 100000;
