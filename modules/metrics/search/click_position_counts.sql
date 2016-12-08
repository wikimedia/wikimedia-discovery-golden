SELECT
  DATE('{from_timestamp}') AS date,
  CASE WHEN (event_position + 1) < 10 THEN (event_position + 1)
       WHEN (event_position + 1) >= 10 AND (event_position + 1) < 20 THEN '10-19'
       WHEN (event_position + 1) >= 20 AND (event_position + 1) <= 100 THEN '20-100'
       WHEN (event_position + 1) > 100 THEN '100+'
       END AS click_position,
  COUNT(*) AS events
FROM MobileWikiAppSearch_15729321
WHERE
  timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
  AND event_action = 'click'
GROUP BY date, click_position;
