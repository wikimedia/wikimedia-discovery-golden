SELECT
  DATE('{from_timestamp}') AS date,
  CASE event_source WHEN 0 THEN 'Main article toolbar'
                    WHEN 1 THEN 'Widget'
                    WHEN 2 THEN 'Share intent'
                    WHEN 3 THEN 'Process-text intent'
                    WHEN 4 THEN 'Floating search bar in the feed'
                    WHEN 5 THEN 'Voice search query'
                    END AS invoke_source,
  COUNT(*) AS events
FROM MobileWikiAppSearch_15729321
WHERE
  timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
  AND event_action = 'start'
GROUP BY date, invoke_source;
