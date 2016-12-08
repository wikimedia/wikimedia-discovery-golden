SELECT
  DATE('{from_timestamp}') AS date,
  CASE event_action WHEN 'click-result' THEN 'clickthroughs'
                    WHEN 'session-start' THEN 'search sessions'
                    WHEN 'impression-results' THEN 'Result pages opened'
                    END AS action,
  COUNT(*) AS events
FROM MobileWebSearch_12054448
WHERE
  timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
  AND event_action IN('click-result', 'session-start', 'impression-results')
GROUP BY date, action;
