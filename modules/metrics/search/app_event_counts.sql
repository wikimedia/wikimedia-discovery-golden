SELECT
  date, action, platform, COUNT(*) AS events
FROM (
  SELECT
    DATE('{from_timestamp}') AS date,
    CASE event_action WHEN 'click' THEN 'clickthroughs'
                      WHEN 'start' THEN 'search sessions'
                      WHEN 'results' THEN 'Result pages opened'
                      END AS action,
    CASE WHEN INSTR(userAgent, 'Android') > 0 THEN 'Android'
         ELSE 'iOS' END AS platform
  FROM MobileWikiAppSearch_10641988
  WHERE
    timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
    AND event_action IN ('click', 'start', 'results')
  UNION ALL
  SELECT
    DATE('{from_timestamp}') AS date,
    CASE event_action WHEN 'click' THEN 'clickthroughs'
                      WHEN 'start' THEN 'search sessions'
                      WHEN 'results' THEN 'Result pages opened'
                      END AS action,
    CASE WHEN INSTR(userAgent, 'Android') > 0 THEN 'Android'
         ELSE 'iOS' END AS platform
  FROM MobileWikiAppSearch_15729321
  WHERE
    timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
    AND event_action IN ('click', 'start', 'results')
) AS MobileWikiAppSearch
GROUP BY date, action, platform;
