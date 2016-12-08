SELECT
  DATE('{from_timestamp}') AS `date`,
  CASE WHEN (
         INSTR(userAgent, 'Android') > 0
         OR INSTR(userAgent, 'Mobile') > 0
         OR INSTR(userAgent, 'iOS') > 0
         OR INSTR(userAgent, 'Phone') > 0
       ) THEN 'Mobile'
       ELSE 'Desktop' END AS device,
  CASE WHEN INSTR(event_destination, 'itunes.apple.com') > 0 THEN 'iOS app link'
       WHEN INSTR(event_destination, 'play.google.com') > 0 THEN 'Android app link'
       ELSE 'List of apps' END AS clicked,
  COUNT(*) AS clicks
FROM WikipediaPortal_15890769
WHERE
  timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
  AND event_cohort = 'baseline'
  AND event_event_type = 'clickthrough'
  AND event_section_used = 'other projects'
  AND (
    INSTR(event_destination, 'itunes.apple.com') > 0
    OR INSTR(event_destination, 'play.google.com') > 0
    OR event_destination = 'https://en.wikipedia.org/wiki/List_of_Wikipedia_mobile_applications'
  )
GROUP BY `date`, device, clicked;
