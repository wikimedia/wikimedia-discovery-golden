SELECT
  date, event_source,
  ROUND(SUM(pow_1)/COUNT(1), 3) AS pow_1,
  ROUND(SUM(pow_2)/COUNT(1), 3) AS pow_2,
  ROUND(SUM(pow_3)/COUNT(1), 3) AS pow_3,
  ROUND(SUM(pow_4)/COUNT(1), 3) AS pow_4,
  ROUND(SUM(pow_5)/COUNT(1), 3) AS pow_5,
  ROUND(SUM(pow_6)/COUNT(1), 3) AS pow_6,
  ROUND(SUM(pow_7)/COUNT(1), 3) AS pow_7,
  ROUND(SUM(pow_8)/COUNT(1), 3) AS pow_8,
  ROUND(SUM(pow_9)/COUNT(1), 3) AS pow_9
FROM (
  SELECT
    DATE('{from_timestamp}') AS date,
    event_searchSessionId,
    event_source,
    SUM(IF(event_action = 'click', POW(0.1, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_1,
    SUM(IF(event_action = 'click', POW(0.2, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_2,
    SUM(IF(event_action = 'click', POW(0.3, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_3,
    SUM(IF(event_action = 'click', POW(0.4, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_4,
    SUM(IF(event_action = 'click', POW(0.5, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_5,
    SUM(IF(event_action = 'click', POW(0.6, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_6,
    SUM(IF(event_action = 'click', POW(0.7, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_7,
    SUM(IF(event_action = 'click', POW(0.8, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_8,
    SUM(IF(event_action = 'click', POW(0.9, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_9
  FROM TestSearchSatisfaction2_16270835
  WHERE
    timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
    AND event_action IN ('searchResultPage', 'click')
    AND IF(event_source = 'autocomplete', event_inputLocation = 'header', TRUE)
    AND IF(event_source = 'autocomplete' AND event_action = 'click', event_position >= 0, TRUE)
  GROUP BY date, event_searchSessionId, event_source
) AS pows
GROUP BY date, event_source;
