SELECT
  DATE('{from_timestamp}') AS date,
  event_feature AS feature,
  event_action AS action,
  COUNT(*) AS events
FROM GeoFeatures_12914994
WHERE timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
GROUP BY date, feature, action;
