SELECT
  DATE('{from_timestamp}') AS date,
  event_feature AS feature,
  COUNT(DISTINCT(event_userToken))*100 AS users
FROM GeoFeatures_12914994
WHERE timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
GROUP BY date, feature;
