# Per-file config:
base_path <- paste0(write_root, "search/")

main <- function(date = NULL) {
  
  # Ensure we have a date and deconstruct it into a MW-friendly format
  if (is.null(date)) {
    date <- Sys.Date() - 1
  }
  date <- gsub(x = date, pattern = "-", replacement = "")
  
  query <- paste0("SELECT
  date, event_source,
  ", paste0("ROUND(SUM(pow_", 1:9,")/COUNT(1), 3) AS pow_", 1:9, collapse = ",\n  "), "
FROM (
  SELECT
    LEFT(timestamp, 8) AS date,
    event_source,
    event_searchSessionId,
    ", paste0("SUM(IF(event_action = 'click', POW(0.", 1:9, ", event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_", 1:9, collapse = ",\n    "), "
  FROM TestSearchSatisfaction2_15700292
  WHERE
    LEFT(timestamp, 8) = '", date, "'
    AND event_action IN ('searchResultPage', 'click')
    AND (event_subTest IS NULL OR event_subTest IN ('null','baseline'))
  GROUP BY date, event_source, event_searchSessionId
) AS pows
GROUP BY date, event_source;") # cat(query) if you want to copy and paste into MySQL CLI
  # See https://phabricator.wikimedia.org/T144424 for more details.
  data <- wmf::mysql_read(query, "log")
  
  # Report
  wmf::write_conditional(paul_scores, file.path(base_path, "paulscore_approximations.tsv"))
  
  return(invisible())
}
