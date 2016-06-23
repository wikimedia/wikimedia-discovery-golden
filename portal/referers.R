# Per-file config:
base_path <- paste0(write_root, "portal/")
check_dir(base_path)

main <- function(date = NULL){
  
  # Date subquery
  clause_data <- wmf::date_clause(date)

  # Write query and run it
  query <- paste("ADD JAR /home/bearloga/Code/analytics-refinery-jars/refinery-hive.jar;
                  CREATE TEMPORARY FUNCTION is_external_search AS
                  'org.wikimedia.analytics.refinery.hive.IsExternalSearchUDF';
                  CREATE TEMPORARY FUNCTION classify_referer AS
                  'org.wikimedia.analytics.refinery.hive.SmartReferrerClassifierUDF';
                  CREATE TEMPORARY FUNCTION get_engine AS
                  'org.wikimedia.analytics.refinery.hive.IdentifySearchEngineUDF';
                  USE wmf;
                  SELECT
                    is_external_search(referer) AS is_search,
                    classify_referer(referer) AS referer_class,
                    get_engine(referer) as search_engine,
                    COUNT(*) AS pageviews
                  FROM webrequest",
                  clause_data$date_clause,
                 "  AND webrequest_source = 'text'
                    AND content_type RLIKE('^text/html')
                    AND uri_host RLIKE('^(www\\.)?wikipedia.org/*$')
                    AND (
                      INSTR(uri_path, 'search-redirect.php') = 0
                      OR
                      NOT referer RLIKE('^(https?://www\\.)?wikipedia.org/+search-redirect.php')
                    )
                    AND NOT referer RLIKE('^http://localhost')
                  GROUP BY
                    is_external_search(referer),
                    classify_referer(referer),
                    get_engine(referer);")
  results <- wmf::query_hive(query)
  
  # Sanitise the resulting data
  results <- results[!is.na(results$pageviews) & results$search_engine != "unknown", ]
  results <- results[!(results$referer_class == "external (search engine)" & results$search_engine == "none"), ]
  results$date <- clause_data$date
  results <- results[, c("date", "is_search", "referer_class", "search_engine", "pageviews")]
  results$is_search <- results$is_search == "true"
  
  # Write out
  wmf::write_conditional(results, file.path(base_path, "portal_referer_data.tsv"))
  
  return(invisible())
}

