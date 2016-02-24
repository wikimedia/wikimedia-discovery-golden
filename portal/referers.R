# Per-file config:
base_path <- paste0(write_root, "portal/")
check_dir(base_path)

main <- function(date = NULL){
  
  # Date handling
  if(is.null(date)){
    date <- Sys.Date() - 1
  }
  
  # Date subquery
  subquery <- date_clause(date)
  
  # Write query and run it
  query <- paste0("ADD JAR /home/ironholds/refinery-hive-0.0.21-SNAPSHOT.jar;
                  CREATE TEMPORARY FUNCTION is_external_search AS
                  'org.wikimedia.analytics.refinery.hive.IsExternalSearchUDF';
                  CREATE TEMPORARY FUNCTION classify_referer AS
                  'org.wikimedia.analytics.refinery.hive.RefererClassifyUDF';
                  CREATE TEMPORARY FUNCTION get_engine AS
                  'org.wikimedia.analytics.refinery.hive.IdentifySearchEngineUDF';
                  USE wmf;
                  SELECT
                    is_external_search(referer) AS is_search,
                    classify_referer(referer) AS referer_class,
                    get_engine(referer) as search_engine,
                    access_method,
                    COUNT(*) AS pageviews
                  FROM webrequest", subquery, "
                    AND webrequest_source IN('text','mobile')
                    AND content_type RLIKE('^text/html')
                    AND uri_host IN('www.wikipedia.org','wikipedia.org')
                    AND access_method IN('desktop','mobile web')
                  GROUP BY
                    is_external_search(referer),
                    classify_referer(referer),
                    get_engine(referer),
                    access_method;")
  results <- query_hive(query)
  
  # Sanitise the resulting data
  results <- results[!is.na(results$pageviews), ]
  results$date <- date
  results <- results[, c("date", "is_search", "referer_class", "search_engine", "access_method", "pageviews")]
  results$is_search <- results$is_search == "true"
  
  # Write out
  conditional_write(results, file.path(base_path, "portal_referer_data.tsv"))
  
  # Mobile vs Desktop Visitors
  results <- data.table(results)
  results_by_platform <- results[, j = list(pageviews = sum(pageviews)),
                                   by = c("date", "access_method")]
  
  # Write out
  conditional_write(results, file.path(base_path, "portal_platform_data.tsv"))
  
  return(invisible())
}

