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
                    COUNT(*) AS pageviews
                  FROM webrequest", subquery, "
                    AND webrequest_source = 'text'
                    AND content_type RLIKE('^text/html')
                    AND uri_host IN('www.wikipedia.org','wikipedia.org')
                  GROUP BY
                    is_external_search(referer),
                    classify_referer(referer),
                    get_engine(referer);")
  results <- query_hive(query)
  
  # Sanitise the resulting data
  results <- results[!is.na(results$pageviews), ]
  results$date <- date
  results <- results[, c("date", "is_search", "referer_class", "search_engine", "pageviews")]
  results$is_search <- results$is_search == "true"
  
  # Write out
  conditional_write(results, file.path(base_path, "portal_referer_data.tsv"))
  
  return(invisible())
}

