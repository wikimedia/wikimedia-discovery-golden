# Per-file config:
base_path <- paste0(write_root, "search/")

# Central function
main <- function(date = NULL){

  # Date handling
  if(is.null(date)){
    date <- Sys.Date() - 1
  }
  
  # Date subquery
  subquery <- date_clause(date)

  # Write query and run it
  query <- paste0("ADD JAR /srv/deployment/analytics/refinery/artifacts/refinery-hive.jar;
                   CREATE TEMPORARY FUNCTION search_classify AS
                  'org.wikimedia.analytics.refinery.hive.SearchClassifierUDF';
                   USE wmf;
                   SELECT year, month, day, search_classify(uri_path, uri_query) AS event_type,
                   COUNT(*) AS search_events
                   FROM webrequest
                  ", subquery,
                  "AND webrequest_source IN('text','mobile') AND http_status = '200'
                   GROUP BY year, month, day, search_classify(uri_path, uri_query);")
  results <- query_hive(query)

  # Filter and reformat
  results <- results[complete.cases(results),]
  results <- results[results$event_type %in% c("language","cirrus","prefix","geo","open"),]
  output <- data.frame(date = as.Date(paste(results$year, results$month, results$day, sep = "-")),
                       event_type = results$event_type,
                       events = results$search_events,
                       stringsAsFactors = FALSE)

  # Write out
  conditional_write(output, file.path(base_path, "search_api_aggregates.tsv"))
  
  return(invisible())
}
