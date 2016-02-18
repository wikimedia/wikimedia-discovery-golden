base_path <- paste0(write_root, "portal/")

main <- function(date = NULL){
  
  # Date handling
  if(is.null(date)){
    date <- Sys.Date() - 1
  }
  
  # Date subquery
  subquery <- date_clause(date)
  
  # Query
  data <- query_hive(paste0("USE wmf;
                             SELECT COUNT(*) AS pageviews
                             FROM webrequest
                            ", subquery, 
                            "AND uri_host IN('www.wikipedia.org', 'wikipedia.org')
                             AND content_type RLIKE('^text/html')
                             AND webrequest_source = 'text'"))
  
  output <- data.frame(date = date, pageviews = data$pageviews)
  
  #Return!
  conditional_write(output, file.path(base_path, "portal_pageviews.tsv"))
}
