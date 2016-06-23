base_path <- paste0(write_root, "portal/")

main <- function(date = NULL){
  
  # Date subquery
  clause_data <- wmf::date_clause(date)

  # Query
  data <- wmf::query_hive(paste("USE wmf;
                                 SELECT COUNT(*) AS pageviews
                                 FROM webrequest",
                                 clause_data$date_clause, 
                                "AND uri_host RLIKE('^(www\\.)?wikipedia.org/*$')
                                 AND INSTR(uri_path, 'search-redirect.php') = 0
                                 AND content_type RLIKE('^text/html')
                                 AND webrequest_source = 'text'
                                 AND NOT (referer RLIKE('^http://localhost'));"))
      
  output <- data.frame(date = clause_data$date, pageviews = data$pageviews)
  
  #Return!
  wmf::write_conditional(output, file.path(base_path, "portal_pageviews.tsv"))
}
