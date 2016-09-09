base_path <- paste0(write_root, "portal/")

main <- function(date = NULL){

  # Date subquery
  clause_data <- wmf::date_clause(date)

  # Query
  data <- wmf::query_hive(paste("USE wmf;
                                 SELECT
                                   client_ip,
                                   COUNT(1) AS pageviews
                                 FROM webrequest",
                                 clause_data$date_clause, "
                                   AND uri_host RLIKE('^(www\\.)?wikipedia.org/*$')
                                   AND INSTR(uri_path, 'search-redirect.php') = 0
                                   AND content_type RLIKE('^text/html')
                                   AND webrequest_source = 'text'
                                   AND NOT (referer RLIKE('^http://localhost'))
                                   AND agent_type = 'user'
                                   AND referer_class != 'unknown'
                                   AND http_status IN('200', '304')
                                 GROUP BY client_ip;"))
  data <- data.table::as.data.table(data[!is.na(data$pageviews), ])
  `99.99th percentile` <- floor(quantile(data$pageviews, 0.9999))
  data$client_type <- ifelse(data$pageviews < `99.99th percentile`, "low_volume", "high_volume")

  output <- cbind(date = clause_data$date,
                  tidyr::spread(data[, list(pageviews = sum(pageviews)), by = "client_type"],
                                client_type, pageviews),
                  threshold = `99.99th percentile`)
  output$pageviews = output$high_volume + output$low_volume

  #Return!
  wmf::write_conditional(output[, c("date", "pageviews", "high_volume", "low_volume", "threshold")],
                         file.path(base_path, "portal_pageviews.tsv"))

}
