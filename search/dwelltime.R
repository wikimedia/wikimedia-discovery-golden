# Per-file config:
base_path <- paste0(write_root, "search/")

main <- function(date = NULL, table = "TestSearchSatisfaction2_14098806"){
  
  # Retrieve data
  data <- wmf::build_query(fields = "
                           SELECT event_searchSessionId AS session_id,
                           timestamp",
                           date = date,
                           table = table,
                           conditionals = "event_action IN('searchResultPage','visitPage') AND event_subTest IS NULL")
  data$timestamp <- lubridate::ymd_hms(data$timestamp)
  
  # Generate the data
  if(is.null(date)){
    date <- as.Date(data$timestamp[1])
  }

  dwell_data <- ortiz::dwell_time(data = data, id_col = "session_id", ts_col = "timestamp", dwell_threshold = 10)
  
  # Turn it into a data.frame we can write out conditionally, and then do that
  output <- data.frame(date = date, threshold_pass = sum(dwell_data)/length(dwell_data))
  wmf::write_conditional(output, file = file.path(base_path, "search_threshold_pass_rate.tsv"))
  
  return(invisible())
}
