# Per-file config:
base_path <- paste0(write_root, "search/")

main <- function(date = NULL, table = "TestSearchSatisfaction2_14098806"){
  
  # Retrieve data
  data <- as.data.frame(query_func(fields = "
                        SELECT event_searchSessionId AS session_id,
                        timestamp",
                        date = date,
                        table = table,
                        conditionals = "event_action IN('searchResultPage','visitPage')"))
  data$timestamp <- lubridate::ymd_hms(data$timestamp)
  
  # Generate the data
  if(is.null(date)){
    date <- as.Date(data$timestamp[1])
  }

  dwell_data <- ortiz::dwell_time(data = data, ids = "session_id", timestamps = "timestamp", dwell_threshold = 10)
  
  # Turn it into a data.frame we can write out conditionally, and then do that
  output <- data.frame(date = date, threshold_pass = sum(dwell_data)/length(dwell_data))
  conditional_write(output, file = file.path(base_path, "search_threshold_pass_rate.tsv"))
  
  return(invisible())
}
