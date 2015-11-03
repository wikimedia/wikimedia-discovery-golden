# Per-file config:
base_path <- paste0(write_root, "search/")

# Retrieves data for the mobile web stuff we care about, drops it in the public-datasets directory.
# Should be run on stat1002, /not/ on the datavis machine.
main <- function(date = NULL, table = "MobileWebSearch_12054448"){
  
  # Get data and format the timestamps
  data <- query_func(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                     CASE event_action WHEN 'click-result' THEN 'clickthroughs'
                     WHEN 'session-start' THEN 'search sessions'
                     WHEN 'impression-results' THEN 'Result pages opened' END AS action,
                     event_clickIndex AS click_index,
                     event_numberOfResults AS result_count,
                     event_resultSetType as result_type,
                     event_timeOffsetSinceStart AS time_offset,
                     event_timeToDisplayResults AS load_time,
                     event_platformVersion AS version",
                     date = date,
                     table = table,
                     conditionals = "event_action IN ('click-result','session-start','impression-results')")
  data$date <- lubridate::ymd(data$date)
  
  # Convert it into event aggregates
  mobile_results <- data[,j = list(events = .N), by = c("date", "action")]
  
  # Process load times
  load_times <- data[data$action == "Result pages opened",{
    output <- data.frame(t(quantile(load_time, c(0.5, 0.95, 0.99))))
    names(output) <- c("Median", "95th percentile", "99th Percentile")
    output
  }, by = "date"]
  
  # Write out and return
  conditional_write(mobile_results, file.path(base_path, "mobile_event_counts.tsv"))
  conditional_write(load_times, file.path(base_path, "mobile_load_times.tsv"))
  
  return(invisible())
}
