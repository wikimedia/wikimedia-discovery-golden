# Per-file config:
base_path <- paste0(write_root, "search/")

# Retrieves data for the desktop stuff we care about, drops it in the aggregate-datasets directory.
# Should be run on stat1002, /not/ on the datavis machine.
main <- function(date = NULL, table = "Search_14361785"){
  # - Search_12057910 stopped collecting data on October 2nd
  # - Use Search_14361785 as of October 28th
  
  # Get data and format
  data <- query_func(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                     CASE event_action WHEN 'click-result' THEN 'clickthroughs'
                     WHEN 'session-start' THEN 'search sessions'
                     WHEN 'impression-results' THEN 'Result pages opened'
                     WHEN 'submit-form' THEN 'Form submissions' END AS action,
                     event_clickIndex AS click_index,
                     event_numberOfResults AS result_count,
                     event_resultSetType as result_type,
                     event_timeOffsetSinceStart AS time_offset,
                     event_timeToDisplayResults AS load_time",
                     date = date,
                     table = table,
                     conditionals = "event_action IN ('click-result','session-start','impression-results', 'submit-form')")
  data$date <- lubridate::ymd(data$date)
  
  # Generate aggregates
  event_data <- data[,j = list(events = .N), by = c("date", "action")]
  
  # Generate load time data and save that
  load_times <- data[data$action == "Result pages opened", {
    output <- data.frame(t(quantile(load_time, c(0.5, 0.95, 0.99))))
    names(output) <- c("Median", "95th percentile", "99th Percentile")
    output
  }, by = "date"]
  
  # Write out
  conditional_write(event_data, file.path(base_path, "desktop_event_counts.tsv"))
  conditional_write(load_times, file.path(base_path, "desktop_load_times.tsv"))
  
  return(invisible())
}
