# Per-file config:
base_path <- paste0(write_root, "search/")

# Retrieves data for the mobile web stuff we care about, drops it in the aggregate-datasets directory. Should be run on stat1002, /not/ on the datavis machine.

main <- function(date = NULL, table = "MobileWikiAppSearch_10641988"){

  # Retrieve data using the query builder in ./common.R
  data <- wmf::build_query(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                           CASE event_action WHEN 'click' THEN 'clickthroughs'
                           WHEN 'start' THEN 'search sessions'
                           WHEN 'results' THEN 'Result pages opened' END AS action,
                           event_timeToDisplayResults AS load_time,
                           userAgent",
                           date = date,
                           table = table,
                           conditionals = "event_action IN ('click','start','results')")
  data <- data.table::as.data.table(data)
  data$date <- lubridate::ymd(data$date)
  data$platform[grepl(x = data$userAgent, pattern = "Android", fixed = TRUE)] <- "Android"
  data$platform[is.na(data$platform)] <- "iOS"
  data <- data[,userAgent := NULL,]

  # Generate aggregates
  app_results <- data[,j = list(events = .N), by = c("date", "action", "platform")]

  # Produce load time data
  load_times <- data[data$action == "Result pages opened", {
    output <- data.frame(t(quantile(load_time, c(0.5, 0.95, 0.99))))
    names(output) <- c("Median", "95th percentile", "99th Percentile")
    output
  }, by = c("date", "platform")]
  
  # Write out
  wmf::write_conditional(app_results, file.path(base_path, "app_event_counts.tsv"))
  wmf::write_conditional(load_times, file.path(base_path, "app_load_times.tsv"))
  
  return(invisible())
}
