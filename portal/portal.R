base_path <- paste0(write_root, "portal/")

main <- function(date = NULL, table = "WikipediaPortal_14377354"){
  
  # Read
  data <- query_func(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                     event_session_id AS session,
                     event_destination AS destination,
                     event_event_type AS type,
                     event_section_used AS section_used",
                     date = date,
                     table = table,
                     conditionals = "event_cohort IS NULL")
  
  # Sanitise
  data$section_used[is.na(data$section_used)] <- "no action"
  data$date <- as.Date(ymd(data$date))
  data <- as.data.table(data)
  
  # Generate clickthrough rate data
  clickthrough_data <- data[,j=list(events=.N),by = c("date","type")]
  
  # Generate click breakdown
  data <- data[order(data$type, decreasing = FALSE),]
  data <- data[!duplicated(data$session),]
  breakdown_data <- data[,j=list(events=.N),by = c("date","section_used")]
  
  conditional_write(clickthrough_data, file.path(base_path, "clickthrough_rate.tsv"))
  conditional_write(breakdown_data, file.path(base_path, "clickthrough_breakdown.tsv"))
  
  return(invisible())
}
