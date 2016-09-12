base_path <- paste0(write_root, "portal/")

main <- function(date = NULL, table = "WikipediaPortal_15890769"){
  
  # Read
  data <- wmf::build_query(fields = "SELECT
                             SUBSTRING(timestamp, 1, 8) AS date,
                             event_session_id AS session,
                             event_destination AS destination,
                             event_event_type AS type,
                             event_section_used AS section_used,
                             event_selected_language AS selected_language",
                           date = date,
                           table = table,
                           conditionals = "((event_cohort IS NULL) OR (event_cohort IN ('null','baseline')))
                           AND event_country != 'US'
                           AND NOT INSTR(event_destination, 'translate.googleusercontent.com')")
  
  # Sanitise
  data$date <- as.Date(lubridate::ymd(data$date))
  data <- data.table::as.data.table(data)
  data2 <- data[type %in% c("landing", "select-language"), ]
  data <- data[type == "clickthrough", ]
  
  # Extract the prefix
  data$prefix <- sub("^https?://(.*)\\.wikipedia\\.org.*", "\\1", data$destination)
  data$prefix[data$section_used == "search" && grepl("search-redirect.php", data$destination, fixed = TRUE)] <- data$selected_language
  
  # Update the internal dataset of prefixes and languages
  if (lubridate::wday(lubridate::today(), label = TRUE, abbr = FALSE) == "Friday") {
    polloi::update_prefixes()
  }
  
  # Only keep the data with valid prefixes
  data <- data[prefix %in% polloi::get_prefixes()$prefix, ]
  
  # Aggregate
  data <- data[order(data$date, data$prefix),
               list(
                 clicks = .N, sessions = length(unique(session)),
                 search = sum(section_used == "search"),
                 primary = sum(section_used == "primary links"),
                 secondary = sum(section_used == "secondary links")),
               by = c("date", "prefix")]
  
  # Language switching by session
  data2 <- data2[, list(
    landings = sum(type == "landing"),
    switches = sum(type == "select-language")),
    by = c("date", "session")]
  data2 <- data2[landings > 0, list(
    sessions = .N,
    switched = sum(switches > 0)
  ), by = c("date")]
  
  wmf::write_conditional(data, file.path(base_path, "language_destination.tsv"))
  wmf::write_conditional(data2, file.path(base_path, "language_switching.tsv"))
  
  return(invisible())
  
}
