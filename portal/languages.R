base_path <- paste0(write_root, "portal/")

main <- function(date = NULL, table = "WikipediaPortal_14377354"){
  
  # Read
  data <- wmf::build_query(fields = "SELECT
                             SUBSTRING(timestamp, 1, 8) AS date,
                             event_session_id AS session,
                             event_destination AS destination,
                             event_event_type AS type,
                             event_section_used AS section_used",
                           date = date,
                           table = table,
                           conditionals = "((event_cohort IS NULL) OR (event_cohort IN ('null','baseline')))
                           AND event_country != 'US'
                           AND (event_section_used IN('primary links', 'secondary links')
                                OR (event_section_used = 'search'
                                    AND (
                                         INSTR(event_destination, '.wikipedia.org/')
                                         AND NOT INSTR(event_destination, 'wikipedia.org/search-redirect.php')
                                        )
                                   )
                               )
                           AND NOT INSTR(event_destination, 'translate.googleusercontent.com')")
  
  # Sanitise
  data$date <- as.Date(lubridate::ymd(data$date))
  data <- data.table::as.data.table(data)
  
  # Extract the prefix
  data$prefix <- sub("^https?://(.*)\\.wikipedia\\.org.*", "\\1", data$destination)
  
  data <- data[order(data$date, data$prefix),
               list(
                 clicks = .N, sessions = length(unique(session)),
                 search = sum(section_used == "search"),
                 primary = sum(section_used == "primary links"),
                 secondary = sum(section_used == "secondary links")),
               by = c("date", "prefix")]
  
  wmf::write_conditional(data, file.path(base_path, "language_destination.tsv"))
  
  return(invisible())
  
}
