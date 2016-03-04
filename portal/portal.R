base_path <- paste0(write_root, "portal/")

main <- function(date = NULL, table = "WikipediaPortal_14377354"){
  
  # Read
  data <- wmf::build_query(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                           event_session_id AS session,
                           event_country AS country,
                           event_destination AS destination,
                           event_event_type AS type,
                           event_section_used AS section_used,
                           timestamp AS ts,
                           userAgent AS user_agent",
                           date = date,
                           table = table,
                           conditionals = "((event_cohort IS NULL) OR (event_cohort IN ('null','baseline')))")

  # Sanitise
  data$section_used[is.na(data$section_used)] <- "no action"
  data$date <- as.Date(lubridate::ymd(data$date))
  data <- data.table::as.data.table(data)
  
  # Generate dwell time
  data$ts <- lubridate::ymd_hms(data$ts)
  dwell_metric <- data[,j = {
    if(.N > 1){
      sorted_ts <- as.numeric(.SD$ts[order(.SD$ts, decreasing = TRUE)])
      sorted_ts[1] - sorted_ts[2]
    } else {
      NULL
    }
  }, by = c("date","session")]
  
  dwell_output <- data.frame(t(quantile(dwell_metric$V1, c(0.5, 0.95, 0.99))))
  dwell_output$date <- dwell_metric$date[1]
  names(dwell_output) <- c("Median", "95th percentile", "99th Percentile", "date")
  dwell_output <- dwell_output[,c(4,1:3)]
  
  # Generate clickthrough rate data
  clickthrough_data <- data[,j=list(events=.N),by = c("date","type")]
  
  # Generate click breakdown
  data <- data[order(data$type, decreasing = FALSE),]
  data <- data[!duplicated(data$session),]
  breakdown_data <- data[,j=list(events=.N),by = c("date","section_used")]
  
  # Generate by-country breakdown
  countries <- c("US", "GB", "CA", "DE", "IN", "AU", "CN", "RU", "PH", "FR")
  country_breakdown <- data[,j=list(events=.N), by = c("date", "country")]
  others <- data.table::data.table(date = date,
                                   country = "Other",
                                   events = sum(country_breakdown$events[!country_breakdown$country %in% countries]))
  country_data <- rbind(country_breakdown[country_breakdown$country %in% countries,], others)
  country_data <- country_data[order(country_data$country, decreasing = TRUE),]
  country_data$country <- c("United States", "Russia", "Philippines", "Other", "India",
                            "United Kingdom", "France", "Germany", "China", "Canada", 
                            "Australia")
  
  # Get user agent data
  wmf::set_proxies() # To allow for the latest YAML to be retrieved.
  uaparser::update_regexes()
  ua_data <- data.table::as.data.table(uaparser::parse_agents(data$user_agent, fields = c("browser","browser_major")))
  ua_data <- ua_data[,j=list(amount = .N), by = c("browser","browser_major")]
  ua_data$date <- data$date[1]
  ua_data$percent <- round((ua_data$amount/sum(ua_data$amount))*100, 2)
  ua_data <- ua_data[ua_data$percent >= 0.5, c("date", "browser", "browser_major", "percent"), with = FALSE]
  data.table::setnames(ua_data, 3, "version")
  
  wmf::write_conditional(clickthrough_data, file.path(base_path, "clickthrough_rate.tsv"))
  wmf::write_conditional(breakdown_data, file.path(base_path, "clickthrough_breakdown.tsv"))
  wmf::write_conditional(dwell_output, file.path(base_path, "dwell_metrics.tsv"))
  wmf::write_conditional(country_data, file.path(base_path, "country_data.tsv"))
  wmf::write_conditional(ua_data, file.path(base_path, "user_agent_data.tsv"))
  
  return(invisible())
}
