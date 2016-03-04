base_path <- paste0(write_root, "maps/")

# Gathers very basic data for Maps.
main <- function(date = NULL, table = "GeoFeatures_12914994"){
  
  # Retrieve data
  data <- wmf::build_query(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date, event_action, event_feature, event_userToken",
                           date = date, table = table)
  data <- data.table::as.data.table(data)
  data$date <- lubridate::ymd(data$date)
  
  # Roll up for high-level numbers on unique users per tool
  unique_per_tool <- data[, j = list(users = length(unique(event_userToken))*100), by = c("date", "event_feature")]
  data.table::setnames(unique_per_tool, 2:3, c("variable", "value"))
  
  # Generate low-level actions per tool.
  actions_per_tool <- data[, j = list(value = .N), by = c("date", "event_feature", "event_action")]
  data.table::setnames(actions_per_tool, 2:4, c("feature", "variable", "value"))

  # Write out
  wmf::write_conditional(unique_per_tool, file.path(base_path, "users_per_feature.tsv"))
  wmf::write_conditional(actions_per_tool, file.path(base_path, "actions_per_tool.tsv"))
  
  return(invisible())
}
