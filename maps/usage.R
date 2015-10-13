base_path <- paste0(write_root, "maps/")

# Gathers very basic data for Maps.
main <- function(date = NULL, table = "GeoFeatures_12914994"){
  
  # Retrieve data
  data <- query_func(fields = "SELECT timestamp, event_action, event_feature, event_userToken",
                     date = date,
                     table = table)
  data$timestamp <- as.Date(olivr::from_mediawiki(data$timestamp))
  
  # Roll up for high-level numbers on unique users per tool
  unique_per_tool <- data[, j=list(users = length(unique(event_userToken))*100), by = c("timestamp","event_feature")]
  setnames(unique_per_tool, 2:3, c("variable","value"))
  conditional_write(unique_per_tool, file.path(base_path, "users_per_feature.tsv"))
  
  # Generate low-level actions per tool.
  actions_per_tool <- data[, j = list(value = .N), by = c("timestamp","event_feature","event_action")]
  setnames(actions_per_tool, 2:4, c("feature","variable","value"))
  conditional_write(actions_per_tool, file.path(base_path, "actions_per_tool.tsv"))
  
  return(invisible())
}

# Good data starts on 20150804, so for backfilling...
# lapply(seq(as.Date("2015-08-04"),Sys.Date()-1, "day"), main) 
