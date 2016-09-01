# Per-file config:
base_path <- paste0(write_root, "search/")

# Retrieves data for the mobile web stuff we care about, drops it in the aggregate-datasets directory. Should be run on stat1002, /not/ on the datavis machine.

main <- function(date = NULL){

  # Retrieve data using the query builder in ./common.R
  data1 <- wmf::build_query(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                           CASE event_action WHEN 'click' THEN 'clickthroughs'
                           WHEN 'start' THEN 'search sessions'
                           WHEN 'results' THEN 'Result pages opened' END AS action,
                           event_timeToDisplayResults AS load_time,
                           userAgent",
                           date = date,
                           table = "MobileWikiAppSearch_10641988",
                           conditionals = "event_action IN ('click','start','results')")
  data2 <- wmf::build_query(fields = "SELECT SUBSTRING(timestamp, 1, 8) AS date,
                           CASE event_action WHEN 'click' THEN 'clickthroughs'
                           WHEN 'start' THEN 'search sessions'
                           WHEN 'results' THEN 'Result pages opened' END AS action,
                           event_timeToDisplayResults AS load_time,
                           userAgent,
						   event_source AS invoke_source,
						   event_position AS click_position",
                           date = date,
                           table = "MobileWikiAppSearch_15729321",
                           conditionals = "event_action IN ('click','start','results')")
  # See https://phabricator.wikimedia.org/T143447 for more info on why we're combining
  # events from these two different schema revisions.
  data1 <- data.table::as.data.table(rbind(data1, data2[,!(names(data2) %in% c("invoke_source", "click_position"))]))
  data1$date <- lubridate::ymd(data1$date)
  data1$platform[grepl(x = data1$userAgent, pattern = "Android", fixed = TRUE)] <- "Android"
  data1$platform[is.na(data1$platform)] <- "iOS"
  data1 <- data1[,userAgent := NULL,]
  
  data2 <- data.table::as.data.table(data2)
  data2$date <- lubridate::ymd(data2$date)
  data2$platform[grepl(x = data2$userAgent, pattern = "Android", fixed = TRUE)] <- "Android"
  data2$platform[is.na(data2$platform)] <- "iOS"
  data2 <- data2[,c("userAgent","load_time"):=NULL,]
  
  # Generate aggregates
  app_results <- data1[,j = list(events = .N), by = c("date", "action", "platform")]  
  data2$click_position <- as.numeric(data2$click_position)+1
  data2$click_position <- ifelse(data2$click_position>=10 & data2$click_position<20, '10-19', 
                                ifelse(data2$click_position>=20 & data2$click_position <=100, '20-100',
								ifelse(data2$click_position>100, '100+', data2$click_position)))								
  position_count <- data2[action=='clickthroughs', j = list(events = .N), by = c("date","click_position")]
  source_count <- data2[action=='search sessions', j = list(events = .N), by = c("date","invoke_source")]
  source_count$invoke_source <- dplyr::recode(source_count$invoke_source, 
	                                    '0'='Main article toolbar', '1'='Widget', '2'='Share intent','3'='Process-text intent',
										'4'='Floating search bar in the feed', '5'='Voice search query')
  
  # Produce load time data
  load_times <- data1[data1$action == "Result pages opened", {
    output <- data.frame(t(quantile(load_time, c(0.5, 0.95, 0.99))))
    names(output) <- c("Median", "95th percentile", "99th Percentile")
    output
  }, by = c("date", "platform")]
  
  # Write out
  wmf::write_conditional(app_results, file.path(base_path, "app_event_counts.tsv"))
  wmf::write_conditional(load_times, file.path(base_path, "app_load_times.tsv"))
  wmf::write_conditional(source_count, file.path(base_path, "invoke_source_counts.tsv"))
  wmf::write_conditional(position_count, file.path(base_path, "click_position_counts.tsv"))  
    
  return(invisible())
}
