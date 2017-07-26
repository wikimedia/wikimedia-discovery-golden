#!/usr/bin/env Rscript

.libPaths("/srv/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "LEFT(timestamp, 8) = '%Y%m%d'")

query <- paste0("
SELECT DISTINCT
  DATE('", opt$date, "') AS date,
  event_session_id AS session,
  userAgent AS user_agent
FROM WikipediaPortal_15890769
WHERE ", date_clause, "
  AND (
    event_cohort IS NULL
    OR event_cohort IN ('null','baseline')
  )
  AND event_country != 'US'
  AND event_event_type = 'landing';
")

# Fetch data from MySQL database:
results <- tryCatch(
  suppressMessages(wmf::mysql_read(query, "log")),
  error = function(e) {
    return(data.frame())
  }
)

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  ua_data <- data.frame(
    date = character(),
    browser = character(),
    browser_major = character(),
    percent = numeric()
  )
} else {
  # Get user agent data
  wmf::set_proxies() # To allow for the latest YAML to be retrieved.
  uaparser::update_regexes()
  ua_data <- data.table::rbindlist(lapply(results$user_agent, function(x){
    if (grepl("^\\{", x)){
      temp <- unlist(jsonlite::fromJSON(x)[c("browser_family", "browser_major")])
      names(temp)[1] <- "browser"
      temp <- as.data.frame(as.list(temp))
      return(temp)
    } else {
      return(uaparser::parse_agents(x, fields = c("browser", "browser_major")))
    }
  }), fill = TRUE)
  ua_data <- ua_data[, j = list(amount = .N), by = c("browser", "browser_major")]
  ua_data$date <- results$date[1]
  ua_data$percent <- round((ua_data$amount/sum(ua_data$amount)) * 100, 2)
  ua_data <- ua_data[ua_data$percent >= 0.5, c("date", "browser", "browser_major", "percent"), with = FALSE]
  data.table::setnames(ua_data, 3, "version")
}

write.table(ua_data, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
