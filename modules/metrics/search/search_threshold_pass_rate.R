#!/usr/bin/env Rscript

.libPaths("/a/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

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

query <- paste0("SELECT
  DATE('", opt$date, "') AS date,
  timestamp,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_action AS action
FROM TestSearchSatisfaction2_", dplyr::if_else(as.Date(opt$date) < "2017-02-10", "15922352", "16270835"), "
WHERE ", date_clause, "
  AND event_action IN('searchResultPage', 'visitPage', 'checkin', 'click')
  AND (event_subTest IS NULL OR event_subTest IN ('null', 'baseline'))
  AND event_source = 'fulltext';")

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
  output <- data.frame(
    date = character(),
    threshold_pass = numeric()
  )
} else {
  # De-duplicate, clean, and sort:
  results$timestamp <- as.POSIXct(results$timestamp, format = "%Y%m%d%H%M%S")
  results <- results[order(results$event_id, results$timestamp), ]
  results <- results[!duplicated(results$event_id, fromLast = TRUE), ]
  results <- results[order(results$session_id, results$timestamp), ]
  ## For debugging and coming up with new thresholds:
  # df <- ortiz:::numeric_check(as.data.frame(results)[,c("session_id", "timestamp")], "timestamp")
  # split_data <- split(df[, "timestamp"], df[, "session_id"])
  # dwell_times <- ortiz:::dwell_time_(split_data)
  # sum(dwell_times > 10)/length(dwell_times)
  dwell_data <- ortiz::dwell_time(data = results, id_col = "session_id", ts_col = "timestamp", dwell_threshold = 10)
  # Output:
  output <- data.frame(
    date = opt$date,
    threshold_pass = mean(dwell_data),
    stringsAsFactors = FALSE
  )
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)