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

query <- paste0("
SELECT
  DATE('", opt$date, "') AS date,
  timestamp AS ts,
  event_session_id AS session
FROM WikipediaPortal_15890769
WHERE ", date_clause, "
  AND (
    event_cohort IS NULL
    OR event_cohort IN ('null','baseline')
  )
  AND event_country != 'US'
  AND event_event_type IN('landing', 'clickthrough');
")

# Fetch data from MySQL database:
results <- tryCatch(
  suppressMessages(data.table::as.data.table(wmf::mysql_read(query, "log"))),
  error = function(e) {
    return(data.frame())
  }
)

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  dwell_output <- dplyr::data_frame(
    date = character(),
    Median = numeric(),
    `95th percentile` = numeric(),
    `99th percentile` = numeric()
  )
} else {
  # Generate dwell time
  results$ts <- as.POSIXct(results$ts, format = "%Y%m%d%H%M%S")
  dwell_metric <- results[, j = {
    if(.N > 1){
      sorted_ts <- as.numeric(.SD$ts[order(.SD$ts, decreasing = TRUE)])
      sorted_ts[1] - sorted_ts[2]
    } else {
      NULL
    }
  }, by = c("date", "session")]
  # Compute summary statistics
  dwell_output <- dwell_metric[, list(
    Median = quantile(V1, 0.5),
    `95th percentile` = quantile(V1, 0.95),
    `99th percentile` = quantile(V1, 0.99)
  ), by = "date"]
}

write.table(dwell_output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
