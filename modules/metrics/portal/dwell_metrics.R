#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("optparse")
  library("glue")
  library("zeallot")
})

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}

c(year, month, day) %<-% wmf::extract_ymd(as.Date(opt$date))

query <- glue("USE event;
SELECT
  '${opt$date}' AS date,
  dt AS ts,
  event.session_id AS session
FROM WikipediaPortal
WHERE year = ${year} AND month = ${month} AND day = ${day}
  AND (
    event.cohort IS NULL
    OR event.cohort IN ('null','baseline')
  )
  AND event.country != 'US'
  AND event.event_type IN('landing', 'clickthrough')
;", .open = "${")

results <- tryCatch(
  suppressMessages(data.table::as.data.table(wmf::query_hive(query))),
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
  results$ts <- lubridate::ymd_hms(results$ts)
  dwell_metric <- results[, j = {
    if (.N > 1) {
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
