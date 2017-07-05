#!/usr/bin/env Rscript

.libPaths("/a/discovery/r-library")
suppressPackageStartupMessages({
  library("methods")
  library("optparse")
})

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option(c("-p", "--platform"), default = NA, action = "store", type = "character",
              help = "Available: desktop, mobileweb, app")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date) || is.na(opt$platform)) {
  quit(save = "no", status = 1)
}

if (!(opt$platform %in% c("desktop", "mobileweb", "app"))) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "LEFT(timestamp, 8) = '%Y%m%d'")

# All three platforms will yield data structured the same way, having the following columns:
# date      : the date of the event
# timestamp : YYYYMMDDHHMMSS of the event
# platform  : 'Android' or 'iOS' for app-based events, NULL otherwise
# event_id  : unique event ID for de-duplication of desktop-based events, NULL o.w.
# load_time : load time (ms)
query <- switch(
  opt$platform,

  # Desktop-based events:
  desktop = paste0("
SELECT
  '", opt$date, "' AS date, timestamp,
  'desktop' AS platform,
  event_uniqueId AS event_id,
  CASE WHEN event_msToDisplayResults <= 0 THEN NULL
       ELSE event_msToDisplayResults END AS load_time
FROM TestSearchSatisfaction2_", dplyr::if_else(as.Date(opt$date) < "2017-02-10", "15922352", dplyr::if_else(as.Date(opt$date) < "2017-06-29", "16270835", "16909631")), "
WHERE ", date_clause, "
  AND event_action = 'searchResultPage'
  AND (event_subTest IS NULL OR event_subTest IN ('null', 'baseline'))
  AND event_source = 'fulltext';"),

  # Mobile Web-based events:
  mobileweb = paste0("
SELECT
  '", opt$date, "' AS date, timestamp,
  'mobileweb' AS platform,
  'N/A' AS event_id,
  CASE WHEN event_timeToDisplayResults <= 0 THEN NULL
       ELSE event_timeToDisplayResults END AS load_time
FROM MobileWebSearch_12054448
WHERE ", date_clause, "
  AND event_action = 'impression-results';"),

  # App-based events:
  app = paste0("
SELECT
  '", opt$date, "' AS date, timestamp,
  CASE WHEN INSTR(userAgent, 'Android') > 0 THEN 'Android'
       ELSE 'iOS' END AS platform,
  '10641988' as event_id,
  CASE WHEN event_timeToDisplayResults <= 0 THEN NULL
       ELSE event_timeToDisplayResults END AS load_time
FROM MobileWikiAppSearch_10641988
WHERE ", date_clause, "
  AND event_action = 'results'
UNION ALL
SELECT
  '", opt$date, "' AS date, timestamp,
  CASE WHEN INSTR(userAgent, 'Android') > 0 THEN 'Android'
       ELSE 'iOS' END AS platform,
  '15729321' as event_id,
  event_timeToDisplayResults AS load_time
FROM MobileWikiAppSearch_15729321
WHERE ", date_clause, "
  AND event_action = 'results';")

)

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
  if (opt$platform == "app") {
    output <- dplyr::data_frame(
      date = character(),
      platform = character(),
      Median = numeric(),
      `95th percentile` = numeric(),
      `99th percentile` = numeric()
    )
  } else {
    output <- dplyr::data_frame(
      date = character(),
      Median = numeric(),
      `95th percentile` = numeric(),
      `99th percentile` = numeric()
    )
  }
} else {
  results$timestamp <- lubridate::ymd_hms(results$timestamp)
  # Remove duplicated events on TSS2:
  if (opt$platform == "desktop") {
    results <- results[order(results$event_id, results$timestamp), ]
    results <- results[!duplicated(results$event_id), ]
  }
  results$event_id <- NULL
  library(magrittr) # Required for piping
  # Process load times:
  output <- results %>%
    dplyr::group_by(date, platform) %>%
    dplyr::summarize(
      Median = round(quantile(load_time, 0.5, na.rm = TRUE), 2),
      `95th percentile` = round(quantile(load_time, 0.95, na.rm = TRUE), 2),
      `99th percentile` = round(quantile(load_time, 0.99, na.rm = TRUE), 2)
    ) %>%
    dplyr::ungroup() %>%
    as.data.frame
  # Remove platform column for non-app load times:
  if (opt$platform != "app") {
    output$platform <- NULL
  }
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
