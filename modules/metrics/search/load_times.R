#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
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
date_clause <- as.character(as.Date(opt$date), format = "year = %Y AND month = %m AND day = %d")

# All three platforms will yield data structured the same way, having the following columns:
# date      : the date of the event
# dt : timestamp (ISO-8601) of the event
# platform  : 'Android' or 'iOS' for app-based events, NULL otherwise
# event_id  : unique event ID for de-duplication of desktop-based events, NULL o.w.
# load_time : load time (ms)
query <- switch(
  opt$platform,

  # Desktop-based events:
  desktop = paste0("
SET mapred.job.queue.name=nice;
USE event;
SELECT
  '", opt$date, "' AS date, dt,
  'desktop' AS platform,
  event.uniqueId AS event_id,
  CASE WHEN event.msToDisplayResults <= 0 THEN NULL
       ELSE event.msToDisplayResults END AS load_time
FROM testsearchsatisfaction2
WHERE ", date_clause, "
  AND event.action = 'searchResultPage'
  AND (event.subTest IS NULL OR event.subTest IN ('null', 'baseline'))
  AND event.source = 'fulltext';"),

  # Mobile Web-based events:
  mobileweb = paste0("
SET mapred.job.queue.name=nice;
USE event;
SELECT
  '", opt$date, "' AS date, dt,
  'mobileweb' AS platform,
  'N/A' AS event_id,
  CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
       ELSE event.timeToDisplayResults END AS load_time
FROM mobilewebsearch
WHERE ", date_clause, "
  AND event.action = 'impression-results';"),

  # App-based events:
  app = paste0("
SET mapred.job.queue.name=nice;
USE event;
SELECT
  '", opt$date, "' AS date, dt,
  useragent.os_family AS platform,
  'N/A' as event_id,
  CASE WHEN COALESCE(event.timeToDisplayResults, event.time_to_display_results) <= 0 THEN NULL
       ELSE COALESCE(event.timeToDisplayResults, event.time_to_display_results) END AS load_time
FROM mobilewikiappsearch
WHERE ", date_clause, "
  AND event.action = 'results'
  -- Need to union with MobileWikiAppiOSSearch after T205551 is fixed
  ;")

)

# Fetch data from database using Hive:
results <- tryCatch(
  suppressMessages(wmf::query_hive(query, use_beeline = TRUE)),
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
  results$dt <- lubridate::ymd_hms(results$dt)
  results$load_time <- as.numeric(gsub("NULL", NA, results$load_time, fixed = TRUE))
  # Remove duplicated events on TSS2:
  if (opt$platform == "desktop") {
    results <- results[order(results$event_id, results$dt), ]
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
