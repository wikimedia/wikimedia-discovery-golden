#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("methods")
  library("optparse")
  library("magrittr") # Required for piping
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

query <- switch(
  opt$platform,

  # Desktop-based events:
  desktop = paste0("
    SET mapred.job.queue.name=nice;
    USE event;
    SELECT '", opt$date, "' AS date,
      'desktop' AS platform,
      round(percentile(load_time, 0.5), 2) AS median,
      round(percentile(load_time, 0.95), 2) AS percentile_95,
      round(percentile(load_time, 0.99), 2) AS percentile_99
    FROM
    (SELECT
      dt,
      event.uniqueId AS event_id,
      CASE WHEN event.msToDisplayResults <= 0 THEN NULL
           ELSE event.msToDisplayResults END AS load_time
    FROM testsearchsatisfaction2
    WHERE ", date_clause, "
      AND event.action = 'searchResultPage'
      AND (event.subTest IS NULL OR event.subTest IN ('null', 'baseline'))
      AND event.source = 'fulltext') AS all
    RIGHT JOIN
    (SELECT event.uniqueId AS event_id,
       MIN(dt) AS dt
    FROM testsearchsatisfaction2
    WHERE ", date_clause, "
      AND event.action = 'searchResultPage'
      AND (event.subTest IS NULL OR event.subTest IN ('null', 'baseline'))
      AND event.source = 'fulltext'
    GROUP BY event.uniqueId) AS dedup
    ON (all.dt=dedup.dt AND all.event_id=dedup.event_id)
    ;"),

  # Mobile Web-based events:
  mobileweb = paste0("
    SET mapred.job.queue.name=nice;
    USE event;
    SELECT
      '", opt$date, "' AS date,
      'mobileweb' AS platform,
      round(percentile(CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
           ELSE event.timeToDisplayResults END, 0.5), 2) AS median,
      round(percentile(CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
           ELSE event.timeToDisplayResults END, 0.95), 2) AS percentile_95,
      round(percentile(CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
           ELSE event.timeToDisplayResults END, 0.99), 2) AS percentile_99
    FROM mobilewebsearch
    WHERE ", date_clause, "
      AND event.action = 'impression-results';"),

  # App-based events:
  app = paste0("
    SET mapred.job.queue.name=nice;
    USE event;
    SELECT
    '", opt$date, "' AS date,
    platform,
    round(percentile(load_time, 0.5), 2) AS median,
    round(percentile(load_time, 0.95), 2) AS percentile_95,
    round(percentile(load_time, 0.99), 2) AS percentile_99
    FROM (
      SELECT
      useragent.os_family AS platform,
      CASE WHEN COALESCE(event.timeToDisplayResults, event.time_to_display_results) <= 0 THEN NULL
           ELSE COALESCE(event.timeToDisplayResults, event.time_to_display_results) END AS load_time
    FROM mobilewikiappsearch
    WHERE ", date_clause, "
      AND event.action = 'results'
      -- Need to union with MobileWikiAppiOSSearch after T205551 is fixed
    ) AS all_serp
    GROUP BY platform;")

)

# Fetch data from database using Hive:
results <- tryCatch(
  suppressMessages(wmf::query_hive(query)),
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
  output <- results %>%
    dplyr::rename(
      Median = median,
      `95th percentile` = percentile_95,
      `99th percentile` = percentile_99
      )
  # Remove platform column for non-app load times:
  if (opt$platform != "app") {
    output$platform <- NULL
  }
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
