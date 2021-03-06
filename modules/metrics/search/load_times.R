#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("methods")
  library("optparse")
  library("glue")
  library("zeallot")
  library("magrittr")
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

c(year, month, day) %<-% wmf::extract_ymd(as.Date(opt$date))

query <- switch(
  opt$platform,

  # Desktop-based events:
  desktop = glue("USE event;
  SELECT
    '${opt$date}' AS date,
    'desktop' AS platform,
    ROUND(PERCENTILE(load_time, 0.5), 2) AS median,
    ROUND(PERCENTILE(load_time, 0.95), 2) AS percentile_95,
    ROUND(PERCENTILE(load_time, 0.99), 2) AS percentile_99
  FROM (
    SELECT
      dt,
      event.uniqueId AS event_id,
      CASE WHEN event.msToDisplayResults <= 0 THEN NULL
           ELSE event.msToDisplayResults END AS load_time
    FROM SearchSatisfaction
    WHERE year = ${year} AND month = ${month} AND day = ${day}
      AND event.action = 'searchResultPage'
      AND (event.subTest IS NULL OR event.subTest IN('null', 'baseline'))
      AND event.source = 'fulltext'
  ) AS all
  RIGHT JOIN (
    SELECT event.uniqueId AS event_id, MIN(dt) AS dt
    FROM SearchSatisfaction
    WHERE year = ${year} AND month = ${month} AND day = ${day}
      AND event.action = 'searchResultPage'
      AND (event.subTest IS NULL OR event.subTest IN('null', 'baseline'))
      AND event.source = 'fulltext'
    GROUP BY event.uniqueId
  ) AS dedup
  ON (
    all.dt = dedup.dt
    AND all.event_id = dedup.event_id
  )
  ;", .open = "${"),

  # Mobile Web-based events:
  mobileweb = glue("USE event;
  SELECT
    '${opt$date}' AS date,
    'mobileweb' AS platform,
    ROUND(PERCENTILE(CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
          ELSE event.timeToDisplayResults END, 0.5), 2) AS median,
    ROUND(PERCENTILE(CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
          ELSE event.timeToDisplayResults END, 0.95), 2) AS percentile_95,
    ROUND(PERCENTILE(CASE WHEN event.timeToDisplayResults <= 0 THEN NULL
          ELSE event.timeToDisplayResults END, 0.99), 2) AS percentile_99
  FROM MobileWebSearch
  WHERE year = ${year} AND month = ${month} AND day = ${day}
    AND event.action = 'impression-results'
  ;", .open = "${"),

  # App-based events:
  app = glue("USE event;
  SELECT
    '${opt$date}' AS date,
    platform,
    ROUND(PERCENTILE(load_time, 0.5), 2) AS median,
    ROUND(PERCENTILE(load_time, 0.95), 2) AS percentile_95,
    ROUND(PERCENTILE(load_time, 0.99), 2) AS percentile_99
  FROM (
    SELECT
      useragent.os_family AS platform,
      CASE WHEN COALESCE(event.timeToDisplayResults, event.time_to_display_results) <= 0 THEN NULL
           ELSE COALESCE(event.timeToDisplayResults, event.time_to_display_results) END AS load_time
    FROM MobileWikiAppSearch
    WHERE year = ${year} AND month = ${month} AND day = ${day}
      AND event.action = 'results'

    UNION ALL

    SELECT
      useragent.os_family AS platform,
      IF(event.time_to_display_results <= 0, NULL, event.time_to_display_results) AS load_time
    FROM MobileWikiAppiOSSearch
    WHERE year = ${year} AND month = ${month} AND day = ${day}
      AND event.action = 'results'
  ) AS all_serp
  WHERE platform IN('Android', 'iOS') AND load_time IS NOT NULL
  GROUP BY '${opt$date}', platform
  ;", .open = "${")

)

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
    output <- dplyr::tibble(
      date = character(),
      platform = character(),
      Median = numeric(),
      `95th percentile` = numeric(),
      `99th percentile` = numeric()
    )
  } else {
    output <- dplyr::tibble(
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
