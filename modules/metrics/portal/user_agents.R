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
  useragent.browser_family AS browser,
  IF(useragent.browser_major IS NULL, 'NA', useragent.browser_major) AS browser_major,
  COUNT(DISTINCT event.session_id) AS amount
FROM WikipediaPortal
WHERE year = ${year} AND month = ${month} AND day = ${day}
  AND (
    event.cohort IS NULL
    OR event.cohort IN ('null','baseline')
  )
  AND event.country != 'US'
  AND event.event_type = 'landing'
GROUP BY useragent.browser_family, useragent.browser_major
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
  ua_data <- data.frame(
    date = character(),
    browser = character(),
    browser_major = character(),
    percent = numeric()
  )
} else {
  # Get user agent data
  ua_data <- results
  ua_data$date <- opt$date
  ua_data$percent <- round((ua_data$amount / sum(ua_data$amount)) * 100, 2)
  ua_data <- ua_data[ua_data$percent >= 0.5, c("date", "browser", "browser_major", "percent"), with = FALSE]
  data.table::setnames(ua_data, 3, "version")
}

write.table(ua_data, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
