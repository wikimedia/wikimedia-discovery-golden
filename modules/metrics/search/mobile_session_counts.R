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

# Build query:
c(year, month, day) %<-% wmf::extract_ymd(as.Date(opt$date))

query <- glue("USE event;
SELECT
  '${opt$date}' AS date,
  event.userSessionToken AS user_session_token,
  COUNT(DISTINCT event.searchSessionToken) AS n_search_session
FROM MobileWebSearch
WHERE year = ${year} AND month = ${month} AND day = ${day}
GROUP BY
  '${opt$date}',
  event.userSessionToken
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
  output <- data.frame(
    date = character(),
    user_sessions = numeric(),
    search_sessions = numeric(),
    high_volume = numeric(),
    medium_volume = numeric(),
    low_volume = numeric(),
    threshold_high = numeric(),
    threshold_low = numeric()
  )
} else {
  # Split session counts:
  `90th percentile` <- floor(quantile(results$n_search_session, 0.9))
  `10th percentile` <- ceiling(quantile(results$n_search_session, 0.1))
  results$session_type <- dplyr::case_when(
    results$n_search_session > `90th percentile` ~ "high_volume",
    results$n_search_session < `10th percentile` ~ "low_volume",
    TRUE ~ "medium_volume"
  )
  output <- cbind(
    date = opt$date,
    user_sessions = nrow(results),
    search_sessions = sum(results$n_search_session, na.rm = TRUE),
    tidyr::spread(
      as.data.frame(results[, list(user_session = length(user_session_token)), by = "session_type"]),
      session_type, user_session
    ),
    threshold_high = `90th percentile`,
    threshold_low = `10th percentile`
  )
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
