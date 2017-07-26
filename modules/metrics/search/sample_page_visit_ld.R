#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages(library("optparse"))

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
  timestamp,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_pageViewId AS page_id,
  event_action AS action,
  event_checkin AS checkin
FROM TestSearchSatisfaction2_", dplyr::if_else(as.Date(opt$date) < "2017-02-10", "15922352", dplyr::if_else(as.Date(opt$date) < "2017-06-29", "16270835", "16909631")), "
WHERE ", date_clause, "
  AND event_action IN('searchResultPage','visitPage', 'checkin')
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
  page_visit_survivorship <- data.frame(
    date = character(),
    LD10 = character(),
    LD25 = character(),
    LD50 = character(),
    LD75 = character(),
    LD90 = character(),
    LD95 = character(),
    LD99 = character()
  )
} else {
  # De-duplicate, clean, and sort:
  results$timestamp <- as.POSIXct(results$timestamp, format = "%Y%m%d%H%M%S")
  results <- results[order(results$event_id, results$timestamp), ]
  results <- results[!duplicated(results$event_id, fromLast = TRUE), ]
  results <- data.table::as.data.table(results[order(results$session_id, results$page_id, results$timestamp), ])
  valid_sessions <- results[, list(valid = all(c("searchResultPage", "visitPage", "checkin") %in% action)),
                            by = "session_id"]$session_id
  results <- results[results$session_id %in% valid_sessions & results$action != "searchResultPage", ]
  ## Calculates the median lethal dose (LD50) and other.
  ## LD50 = the time point at which we have lost 50% of our users.
  checkins <- c(0, 10, 20, 30, 40, 50, 60, 90, 120, 150, 180, 210, 240, 300, 360, 420)
  # ^ this will be used for figuring out the interval bounds for each check-in
  # Treat each individual search session as its own thing, rather than belonging
  #   to a set of other search sessions by the same user.
  page_visits <- results[, {
    if (all(!is.na(.SD$checkin))) {
      last_checkin <- max(.SD$checkin, na.rm = TRUE)
      idx <- which(checkins > last_checkin)
      if (length(idx) == 0) idx <- 16 # length(checkins) = 16
      next_checkin <- checkins[min(idx)]
      status <- ifelse(last_checkin == 420, 0, 3)
      data.table::data.table(
        `last check-in` = last_checkin,
        `next check-in` = next_checkin,
        status = status
      )
    }
  }, by = c("session_id", "page_id")]
  surv <- survival::Surv(time = page_visits$`last check-in`,
                         time2 = page_visits$`next check-in`,
                         event = page_visits$status,
                         type = "interval")
  fit <- survival::survfit(surv ~ 1)
  page_visit_survivorship <- data.frame(date = opt$date, rbind(quantile(fit, probs = c(0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99))$quantile))
  colnames(page_visit_survivorship) <- c('date', 'LD10', 'LD25', 'LD50', 'LD75', 'LD90', 'LD95', 'LD99')
}

write.table(page_visit_survivorship, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
