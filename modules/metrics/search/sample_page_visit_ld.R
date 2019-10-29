#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("optparse")
  library("glue")
  library("magrittr")
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
  dt AS ts,
  event.uniqueId AS event_id,
  event.searchSessionId AS session_id,
  event.pageViewId AS page_id,
  event.action AS event,
  IF(event.checkin IS NULL, 'NA', event.checkin) AS checkin
FROM SearchSatisfaction
WHERE year = ${year} AND month = ${month} AND day = ${day}
  AND event.action IN('searchResultPage','visitPage', 'checkin')
  AND (event.subTest IS NULL OR event.subTest IN('null', 'baseline'))
  AND event.source = 'fulltext'
;", .open = "${")

results <- tryCatch(
  suppressMessages(wmf::query_hive(query)),
  error = function(e) {
    return(data.frame())
  }
)

empty_df <- function() {
  data.frame(
    date = character(),
    LD10 = character(),
    LD25 = character(),
    LD50 = character(),
    LD75 = character(),
    LD90 = character(),
    LD95 = character(),
    LD99 = character()
  )
}

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  page_visit_survivorship <- empty_df()
} else {
  # De-duplicate, clean, and sort:
  results %<>%
    dplyr::mutate(ts = lubridate::ymd_hms(ts)) %>%
    dplyr::arrange(session_id, event_id, ts) %>%
    dplyr::distinct(session_id, event_id, .keep_all = TRUE) %>%
    dplyr::arrange(session_id, page_id, ts) %>%
    dplyr::select(ts, session_id, page_id, event, checkin) %>%
    data.table::data.table(key = c("session_id", "page_id"))
  valid_sessions <- results[, list(valid = all(c("searchResultPage", "visitPage", "checkin") %in% event)),
                            by = "session_id"]
  results <- results[results$session_id %in% valid_sessions$session_id[valid_sessions$valid] & results$event != "searchResultPage", ]
  ## Calculates the median lethal dose (LD50) and other.
  ## LD50 = the time point at which we have lost 50% of our users.
  checkins <- c(0, 10, 20, 30, 40, 50, 60, 90, 120, 150, 180, 210, 240, 300, 360, 420)
  # ^ this will be used for figuring out the interval bounds for each check-in
  # Treat each individual search session as its own thing, rather than belonging
  #   to a set of other search sessions by the same user.
  page_visits <- results[, {
    if (any(.SD$event == "checkin")) {
      last_checkin <- max(.SD$checkin, na.rm = TRUE)
      idx <- which(checkins > last_checkin)
      if (length(idx) == 0) idx <- 16 # length(checkins) = 16
      next_checkin <- checkins[min(idx)]
      status <- ifelse(last_checkin == 420, 0, 3)
      data.table::data.table(
        `last check-in` = as.integer(last_checkin),
        `next check-in` = as.integer(next_checkin),
        status = as.integer(status)
      )
    } else {
      # If there is no checkin event, that means users leave the page within 10s
      data.table::data.table(
        `last check-in` = 0L,
        `next check-in` = 10L,
        status = 3L
      )
    }
  }, by = c("session_id", "page_id")]

  if (nrow(page_visits) == 0) {
    page_visit_survivorship <- empty_df()
  } else {
    surv <- survival::Surv(
      time = page_visits$`last check-in`,
      time2 = page_visits$`next check-in`,
      event = page_visits$status,
      type = "interval"
    )
    fit <- survival::survfit(surv ~ 1)
    page_visit_survivorship <- data.frame(date = opt$date, rbind(quantile(fit, probs = c(0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99))$quantile))
    colnames(page_visit_survivorship) <- c('date', 'LD10', 'LD25', 'LD50', 'LD75', 'LD90', 'LD95', 'LD99')
  }
}

write.table(page_visit_survivorship, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
