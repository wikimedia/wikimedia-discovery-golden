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
  dt AS timestamp,
  event.uniqueId AS event_id,
  event.searchSessionId AS session_id,
  event.pageViewId AS page_id,
  wiki,
  event.action AS event,
  MD5(LOWER(TRIM(event.query))) AS query_hash
FROM SearchSatisfaction
WHERE year = ${year} AND month = ${month} AND day = ${day}
  AND event.action IN ('searchResultPage', 'click', 'iwclick', 'ssclick')
  AND (event.subTest IS NULL OR event.subTest IN ('null', 'baseline'))
  AND event.source = 'fulltext'
;", .open = "${")

results <- tryCatch(
  suppressMessages(wmf::query_hive(query)),
  error = function(e) {
    return(data.frame())
  }
)

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  output <- data.frame(
    date = character(),
    return_to_same_search = numeric(),
    n_search = numeric(),
    return_to_make_another_search = numeric(),
    n_session = numeric()
  )
} else {
  # De-duplicating events
  results <- results %>%
    dplyr::mutate(
      timestamp = lubridate::ymd_hms(timestamp),
      date = as.Date(timestamp)
    ) %>%
    dplyr::arrange(session_id, event_id, timestamp) %>%
    dplyr::distinct(session_id, event_id, .keep_all = TRUE)

  # Remove outliers (see https://phabricator.wikimedia.org/T150539):
  results <- results %>%
    dplyr::group_by(session_id) %>%
    dplyr::filter(length(unique(page_id)) < 1000) %>%
    dplyr::ungroup()

  # De-duplicating SERPs...
  SERPs <- results %>%
    dplyr::filter(event == "searchResultPage") %>%
    dplyr::select(c(session_id, page_id, query_hash)) %>%
    dplyr::group_by(session_id, query_hash) %>%
    dplyr::mutate(serp_id = page_id[1]) %>%
    dplyr::ungroup() %>%
    dplyr::select(c(page_id, serp_id))
  results <- results %>%
    dplyr::left_join(SERPs, by = "page_id")
  rm(SERPs) # to free up memory

  # Removing events without an associated SERP (orphan clicks)...
  results <- results %>%
    dplyr::filter(!(is.na(serp_id)))

  returnRate_to_same_search <- results %>%
    dplyr::group_by(date, serp_id) %>%
    dplyr::filter(sum(grepl("click", event)) > 0) %>% # Among search with at least 1 click
    dplyr::arrange(timestamp) %>%
    dplyr::mutate(n_click_cumsum = cumsum(grepl("click", event))) %>%
    dplyr::filter(n_click_cumsum > 0) %>% # delete serp before first click
    dplyr::summarize(comeback = "searchResultPage" %in% event | sum(n_click_cumsum > 1)) %>% # comeback to the same serp or make another click
    dplyr::group_by(date) %>%
    dplyr::summarize(return_to_same_search = sum(comeback), n_search = dplyr::n())

  returnRate_to_other_search <-  results %>%
    dplyr::group_by(date, session_id) %>%
    dplyr::filter(sum(grepl("click", event)) > 0) %>% # Among session with at least 1 click
    dplyr::arrange(timestamp) %>%
    dplyr::mutate(n_click_cumsum = cumsum(grepl("click", event))) %>%
    dplyr::filter(n_click_cumsum > 0) %>% # delete serp before first click
    dplyr::summarize(another_search = length(unique(serp_id)) > 1) %>% # comeback to make another search
    dplyr::group_by(date) %>%
    dplyr::summarize(return_to_make_another_search = sum(another_search), n_session = length(unique(session_id)))

  output <- returnRate_to_same_search %>%
    dplyr::inner_join(returnRate_to_other_search, by = "date")

}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
