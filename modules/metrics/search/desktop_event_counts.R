#!/usr/bin/env Rscript

.libPaths("/a/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option(c("-o", "--output"), default = "overall", action = "store",
                help = "Available: [default %default], langproj")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date) || !(opt$output %in% c("overall", "langproj"))) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "LEFT(timestamp, 8) = '%Y%m%d'")

query <- paste0("SELECT
  DATE('", opt$date, "') AS date,
  timestamp,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_pageViewId AS page_id,
  wiki,
  event_action AS action
FROM TestSearchSatisfaction2_", dplyr::if_else(as.Date(opt$date) < "2017-02-10", "15922352", dplyr::if_else(as.Date(opt$date) < "2017-06-29", "16270835", "16909631")), "
WHERE ", date_clause, "
  AND event_action IN('searchResultPage', 'click')
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
  output <- switch(
    opt$output,
    output = data.frame(
      date = character(),
      action = character(),
      events = numeric()
    ),
    langproj = data.frame(
      date = character(),
      language = character(),
      project = character(),
      action = character(),
      events = numeric()
    )
  )
} else {
  # De-duplicate, clean, and sort:
  results$timestamp <- as.POSIXct(results$timestamp, format = "%Y%m%d%H%M%S")
  results <- results[order(results$event_id, results$timestamp), ]
  results <- results[!duplicated(results$event_id, fromLast = TRUE), ]
  results <- data.table::as.data.table(results[order(results$session_id, results$page_id, results$timestamp), ])
  # Remove outliers (see https://phabricator.wikimedia.org/T150539):
  serp_counts <- results[action == "searchResultPage", list(SERPs = .N), by = "session_id"]
  valid_sessions <- serp_counts$session_id[serp_counts$SERPs < 1000]
  # Filter:
  results <- results[results$session_id %in% valid_sessions, ]
  ## Reimplement desktop event counts. Need the following counts:
  # - 'clickthroughs'
  # - 'Form submissions' (I don't think we can figure this out?)
  # - 'Result pages opened'
  # - 'search sessions'
  # Output:
  suppressWarnings(output <- switch(
    opt$output,
    overall = {
      clickthroughs <- results[, {
        data.frame(clickthrough = any(action == "click", na.rm = TRUE))
      }, by = c("session_id", "page_id")]
      interim <- data.frame(date = opt$date,
                            clickthroughs = sum(clickthroughs$clickthrough),
                            "Result pages opened" = nrow(clickthroughs),
                            "search sessions" = length(unique(clickthroughs$session_id)),
                            check.names = FALSE, stringsAsFactors = FALSE)
      tidyr::gather(interim, "action", "events", -date)
    },
    langproj = {
      wmf::set_proxies() # to allow for the latest prefixes to be retrieved.
      # Update the internal dataset of prefixes and languages:
      suppressMessages(try(polloi::update_prefixes(), silent = TRUE))
      suppressMessages(lang_proj <- polloi::parse_wikiid(results$wiki))
      # Remove accents because Reportupdater requires ASCII:
      lang_proj$language <- stringi::stri_trans_general(lang_proj$language, "Latin-ASCII")
      results <- cbind(results, lang_proj) # add parsed languages and projects to results.
      clickthroughs <- results[, {
        data.frame(clickthrough = any(action == "click", na.rm = TRUE))
      }, by = c("date", "language", "project", "session_id", "page_id")]
      interim <- clickthroughs[is.na(project) == FALSE, {
        data.frame("clickthroughs" = sum(clickthrough),
                   "Result pages opened" = length(page_id),
                   "search sessions" = length(unique(session_id))
        )
      }, by = c("date", "language", "project")]
      colnames(interim) <- gsub("\\.", " ", colnames(interim))
      tidyr::gather(interim, "action", "events", -date, -language, -project)
    }
  ))
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
