#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("optparse")
  library("glue")
  library("zeallot")
})

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

c(year, month, day) %<-% wmf::extract_ymd(as.Date(opt$date))

query <- glue("USE event;
SELECT
  '${opt$date}' AS date,
  dt AS timestamp,
  event.uniqueId AS event_id,
  event.searchSessionId AS session_id,
  event.pageViewId AS page_id,
  wiki,
  event.action AS action
FROM SearchSatisfaction
WHERE year = ${year} AND month = ${month} AND day = ${day}
  AND event.action IN('searchResultPage', 'click')
  AND (event.subTest IS NULL OR event.subTest IN('null', 'baseline'))
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
