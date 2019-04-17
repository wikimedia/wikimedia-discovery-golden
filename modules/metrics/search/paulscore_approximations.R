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
  '${opt$date}' AS date, session_id, source, wiki,
  SUM(IF(action = 'click', POW(0.1, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_1,
  SUM(IF(action = 'click', POW(0.2, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_2,
  SUM(IF(action = 'click', POW(0.3, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_3,
  SUM(IF(action = 'click', POW(0.4, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_4,
  SUM(IF(action = 'click', POW(0.5, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_5,
  SUM(IF(action = 'click', POW(0.6, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_6,
  SUM(IF(action = 'click', POW(0.7, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_7,
  SUM(IF(action = 'click', POW(0.8, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_8,
  SUM(IF(action = 'click', POW(0.9, position), 0)) / SUM(IF(action = 'searchResultPage', 1, 0)) AS pow_9
FROM (
  SELECT DISTINCT
    event.searchSessionId AS session_id,
    event.source AS source,
    wiki,
    event.action AS action,
    event.position AS position,
    event.pageViewId AS view_id,
    event.query AS query
  FROM TestSearchSatisfaction2
  WHERE year = ${year} AND month = ${month} AND day = ${day}
    AND event.action IN('searchResultPage', 'click')
    AND IF(event.source = 'autocomplete' AND event.action = 'searchResultPage', event.inputLocation = 'header', TRUE)
    AND IF(event.source = 'autocomplete' AND event.action = 'click', event.position >= 0, TRUE)
) AS deduplicate
GROUP BY '${opt$date}', session_id, source, wiki
HAVING SUM(IF(action = 'searchResultPage', 1, 0)) > 0
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
      source = character(),
      pow_1 = numeric(),
      pow_2 = numeric(),
      pow_3 = numeric(),
      pow_4 = numeric(),
      pow_5 = numeric(),
      pow_6 = numeric(),
      pow_7 = numeric(),
      pow_8 = numeric(),
      pow_9 = numeric()
    ),
    langproj = data.frame(
      date = character(),
      language = character(),
      project = character(),
      search_sessions = character(),
      pow_1 = numeric(),
      pow_2 = numeric(),
      pow_3 = numeric(),
      pow_4 = numeric(),
      pow_5 = numeric(),
      pow_6 = numeric(),
      pow_7 = numeric(),
      pow_8 = numeric(),
      pow_9 = numeric()
    )
  )
} else {
  results <- data.table::as.data.table(results)
  suppressWarnings(output <- switch(
    opt$output,
    overall = {
      results[, {
        data.frame(
          pow_1 = round(mean(pow_1, na.rm = TRUE), 3),
          pow_2 = round(mean(pow_2, na.rm = TRUE), 3),
          pow_3 = round(mean(pow_3, na.rm = TRUE), 3),
          pow_4 = round(mean(pow_4, na.rm = TRUE), 3),
          pow_5 = round(mean(pow_5, na.rm = TRUE), 3),
          pow_6 = round(mean(pow_6, na.rm = TRUE), 3),
          pow_7 = round(mean(pow_7, na.rm = TRUE), 3),
          pow_8 = round(mean(pow_8, na.rm = TRUE), 3),
          pow_9 = round(mean(pow_9, na.rm = TRUE), 3))
      }, by = c("date", "source")]
    },
    langproj = {
      results <- results[source == 'fulltext', {
        data.frame(
          search_sessions = length(session_id),
          pow_1 = round(mean(pow_1, na.rm = TRUE), 3),
          pow_2 = round(mean(pow_2, na.rm = TRUE), 3),
          pow_3 = round(mean(pow_3, na.rm = TRUE), 3),
          pow_4 = round(mean(pow_4, na.rm = TRUE), 3),
          pow_5 = round(mean(pow_5, na.rm = TRUE), 3),
          pow_6 = round(mean(pow_6, na.rm = TRUE), 3),
          pow_7 = round(mean(pow_7, na.rm = TRUE), 3),
          pow_8 = round(mean(pow_8, na.rm = TRUE), 3),
          pow_9 = round(mean(pow_9, na.rm = TRUE), 3))
      }, by = c("date", "wiki")]
      wmf::set_proxies() # to allow for the latest prefixes to be retrieved.
      # Update the internal dataset of prefixes and languages:
      suppressMessages(try(polloi::update_prefixes(), silent = TRUE))
      suppressMessages(lang_proj <- polloi::parse_wikiid(results$wiki))
      # Remove accents because Reportupdater requires ASCII:
      lang_proj$language <- stringi::stri_trans_general(lang_proj$language, "Latin-ASCII")
      results <- cbind(results[, wiki := NULL], lang_proj) # add parsed languages and projects to results.
      data.table::setcolorder(results, c("date", "language", "project", "search_sessions", paste("pow", 1:9, sep = "_")))
      results
    }
  ))
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
