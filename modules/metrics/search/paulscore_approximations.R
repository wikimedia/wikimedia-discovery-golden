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

query <-paste0("SELECT
  DATE('", opt$date, "') AS date,
  event_searchSessionId,
  event_source,
  wiki,
  SUM(IF(event_action = 'click', POW(0.1, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_1,
  SUM(IF(event_action = 'click', POW(0.2, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_2,
  SUM(IF(event_action = 'click', POW(0.3, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_3,
  SUM(IF(event_action = 'click', POW(0.4, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_4,
  SUM(IF(event_action = 'click', POW(0.5, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_5,
  SUM(IF(event_action = 'click', POW(0.6, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_6,
  SUM(IF(event_action = 'click', POW(0.7, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_7,
  SUM(IF(event_action = 'click', POW(0.8, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_8,
  SUM(IF(event_action = 'click', POW(0.9, event_position), 0)) / SUM(IF(event_action = 'searchResultPage', 1, 0)) AS pow_9
FROM TestSearchSatisfaction2_16270835
WHERE ", date_clause, "
  AND event_action IN ('searchResultPage', 'click')
  AND IF(event_source = 'autocomplete', event_inputLocation = 'header', TRUE)
  AND IF(event_source = 'autocomplete' AND event_action = 'click', event_position >= 0, TRUE)
GROUP BY date, event_searchSessionId, event_source, wiki;")

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
      event_source = character(),
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
      }, by = c("date", "event_source")]
    },
    langproj = {
      results <- results[event_source == 'fulltext', {
        data.frame(
          search_sessions = length(event_searchSessionId),
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
