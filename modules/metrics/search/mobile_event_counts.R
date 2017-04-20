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
  wiki,
  CASE event_action WHEN 'click-result' THEN 'clickthroughs'
                    WHEN 'session-start' THEN 'search sessions'
                    WHEN 'impression-results' THEN 'Result pages opened'
                    END AS action,
  COUNT(*) AS events
FROM MobileWebSearch_12054448
WHERE ", date_clause, "
  AND event_action IN('click-result', 'session-start', 'impression-results')
GROUP BY date, wiki, action;")


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
  results <- data.table::as.data.table(results)
  suppressWarnings(output <- switch(
    opt$output,
    overall = {
      results[, {
        data.frame(events=sum(events, na.rm=TRUE))
      }, by = c("date", "action")]
    },
    langproj = {
      wmf::set_proxies() # to allow for the latest prefixes to be retrieved.
      # Update the internal dataset of prefixes and languages:
      suppressMessages(try(polloi::update_prefixes(), silent = TRUE))
      suppressMessages(lang_proj <- polloi::parse_wikiid(results$wiki))
      # Remove accents because Reportupdater requires ASCII:
      lang_proj$language <- stringi::stri_trans_general(lang_proj$language, "Latin-ASCII")
      results <- cbind(results[, wiki:=NULL], lang_proj) # add parsed languages and projects to results.
      data.table::setcolorder(results, c("date", "language", "project", "action", "events"))
      results
    }
  ))
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)