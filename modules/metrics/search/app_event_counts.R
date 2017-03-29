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
  date, wiki, action, platform, COUNT(*) AS events
FROM (
  SELECT
    DATE('", opt$date, "') AS date,
    wiki,
    CASE event_action WHEN 'click' THEN 'clickthroughs'
                      WHEN 'start' THEN 'search sessions'
                      WHEN 'results' THEN 'Result pages opened'
                      END AS action,
    CASE WHEN INSTR(userAgent, 'Android') > 0 THEN 'Android'
         ELSE 'iOS' END AS platform
  FROM MobileWikiAppSearch_10641988
  WHERE ", date_clause, "
    AND event_action IN ('click', 'start', 'results')
    AND wiki NOT RLIKE '^WikipediaApp'
  UNION ALL
  SELECT
    DATE('", opt$date, "') AS date,
    wiki,
    CASE event_action WHEN 'click' THEN 'clickthroughs'
                      WHEN 'start' THEN 'search sessions'
                      WHEN 'results' THEN 'Result pages opened'
                      END AS action,
    CASE WHEN INSTR(userAgent, 'Android') > 0 THEN 'Android'
         ELSE 'iOS' END AS platform
  FROM MobileWikiAppSearch_15729321
  WHERE ", date_clause, "
    AND event_action IN ('click', 'start', 'results')
    AND wiki NOT RLIKE '^WikipediaApp'
) AS MobileWikiAppSearch
GROUP BY date, wiki, action, platform;")

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
      platform = character(),
      events = numeric()
    ),
    langproj = data.frame(
      date = character(),
      language = character(),
      action = character(),
      platform = character(),
      events = numeric()
    )
  )
} else {
  results <- data.table::as.data.table(results)
  suppressWarnings(output <- switch(
    opt$output,
    overall = {
      results[, {
        data.frame(events = sum(events, na.rm = TRUE))
      }, by = c("date", "action", "platform")]
    },
    langproj = {
      wmf::set_proxies() # to allow for the latest prefixes to be retrieved.
      # Update the internal dataset of prefixes and languages:
      suppressMessages(try(polloi::update_prefixes(), silent = TRUE))
      suppressMessages(lang_proj <- polloi::parse_wikiid(results$wiki))
      # Remove accents because Reportupdater requires ASCII:
      lang_proj$language <- stringi::stri_trans_general(lang_proj$language, "Latin-ASCII")
      results <- cbind(results[, wiki := NULL], language = lang_proj$language) # add parsed languages to results (project is wikipedia for app).
      data.table::setcolorder(results, c("date", "language", "action", "platform", "events"))
      results[!is.na(results$language), ]
    }
  ))
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
