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
  '${opt$date}' AS date, wiki, action, platform, COUNT(1) AS events
FROM (
  SELECT
    wiki,
    CASE event.action WHEN 'click' THEN 'clickthroughs'
                      WHEN 'start' THEN 'search sessions'
                      WHEN 'results' THEN 'Result pages opened'
                      END AS action,
    useragent.os_family AS platform
  FROM MobileWikiAppSearch
  WHERE year = ${year} AND month = ${month} AND day = ${day}
    AND event.action IN('click', 'start', 'results')

  UNION ALL

  SELECT
    wiki,
    CASE event.action WHEN 'click' THEN 'clickthroughs'
                      WHEN 'start' THEN 'search sessions'
                      WHEN 'results' THEN 'Result pages opened'
                      END AS action,
    useragent.os_family AS platform
  FROM MobileWikiAppiOSSearch
  WHERE year = ${year} AND month = ${month} AND day = ${day}
    AND event.action IN('click', 'start', 'results')
) AS MobileWikiAppSearch
WHERE platform IN('iOS', 'Android')
GROUP BY '${opt$date}', wiki, action, platform
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
