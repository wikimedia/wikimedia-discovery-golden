#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("optparse")
  library("glue")
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

query <- glue("USE wmf;
SELECT
  client_ip,
  COUNT(1) AS pageviews
FROM webrequest
WHERE
  webrequest_source = 'text'
  AND year = ${year} AND month = ${month} AND day = ${day}
  AND uri_host RLIKE('^(www\\.)?wikipedia.org/*$')
  AND INSTR(uri_path, 'search-redirect.php') = 0
  AND content_type RLIKE('^text/html')
  AND NOT (referer RLIKE('^http://localhost'))
  AND agent_type = 'user'
  AND referer_class != 'unknown'
  AND http_status IN('200', '304')
GROUP BY client_ip
;", .open = "${")

# Fetch data from database using Hive:
results <- tryCatch(
  data.table::as.data.table(wmf::query_hive(query)),
  error = function(e) {
    return(data.frame())
  }
)

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  output <- data.frame(
    date = character(),
    pageviews = numeric(),
    high_volume = numeric(),
    low_volume = numeric(),
    threshold = numeric()
  )
} else {
  # Split pageview counts:
  `99.99th percentile` <- floor(quantile(results$pageviews, 0.9999))
  results$client_type <- ifelse(results$pageviews < `99.99th percentile`, "low_volume", "high_volume")
  results$date <- opt$date
  output <- results[, list(pageviews = sum(as.numeric(pageviews))), by = c("date", "client_type")]
  output <- data.table::dcast(output, date ~ client_type, value.var = "pageviews")
  output$threshold <- `99.99th percentile`
  output$pageviews <- output$high_volume + output$low_volume
}

write.table(output[, c("date", "pageviews", "high_volume", "low_volume", "threshold")],
            file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
