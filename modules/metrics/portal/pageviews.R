#!/usr/bin/env Rscript

.libPaths("/srv/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "year = %Y AND month = %m AND day = %d")

query <- paste0("SET mapred.job.queue.name=nice;
USE wmf;
SELECT
  client_ip,
  COUNT(1) AS pageviews
FROM webrequest
WHERE
  webrequest_source = 'text'
  AND ", date_clause, "
  AND uri_host RLIKE('^(www\\.)?wikipedia.org/*$')
  AND INSTR(uri_path, 'search-redirect.php') = 0
  AND content_type RLIKE('^text/html')
  AND NOT (referer RLIKE('^http://localhost'))
  AND agent_type = 'user'
  AND referer_class != 'unknown'
  AND http_status IN('200', '304')
GROUP BY client_ip;")

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
  output <- cbind(date = opt$date,
                  tidyr::spread(results[, list(pageviews = sum(as.numeric(pageviews))), by = "client_type"],
                                client_type, pageviews),
                  threshold = `99.99th percentile`)
  output$pageviews = output$high_volume + output$low_volume
}

write.table(output[, c("date", "pageviews", "high_volume", "low_volume", "threshold")],
            file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
