#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option("--include_automata", default = FALSE, action = "store_true",
              help = "Whether to include automata [default %default]")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "year = %Y AND month = %m AND day = %d")

## This script extracts Vagrant logs and processes them to summarize server-side maps usage.
# Specifically, it generates a dataset containing summaries (avg, median, percentiles) of:
# - total tile requests
# - tile requests per style, e.g. "osm", "osm-intl", ...
# - tile requests per style per zoom, e.g. "osm-z10", "osm-z11", ...

# Get the per-user tile usage:
query <- paste0("SET mapred.job.queue.name=nice;
SELECT
  date, style, zoom, scale, format, cache, user_id, is_automata, COUNT(1) AS n
FROM (
  SELECT
    '", opt$date, "' AS date,
    REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 1) AS style,
    REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 2) AS zoom,
    COALESCE(REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 6), '1') AS scale,
    REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 7) AS format,
    CONCAT(user_agent, client_ip) AS user_id,
    cache_status AS cache,
    CASE
      WHEN (
        agent_type = 'user' AND (
          user_agent RLIKE 'https?://'
          OR INSTR(user_agent, 'www.') > 0
          OR INSTR(user_agent, 'github') > 0
          OR LOWER(user_agent) RLIKE '([a-z0-9._%-]+@[a-z0-9.-]+\\.(com|us|net|org|edu|gov|io|ly|co|uk))'
          OR (
            user_agent_map['browser_family'] = 'Other'
            AND user_agent_map['device_family'] = 'Other'
            AND user_agent_map['os_family'] = 'Other'
          )
        )
      ) OR agent_type = 'spider' THEN 'TRUE'
      ELSE 'FALSE' END AS is_automata
  FROM wmf.webrequest
  WHERE
    webrequest_source = 'upload'
    AND ", date_clause, "
    AND uri_host = 'maps.wikimedia.org'
    AND http_status IN('200', '304')
    AND uri_path RLIKE '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$'
    AND uri_query <> '?loadtesting'
) prepared
WHERE zoom != '' AND style != ''
GROUP BY date, style, zoom, scale, format, cache, is_automata, user_id;")

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
    style = character(),
    zoom = character(),
    scale = character(),
    format = character(),
    cache = character(),
    users = numeric(),
    total = numeric(),
    average = numeric(),
    median = numeric(),
    percentile95 = numeric(),
    percentile99 = numeric()
  )
} else {
  # Exclude automata if requested:
  if (!opt$include_automata) {
    results <- results[is_automata == FALSE, ]
  }
  # The zoom sometimes exceeds what we actually allow (18). Yuri said that's acceptable but we
  # enlarge the images, so they're not actually getting zoom level 21-26 tiles.
  output <- results[, list(users = length(unique(user_id)),
                           total = sum(n),
                           average = round(mean(n)),
                           median = ceiling(median(n)),
                           percentile95 = ceiling(quantile(n, 0.95)),
                           percentile99 = ceiling(quantile(n, 0.99))),
                    by = c("date", "style", "zoom", "scale", "format", "cache")]
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
