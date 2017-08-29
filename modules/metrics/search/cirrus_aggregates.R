#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option(c("-o", "--output"), default = "overall", action = "store",
              help = "Available: [default %default], breakdown, suggestion, langproj"),
  make_option("--include_automata", default = FALSE, action = "store_true",
              help = "Whether to include automata [default %default]")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date) || !(opt$output %in% c("overall", "breakdown", "suggestion", "langproj"))) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "year = %Y AND month = %m AND day = %d")

query <- paste0("SET mapred.job.queue.name=nice;
ADD JAR hdfs:///wmf/refinery/current/artifacts/refinery-hive.jar;
CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
USE wmf_raw;
SELECT
  date,
  wiki_id,
  has_suggestion,
  requested_suggestion,
  query_type,
  is_automata,
  COUNT(1) AS total,
  SUM(IF(zero_result, 1, 0)) AS zero_results
FROM (
  SELECT
    '", opt$date, "' AS date,
    wikiid AS wiki_id,
    IF(length(concat_ws('', requests.suggestion)) > 0, 'TRUE', 'FALSE') AS has_suggestion,
    IF(array_contains(requests.suggestionrequested, TRUE), 'TRUE', 'FALSE') AS requested_suggestion,
    CASE WHEN requests[size(requests)-1].querytype = 'degraded_full_text' THEN 'full_text'
         ELSE requests[size(requests)-1].querytype
         END AS query_type,
    array_sum(requests.hitstotal, -1) = 0 AS zero_result,
    CASE WHEN (
           ua_parser(useragent)['device_family'] = 'Spider'
           OR is_spider(useragent)
           OR ip = '127.0.0.1'
           OR useragent RLIKE 'https?://'
           OR INSTR(useragent, 'www.') > 0
           OR INSTR(useragent, 'github') > 0
           OR LOWER(useragent) RLIKE '([a-z0-9._%-]+@[a-z0-9.-]+\\.(com|us|net|org|edu|gov|io|ly|co|uk))'
           OR (
             ua_parser(useragent)['browser_family'] = 'Other'
             AND ua_parser(useragent)['device_family'] = 'Other'
             AND ua_parser(useragent)['os_family'] = 'Other'
           )
         ) THEN 'TRUE'
         ELSE 'FALSE'
         END AS is_automata
  FROM CirrusSearchRequestSet
  WHERE
    ", date_clause, "
    AND NOT array_contains(requests.hitstotal, -1)
    AND requests[size(requests)-1].querytype IN('comp_suggest', 'full_text', 'GeoData_spatial_search', 'prefix', 'more_like', 'regex')
) AS data_source
WHERE query_type != ''
GROUP BY
  date,
  wiki_id,
  has_suggestion,
  requested_suggestion,
  query_type,
  is_automata;")

suppressPackageStartupMessages(library(data.table))

# Fetch data from database using Hive:
results <- tryCatch(
  as.data.table(wmf::query_hive(query)),
  error = function(e) {
    return(data.frame())
  }
)

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  output <- switch(
    opt$output,
    overall = data.frame(
      date = character(),
      rate = numeric()
    ),
    breakdown = data.frame(
      date = character(),
      query_type = character(),
      rate = numeric()
    ),
    suggestion = data.frame(
      date = character(),
      rate = numeric()
    ),
    langproj = data.frame(
      date = character(),
      language = character(),
      project = character(),
      zero_results = numeric(),
      total = numeric()
    )
  )
} else {
  # Exclude automata if requested:
  if (!opt$include_automata) {
    results <- results[is_automata == FALSE, ]
  }
  suppressWarnings(output <- switch(
    opt$output,
    overall = results[, list(rate = round(sum(zero_results)/sum(total), 4)), by = "date"],
    breakdown = results[, list(rate = round(sum(zero_results)/sum(total), 4)), by = c("date", "query_type")],
    suggestion = results[has_suggestion == TRUE, list(rate = round(sum(zero_results)/sum(total), 4)), by = "date"],
    langproj = {
      wmf::set_proxies() # to allow for the latest prefixes to be retrieved.
      # Update the internal dataset of prefixes and languages:
      suppressMessages(try(polloi::update_prefixes(), silent = TRUE))
      suppressMessages(lang_proj <- polloi::parse_wikiid(results$wiki_id))
      # Remove accents because Reportupdater requires ASCII:
      lang_proj$language <- stringi::stri_trans_general(lang_proj$language, "Latin-ASCII")
      results <- cbind(results, lang_proj) # add parsed languages and projects to results.
      results[is.na(project) == FALSE,
              list(zero_results = sum(zero_results), total = sum(total)),
              by = c("date", "language", "project")]
    }
  ))
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
