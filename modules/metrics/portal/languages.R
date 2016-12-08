#!/usr/bin/env Rscript

.libPaths("/a/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option(c("-o", "--output"), default = NA, action = "store", type = "character",
              help = "Available: destination, switching")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date) || is.na(opt$output)) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "LEFT(timestamp, 8) = '%Y%m%d'")

query <- paste0("
SELECT
  DATE('", opt$date, "') AS date,
  event_session_id AS session,
  event_destination AS destination,
  event_event_type AS type,
  event_section_used AS section_used,
  event_selected_language AS selected_language
FROM WikipediaPortal_15890769
WHERE ", date_clause, "
  AND (
    event_cohort IS NULL
    OR event_cohort IN ('null','baseline')
  )
  AND event_country != 'US'
  AND (NOT INSTR(event_destination, 'translate.googleusercontent.com') OR event_destination IS NULL)
  AND event_event_type IN('landing', 'clickthrough', 'select-language');
")

suppressPackageStartupMessages(library(data.table))

# Fetch data from MySQL database:
results <- tryCatch(
  suppressMessages(as.data.table(wmf::mysql_read(query, "log"))),
  error = function(e) {
    return(data.frame())
  }
)

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  output <- switch(
    opt$output,
    destination = {
      data.frame(
        date = character(),
        prefix = character(),
        clicks = numeric(),
        search = numeric(),
        primary = numeric(),
        secondary = numeric()
      )
    },
    switching = {
      data.frame(
        date = character(),
        sessions = numeric(),
        switched = numeric()
      )
    }
  )
} else {
  suppressWarnings(output <- switch(
    opt$output,
    destination = {
      results <- results[type == "clickthrough", ]
      # Extract the prefix:
      results$prefix <- sub("^https?://(.*)\\.wikipedia\\.org.*", "\\1", results$destination)
      results$prefix[results$section_used == "search" & grepl("search-redirect.php", results$destination, fixed = TRUE)] <- results$selected_language[results$section_used == "search" & grepl("search-redirect.php", results$destination, fixed = TRUE)]
      wmf::set_proxies() # to allow for the latest prefixes to be retrieved
      # Update the internal dataset of prefixes and languages:
      suppressMessages(try(polloi::update_prefixes(), silent = TRUE))
      # Only keep the data with valid prefixes:
      suppressMessages(prefixes <- polloi::get_prefixes()$prefix)
      results <- results[prefix %in% prefixes, ]
      # Aggregate:
      results <- results[order(results$date, results$prefix), ]
      results[, list(
        clicks = .N, sessions = length(unique(session)),
        search = sum(section_used == "search"),
        primary = sum(section_used == "primary links"),
        secondary = sum(section_used == "secondary links")
      ), by = c("date", "prefix")]
    },
    switching = {
      # Language switching by session
      interim <- results[type %in% c("landing", "select-language"),
                         list(landings = sum(type == "landing"),
                              switches = sum(type == "select-language")),
                         by = c("date", "session")]
      interim[landings > 0, list(sessions = .N, switched = sum(switches > 0)), by = "date"]
    }
  ))
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
