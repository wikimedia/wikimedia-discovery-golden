#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)

# Check dependencies:
dependencies <- c(
  # Essentials:
  "devtools", "testthat", "Rcpp",
  "tidyverse", "data.table", "plyr",
  "optparse", "yaml", "data.tree",
  "knitr", "glue",
  # For forecasting modules:
  "bsts", "forecast", "prophet",
  # For querying, etc.:
  "ISOcodes", "uaparser", "ortiz", "wmf", "polloi"
)

installed <- as.data.frame(installed.packages(), stringsAsFactors = FALSE)
if (any(!dependencies %in% unname(installed$Package))) {
  stop("The following R package(s) are required but are missing: ", paste0(dependencies[!dependencies %in% installed$Package], collapse = ", "))
}

suppressPackageStartupMessages({
  library("methods")
  library("optparse")
})

option_list <- list(
  make_option("--start_date", default = as.character(Sys.Date() - 1, "%Y-%m-%d"), action = "store", type = "character"),
  make_option("--end_date", default = as.character(Sys.Date(), "%Y-%m-%d"), action = "store", type = "character",
              help = "This is required for proper Reportupdater emulation; should be 'start_date' + 1"),
  make_option("--omit_times", default = FALSE, action = "store_true",
              help = "Do not include a table of execution times in addition to basic statistics"),
  make_option("--include_samples", default = FALSE, action = "store_true",
              help = "Whether to print head & tail of existing datasets"),
  make_option("--disable_metrics", default = FALSE, action = "store_true",
              help = "Skip metrics modules to make the test run shorter"),
  make_option("--disable_forecasts", default = FALSE, action = "store_true",
              help = "Skip forecasting modules to make the test run shorter"),
  make_option("--forecast_iters", default = 100, action = "store", type = "numeric",
              help = "Overrides number of MCMC iterations used in BSTS models [default %default]"),
  make_option("--forecast_burnin", default = 50, action = "store", type = "numeric",
              help = "Overrides number of MCMC iterations discarded in BSTS models [default %default]")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (opt$disable_metrics && opt$disable_forecasts) {
  stop("Cannot run test utility with metrics AND forecasting modules disabled.")
}

# Other packages used:
suppressPackageStartupMessages(library(lubridate))
suppressPackageStartupMessages(library(knitr))

# Build a table of modules and reports within them:
config_yamls <- list.files(pattern = "^config\\.yaml$", recursive = TRUE)
names(config_yamls) <- sub("modules/", "", dirname(config_yamls), fixed = TRUE)
reports <- dplyr::bind_rows(lapply(config_yamls, function(path) {
  config_yaml <- suppressMessages(suppressWarnings(data.tree::as.Node(yaml::yaml.load_file(path))))
  reports <- data.tree::ToDataFrameTable(config_yaml[["reports"]], "report" = "name", "type", "funnel", "max_data_points", "description")
  reports$path = paste0(file.path(dirname(path), reports$report), ifelse(reports$type == "sql", ".sql", ""))
  return(reports)
}), .id = "module")

# Give preference to metrics over forecasts, SQL reports over script-type reports:
reports <- reports[order(sub("^(forecasts|metrics)/.*$", "\\1", reports$module),
                         sub("^(forecasts|metrics)/(.*)$", "\\2", reports$module),
                         reports$type, reports$report,
                         decreasing = TRUE), ]

if (opt$disable_metrics) {
  reports <- reports[!grepl("metrics/", reports$module, fixed = TRUE), ]
}
if (opt$disable_forecasts) {
  reports <- reports[!grepl("forecasts/", reports$module, fixed = TRUE), ]
}

elapsed_total <- numeric(nrow(reports))
rownames(reports) <- NULL

# Execute the reports the way update_reports.py would, but without checking for missingness and without writing data to files:
message("# Test Run\n\n## Options")
print(kable(tidyr::gather(dplyr::bind_cols(opt), option, value), format = "markdown"))
used_pkgs <- installed[installed$Package %in% dependencies, c("Package", "Version")]
message("\n## Dependencies\n\nUsing the following packages from **", .libPaths()[1], "**: ",
        paste0(paste0(used_pkgs$Package, " (", used_pkgs$Version, ")"), collapse = ", "))
message("\n## Reports")
for (i in 1:nrow(reports)) {
  message("\n### Report ", i, " of ", nrow(reports), "\n")
  if (!is.na(reports$description[i])) {
    message("_", reports$description[i], "_\n")
  }
  if (!is.na(reports$funnel[i]) && reports$funnel[i]) {
    message("**Note**: this report is configured to allow more than one row per day.\n")
  } else {
    message("**Note**: this report is configured to have one row per day.\n")
  }
  if (!is.na(reports$max_data_points[i])) {
    message("**Note**: this report is configured to be \"rolling\" -- ", reports$max_data_points[i], " maximum number of days allowed in the final dataset.\n")
  }
  if (opt$include_samples) {
    filename <- file.path(sub("(metrics|forecasts)/", ifelse(grepl("forecasts", reports$module[i], fixed = TRUE), "/a/aggregate-datasets/discovery-forecasts/", "/a/aggregate-datasets/discovery/"), reports$module[i]), paste0(reports$report[i], ".tsv"))
    if (file.exists(filename)) {
      message("\nSome of the existing data in **", filename, "**:\n\n```")
      system(paste("head -n 3", filename))
      message("...")
      suppressWarnings(suppressMessages(existing_data <- readr::read_tsv(filename)))
      if (any(existing_data$date == opt$start_date)) {
        write.table(existing_data[existing_data$date == opt$start_date, ], file = "", append = FALSE, sep = "\t", row.names = FALSE, col.names = FALSE, quote = FALSE)
        message("...")
      }
      system(paste("tail -n 2", filename))
      message("```")
    } else {
      message("\n<span style=\"font-weight:bold;color:red;\">", filename, " does not exist!<span>")
    }
  }
  message('1. Executing report "', reports$report[i], '" from the ', reports$module[i], ' module.')
  if (reports$type[i] == "script") {
    if (grepl("forecasts", reports$module[i], fixed = TRUE) && grepl("_bsts$", reports$path[i])) {
      # When testing out BSTS forecasting, we need to intercept the command and specify iters and burnin options:
      command <- grep("^Rscript", readr::read_lines(reports$path[i]), value = TRUE)
      command <- paste0(command, " --iters=", opt$forecast_iters, " --burnin=", opt$forecast_burnin)
      command <- sub("$1", opt$end_date, command, fixed = TRUE)
    } else {
      command <- paste("sh", reports$path[i], opt$start_date, opt$end_date)
    }
    message("2. About to run the following command: `", command, "`")
    message("\n**Output**:\n\n```")
    elapsed <- system.time(system(command))["elapsed"]
    message("```")
  } else {
    message("2. Filling in the timestamp placeholders in the SQL query.")
    query <- paste0(gsub("{to_timestamp}", gsub("-", "", opt$end_date, fixed = TRUE), gsub("{from_timestamp}", gsub("-", "", opt$start_date, fixed = TRUE), readLines(reports$path[i]), fixed = TRUE), fixed = TRUE), collapse = "\n")
    message("3. About to run the following query:\n")
    message("```SQL\n", query, "\n```")
    elapsed <- system.time(tryCatch({
      results <- suppressMessages(wmf::mysql_read(query, "log"))
      message("\n**Output**:\n\n```")
      write.table(results, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
      message("```")
    }, error = function(e) {
      message("\n<span style=\"font-weight:bold;color:red;\">Encountered a problem: ", e, "<span>")
    }))["elapsed"]
  }
  message("\nIt took ", tolower(seconds_to_period(ceiling(as.numeric(elapsed)))), " to generate this report.")
  elapsed_total[i] <- elapsed
}

message("\n## Summary Statistics\n")
reports$seconds_elapsed <- ceiling(elapsed_total) # otherwise seconds_to_period's output will be funky
reports <- reports[order(elapsed_total, decreasing = FALSE), ]
reports$time_elapsed <- tolower(seconds_to_period(reports$seconds_elapsed))
message("* It took ", tolower(seconds_to_period(ceiling(sum(elapsed_total)))), " overall to generate these ", nrow(reports), " reports.")
message('* The quickest report took ', head(reports$time_elapsed, 1), ' and it was "', head(reports$report, 1), '" (', head(reports$type, 1), ') from the ', head(reports$module, 1), ' module.')
message('* The slowest report took ', tail(reports$time_elapsed, 1), ' and it was "', tail(reports$report, 1), '" (', tail(reports$type, 1), ') from the ', tail(reports$module, 1), ' module.')
message("* The median is ", tolower(seconds_to_period(ceiling(median(elapsed_total)))), " per report. The average is ", tolower(seconds_to_period(ceiling(mean(elapsed_total)))), " per report.")
if (!opt$omit_times) {
  message("\nHere are this run's times:\n")
  print(kable(reports[, c("module", "report", "type", "time_elapsed")], format = "markdown", row.names = FALSE))
}
