#!/usr/bin/env Rscript

.libPaths("/a/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option("--metric", default = NA, action = "store", type = "character",
              help = "Available:
                  * search_api_cirrus
                  * search_zrr_overall
                  * wdqs_homepage
                  * wdqs_sparql"),
  make_option("--model", default = NA, action = "store", type = "character",
              help = "Available: ARIMA, BSTS"),
  make_option("--iters", default = 1000, action = "store", type = "numeric",
              help = "Number of MCMC iterations to keep in BSTS models [default %default]"),
  make_option("--burnin", default = 500, action = "store", type = "numeric",
              help = "Number of iterations to use as burn-in in BSTS models [default %default]")
)

read_data <- function(path, ...) {
  return(readr::read_tsv(file.path("/a/aggregate-datasets/discovery/", path), ...))
}

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date) || is.na(opt$model) || is.na(opt$metric)) {
  quit(save = "no", status = 1)
}

source("modules/forecasts/models.R")

if (grepl("^wdqs_", opt$metric)) {
  wdqs_usage <- read_data("wdqs/basic_usage.tsv", col_types = "Dclli") %>%
    dplyr::distinct(date, path, http_success, is_automata, .keep_all = TRUE) %>%
    dplyr::filter(http_success & !is_automata) %>%
    dplyr::filter(date < as.Date(opt$date)) %>%
    dplyr::select(c(date, path, events)) %>%
    tidyr::spread(path, events) %>%
    magrittr::set_colnames(c("date", "homepage", "ldf", "sparql")) %>%
    { xts::xts(.[, -1], order.by = .$date) }
}

output <- switch(
  opt$metric,

  "search_api_cirrus" = {
    api_usage <- read_data("search/search_api_usage.tsv", col_types = "Dci") %>%
      dplyr::filter(date < as.Date(opt$date)) %>%
      dplyr::arrange(date, api) %>%
      dplyr::distinct(date, api, .keep_all = TRUE) %>%
      tidyr::spread(api, calls) %>%
      { xts::xts(.[, -1], order.by = .$date) }
    if (opt$model == "ARIMA") {
      try(
        forecast_arima(api_usage[, "cirrus"], arima_params = list(order = c(0, 1, 2), seasonal = list(order = c(2, 1, 1), period = 7)))
      )
    } else { # BSTS
      forecast_bsts(api_usage[, "cirrus"], transformation = "log", ar_lags = 1, n_iter = opt$iters, burn_in = opt$burnin)
    }
  },

  "search_zrr_overall" = {
    zrr_overall <- read_data("search/cirrus_query_aggregates_no_automata.tsv", col_types = "Dd") %>%
      dplyr::arrange(date, rate) %>%
      dplyr::distinct(date, .keep_all = TRUE) %>%
      dplyr::filter(date < as.Date(opt$date)) %>%
      { xts::xts(.[, -1], order.by = .$date) }
    if (opt$model == "ARIMA") {
      try(
        forecast_arima(zrr_overall[, "rate"], arima_params = list(order = c(2, 1, 2), seasonal = list(order = c(1, 0, 0), period = 7)))
      )
    } else { # BSTS
      forecast_bsts(zrr_overall[, "rate"], transformation = "logit", ar_lags = 1, n_iter = opt$iters, burn_in = opt$burnin)
    }
  },

  "wdqs_homepage" = {
    if (opt$model == "ARIMA") {
      try(
        forecast_arima(wdqs_usage[, "homepage"], transformation = "log", arima_params = list(order = c(1, 1, 1), seasonal = list(order = c(1, 0, 0), period = 7)))
      )
    } else { # BSTS
      forecast_bsts(wdqs_usage[, "homepage"], transformation = "log", ar_lags = 1, n_iter = opt$iters, burn_in = opt$burnin)
    }
  },

  "wdqs_sparql" = {
    if (opt$model == "ARIMA") {
      try(
        forecast_arima(wdqs_usage[, "sparql"], transformation = "log", arima_params = list(order = c(1, 1, 2), seasonal = list(order = c(1, 0, 0), period = 7)))
      )
    } else { # BSTS
      forecast_bsts(wdqs_usage[, "sparql"], transformation = "log", ar_lags = 1, n_iter = opt$iters, burn_in = opt$burnin)
    }
  }

)

write.table(cbind(date = opt$date, round(output, 4)), file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
