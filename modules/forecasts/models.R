# .libPaths("/a/discovery/r-library")

suppressPackageStartupMessages(suppressWarnings(suppressMessages({
  library(magrittr) # install.packages("tidyverse")
  library(xts) # install.packages("xts")
  library(bsts) # install.packages("bsts")
  library(forecast) # install.packages("forecast")
})))

forecast_arima <- function(
  x, # a 1-column xts object
  arima_params = NULL, # list w/ order & seasonal components
  bootstrap_ci = FALSE, # If TRUE, then prediction intervals computed using simulation with resampled errors
  bootstrap_npaths = 5000,
  transformation = c("none", "log", "logit", "in millions")
) {
  if (is.null(arima_params)) {
    arima_params <- list(order = c(0L, 0L, 0L),
                         seasonal = list(order = c(0L, 0L, 0L), period = NA))
  }
  if (!(transformation[1] %in% c("none", "log", "logit", "in millions"))) {
    stop("transformation must be one of: 'none', 'log', 'logit', 'in millions'")
  }
  transforms <- list(
    "none" = list(
      to = identity,
      from = identity
    ),
    "log" = list(
      to = log10,
      from = function(x) { return(10^x) }
    ),
    "logit" = list(
      to = function(p) { return(log(p/(1-p))) },
      from = function(x) { return(exp(x)/(exp(x)+1)) }
    ),
    "in millions" = list(
      to = function(x) { return(x/1e6) },
      from = function(x) { return(x*1e6) }
    )
  )
  transform <- transforms[[transformation[1]]]
  # Fit:
  fit <- arima(transform$to(x), order = arima_params$order, seasonal = arima_params$seasonal)
  # Forecast:
  predicted <- forecast(fit, h = 1, level = c(80, 95), bootstrap = bootstrap_ci, npaths = bootstrap_npaths)
  # Post-processing:
  output <- predicted %>%
    as.data.frame %>%
    lapply(transform$from) %>%
    as.data.frame
  names(output) <- c("point_est", "lower_80", "upper_80", "lower_95", "upper_95")
  # Return:
  return(output)
}

forecast_bsts <- function(
  x, # a 1-column xts object
  n_iter = 1e3, burn_in = 5e2,
  transformation = c("none", "log", "logit", "in millions"),
  ar_lags = NULL
) {
  if (!is.null(ar_lags)) {
    if (!is.numeric(ar_lags)) {
      stop("ar_lags must be numeric")
    }
  }
  if (!(transformation[1] %in% c("none", "log", "logit", "in millions"))) {
    stop("transformation must be one of: 'none', 'log', 'logit', 'in millions'")
  }
  transforms <- list(
    "none" = list(
      to = identity,
      from = identity
    ),
    "log" = list(
      to = log10,
      from = function(x) { return(10^x) }
    ),
    "logit" = list(
      to = function(p) { return(log(p/(1-p))) },
      from = function(x) { return(exp(x)/(exp(x)+1)) }
    ),
    "in millions" = list(
      to = function(x) { return(x/1e6) },
      from = function(x) { return(x*1e6) }
    )
  )
  transform <- transforms[[transformation[1]]]
  # Pre-processing because Some days may be missing, so this ensures that we
  # have something for every day, even if that something is a NA.
  temp <- data.frame(date = index(x), response = transform$to(as.numeric(x)))
  date_range <- range(index(x))
  days <- data.frame(date = seq(date_range[1], date_range[2], "day"))
  temp <- dplyr::left_join(days, temp, by = "date")
  y <- xts(temp[, -1, drop = FALSE], temp$date)
  # State Specifications:
  ss <- AddLocalLinearTrend(list(), y)
  ss <- AddSeasonal(ss, y, nseasons = 7, season.duration = 1) # Weekly seasonality
  ss <- AddSeasonal(ss, y, nseasons = 4, season.duration = 7) # Monthly seasonality
  ss <- AddNamedHolidays(ss, NamedHolidays(), y)
  if (!is.null(ar_lags)) {
    ss <- AddAr(ss, y, lags = ar_lags)
  }
  # Fit:
  model <- bsts(y, family = "gaussian",
                state.specification = ss,
                niter = burn_in + n_iter, seed = 0,
                ping = 0)
  # Forecast:
  predicted <- predict(model, horizon = 1, burn = burn_in)
  # Post-processing:
  output <- predicted$distribution %>%
    as.numeric %>%
    quantile(c(0.5, 0.1, 0.9, 0.025, 0.975)) %>%
    matrix %>%
    t %>%
    apply(1:2, transform$from) %>%
    as.data.frame %>%
    round(3)
  names(output) <- c("point_est", "lower_80", "upper_80", "lower_95", "upper_95")
  if (!is.data.frame(output)) {
    stop("Output is not a data frame for some reason?!?")
  }
  # Return:
  return(output)
}
