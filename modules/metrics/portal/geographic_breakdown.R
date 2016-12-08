#!/usr/bin/env Rscript

.libPaths("/a/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option("--include_all", default = FALSE, action = "store_true",
              help = "Whether to output traffic breakdown across all countries")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}
# Build query:
date_clause <- as.character(as.Date(opt$date), format = "LEFT(timestamp, 8) = '%Y%m%d'")

query <- paste0("
SELECT
  DATE('", opt$date, "') AS date,
  timestamp AS ts,
  event_session_id AS session,
  UPPER(event_country) AS country,
  event_event_type AS type
FROM WikipediaPortal_15890769
WHERE ", date_clause, "
  AND (
    event_cohort IS NULL
    OR event_cohort IN ('null','baseline')
  )
  AND event_country != 'US'
  AND event_event_type IN('landing', 'clickthrough');
")
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
  if (opt$include_all) {
    output <- data.frame(
      date = character(),
      country = character(),
      events = numeric(),
      ctr = numeric(),
      n_visit = numeric(),
      ctr_visit = numeric(),
      n_session = numeric(),
      ctr_session = numeric()
    )
  } else {
    output <- data.frame(
      date = character(),
      country = character(),
      events = numeric()
    )
  }
} else {
  results$ts <- as.POSIXct(results$ts, format = "%Y%m%d%H%M%S")
  # Geography data that is common to both outputs:
  data("ISO_3166_1", package = "ISOcodes")
  # Remove accents because Reportupdater requires ASCII:
  ISO_3166_1$Name <- stringi::stri_trans_general(ISO_3166_1$Name, "Latin-ASCII")
  us_other_abb <- c("AS", "GU", "MP", "PR", "VI")
  us_other_mask <- match(us_other_abb, ISO_3166_1$Alpha_2)
  regions <- data.frame(abb = c(paste0("US:", c(as.character(state.abb), "DC")), us_other_abb),
                        region = paste0("U.S. (", c(as.character(state.region), "South", rep("Other",5)), ")"),
                        state = c(state.name, "District of Columbia", ISO_3166_1$Name[us_other_mask]),
                        stringsAsFactors = FALSE)
  regions$region[regions$region == "U.S. (North Central)"] <- "U.S. (Midwest)"
  regions$region[c(state.division == "Pacific", rep(FALSE, 5))] <- "U.S. (Pacific)" # see https://phabricator.wikimedia.org/T136257#2399411
  library(magrittr) # Required for piping
  if (opt$include_all) {
    # Generate all countries breakdown
    all_countries <- data.frame(abb = c(regions$abb, ISO_3166_1$Alpha_2[-us_other_mask]),
                                name = c(regions$region, ISO_3166_1$Name[-us_other_mask]),
                                stringsAsFactors = FALSE)
    data_w_countryname <- results %>%
      dplyr::mutate(country = ifelse(country %in% all_countries$abb, country, "Other")) %>%
      dplyr::left_join(all_countries, by = c("country" = "abb")) %>%
      dplyr::mutate(name = ifelse(is.na(name), "Other", name)) %>%
      dplyr::select(-country) %>% dplyr::rename(country = name)
    ctr_visit <- data_w_countryname %>%
      dplyr::arrange(session, ts) %>%
      dplyr::group_by(session) %>%
      dplyr::mutate(visit = cumsum(type == "landing")) %>%
      dplyr::group_by(date, country, session, visit) %>%
      dplyr::summarize(dummy_clt = sum(type == "clickthrough") > 0) %>%
      dplyr::group_by(country) %>%
      dplyr::summarize(n_visit = n(), ctr_visit = round(sum(dummy_clt)/n(), 4))
    ctr_session <- data_w_countryname %>%
      dplyr::group_by(date, country, session) %>%
      dplyr::summarize(dummy_clt = sum(type == "clickthrough") > 0) %>%
      dplyr::group_by(country) %>%
      dplyr::summarize(n_session = n(), ctr_session = round(sum(dummy_clt)/n(), 4))
    output <- data_w_countryname %>%
      dplyr::group_by(country) %>%
      dplyr::summarize(events = n(), ctr = round(sum(type == "clickthrough")/n(), 4)) %>%
      dplyr::mutate(date = results$date[1]) %>%
      dplyr::select(c(date, country, events, ctr)) %>%
      dplyr::arrange(desc(country)) %>%
      dplyr::left_join(ctr_visit, by = "country") %>%
      dplyr::left_join(ctr_session, by = "country")
  } else {
    # Generate by-country breakdown with regional data for US
    countries <- data.frame(abb = c(regions$abb, "GB", "CA",
                                    "DE", "IN", "AU", "CN",
                                    "RU", "PH", "FR"),
                            name = c(regions$region, "United Kingdom", "Canada",
                                     "Germany", "India", "Australia", "China",
                                     "Russia", "Philippines", "France"),
                            stringsAsFactors = FALSE)
    output <- results %>%
      dplyr::mutate(country = ifelse(country %in% countries$abb, country, "Other")) %>%
      dplyr::left_join(countries, by = c("country" = "abb")) %>%
      dplyr::mutate(name = ifelse(is.na(name), "Other", name)) %>%
      dplyr::select(-country) %>% dplyr::rename(country = name) %>%
      dplyr::group_by(country) %>%
      dplyr::summarize(events = n()) %>%
      dplyr::mutate(date = results$date[1]) %>%
      dplyr::select(c(date, country, events)) %>%
      dplyr::arrange(desc(country))
  }
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE, fileEncoding = "ASCII")
