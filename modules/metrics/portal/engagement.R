#!/usr/bin/env Rscript

.libPaths("/srv/discovery/r-library"); suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character"),
  make_option(c("-o", "--output"), default = NA, action = "store", type = "character",
              help = "Available:
                  * clickthrough_rate
                  * clickthrough_breakdown (can be broken down by country)
                  * clickthrough_firstvisit (can be broken down by country)
                  * clickthrough_sisterprojects
                  * most_common_per_visit (can be broken down by country)
                  * clickthrough_by_device
                  * mobile_use_us_elsewhere"),
  make_option("--by_country", default = FALSE, action = "store_true",
              help = "Whether to break output down across all countries")
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
  timestamp AS ts,
  event_session_id AS session,
  UPPER(event_country) AS country,
  event_destination AS destination,
  event_event_type AS type,
  event_section_used AS section_used,
  userAgent AS user_agent
FROM WikipediaPortal_15890769
WHERE ", date_clause, "
  AND (
    event_cohort IS NULL
    OR event_cohort IN ('null','baseline')
  )
  AND event_country != 'US'
  AND event_event_type IN('landing', 'clickthrough')
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
  if (opt$by_country) {
    output <- switch(
      opt$output,
      clickthrough_breakdown = data.frame(
        date = character(),
        section_used = character(),
        country = character(),
        events = numeric(),
        proportion = numeric()
      ),
      clickthrough_firstvisit = data.frame(
        date = character(),
        section_used = character(),
        country = character(),
        sessions = numeric(),
        proportion = numeric()
      ),
      most_common_per_visit = data.frame(
        date = character(),
        section_used = character(),
        country = character(),
        visits = numeric(),
        proportion = numeric()
      )
    )
  } else {
    output <- switch(
      opt$output,
      clickthrough_rate = data.frame(
        date = character(),
        type = character(),
        events = numeric()
      ),
      clickthrough_breakdown = data.frame(
        date = character(),
        section_used = character(),
        events = numeric()
      ),
      clickthrough_firstvisit = dplyr::data_frame(
        date = character(),
        `no action` = numeric(),
        `primary links` = numeric(),
        `search` = numeric(),
        `secondary links` = numeric(),
        `other languages` = numeric(),
        `other projects` = numeric()
      ),
      clickthrough_sisterprojects = data.frame(
        date = character(),
        destination = character(),
        users = numeric(),
        clicks = numeric()
      ),
      most_common_per_visit = data.frame(
        date = character(),
        section_used = character(),
        visits = numeric()
      ),
      clickthrough_by_device = data.frame(
        date = character(),
        device = character(),
        n_sessions = numeric(),
        clickthrough = numeric()
      ),
      mobile_use_us_elsewhere = data.frame(
        date = character(),
        region = character(),
        n_mobile = numeric(),
        n_sessions = numeric()
      )
    )
  }
} else {
  results$section_used[is.na(results$section_used)] <- "no action"
  results$ts <- as.POSIXct(results$ts, format = "%Y%m%d%H%M%S")
  library(magrittr) # Required for piping
  # 'data_w_countryname' is used in calculation of metrics when 'by_country' is enabled
  if (opt$by_country) {
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

    all_countries <- data.frame(abb = c(regions$abb, ISO_3166_1$Alpha_2[-us_other_mask]),
                                name = c(regions$region, ISO_3166_1$Name[-us_other_mask]),
                                stringsAsFactors = FALSE)
    data_w_countryname <- results %>%
      dplyr::mutate(country = ifelse(country %in% all_countries$abb, country, "Other")) %>%
      dplyr::left_join(all_countries, by = c("country" = "abb")) %>%
      dplyr::mutate(name = ifelse(is.na(name), "Other", name)) %>%
      dplyr::select(-country) %>% dplyr::rename(country = name)
  }

  output <- switch(
    opt$output,
    clickthrough_rate = {
      results %>%
        dplyr::group_by(date, type) %>%
        dplyr::summarize(events = n()) %>%
        dplyr::ungroup()
    },
    clickthrough_sisterprojects = {
      results %>%
        dplyr::filter(section_used == "other projects") %>%
        dplyr::filter(
          destination != "https://en.wikipedia.org/wiki/List_of_Wikipedia_mobile_applications",
          !grepl("(https://itunes.apple.com/|https://play.google.com/)", destination)
        ) %>%
        dplyr::mutate(destination = sub("^https?://(www.)?(.*)/$", "\\2", destination)) %>%
        dplyr::group_by(date, destination) %>%
        dplyr::summarize(users = length(unique(session)), clicks = n()) %>%
        dplyr::ungroup()
    },
    clickthrough_breakdown = {
      if (opt$by_country) {
        data_w_countryname %>%
          dplyr::arrange(ts) %>%
          dplyr::filter(!duplicated(session, fromLast = TRUE)) %>%
          dplyr::group_by(date, section_used, country) %>%
          dplyr::summarize(events = n()) %>%
          dplyr::mutate(proportion = round(events/sum(events), 4)) %>%
          dplyr::ungroup()
      } else {
        results %>%
          dplyr::arrange(ts) %>%
          dplyr::filter(!duplicated(session, fromLast = TRUE)) %>%
          dplyr::group_by(date, section_used) %>%
          dplyr::summarize(events = n()) %>%
          dplyr::ungroup()
      }
    },
    clickthrough_firstvisit = {
      possible_sections <- data.frame(
        section_used = c("no action", "primary links", "search",
                         "secondary links", "other languages", "other projects"),
        stringsAsFactors = FALSE
      )
      if (opt$by_country) {
        data_w_countryname %>%
          dplyr::arrange(session, ts) %>%
          dplyr::group_by(session) %>%
          dplyr::mutate(visit = cumsum(type == "landing")) %>%
          dplyr::filter(visit == 1) %>%
          dplyr::group_by(date, section_used, country) %>%
          dplyr::summarize(sessions = n()) %>%
          dplyr::mutate(proportion = round(sessions/sum(sessions), 4)) %>%
          dplyr::ungroup()
      } else {
        results %>%
          dplyr::arrange(session, ts) %>%
          dplyr::group_by(session) %>%
          dplyr::mutate(visit = cumsum(type == "landing")) %>%
          dplyr::filter(visit == 1) %>%
          dplyr::group_by(section_used) %>%
          dplyr::summarize(sessions = n()) %>%
          dplyr::mutate(proportion = round(sessions/sum(sessions), 4)) %>%
          dplyr::select(-sessions) %>%
          dplyr::right_join(possible_sections, by = "section_used") %>%
          dplyr::mutate(proportion = ifelse(is.na(proportion), 0, proportion)) %>%
          tidyr::spread(section_used, proportion) %>%
          dplyr::mutate(date = results$date[1]) %>%
          dplyr::select(c(date, `no action`, `primary links`, `search`, `secondary links`, `other languages`, `other projects`))
      }
    },
    most_common_per_visit = {
      if (opt$by_country) {
        data_w_countryname %>%
          dplyr::arrange(session, ts) %>%
          dplyr::group_by(session) %>%
          dplyr::mutate(visit = cumsum(type == "landing")) %>%
          dplyr::filter(type == "clickthrough") %>%
          dplyr::group_by(date, country, session, visit, section_used) %>%
          dplyr::tally() %>%
          dplyr::top_n(1, n) %>%
          dplyr::ungroup() %>%
          dplyr::group_by(date, section_used, country) %>%
          dplyr::summarize(visits = n()) %>%
          dplyr::mutate(proportion = round(visits/sum(visits), 4)) %>%
          dplyr::ungroup()
      } else {
        results %>%
          dplyr::arrange(session, ts) %>%
          dplyr::group_by(session) %>%
          dplyr::mutate(visit = cumsum(type == "landing")) %>%
          dplyr::filter(type == "clickthrough") %>%
          dplyr::group_by(date, session, visit, section_used) %>%
          dplyr::tally() %>%
          dplyr::top_n(1, n) %>%
          dplyr::ungroup() %>%
          dplyr::group_by(date, section_used) %>%
          dplyr::summarize(visits = n()) %>%
          dplyr::ungroup()
      }
    },
    clickthrough_by_device = {
      results %>%
        cbind(purrr::map_df(.$user_agent, ~ wmf::null2na(jsonlite::fromJSON(.x, simplifyVector = FALSE)))) %>%
        dplyr::mutate(
          device = dplyr::if_else(browser_family %in% c("Opera Mini") | grepl("^Symbian", os_family) |
            os_family %in% c("iOS", "Android", "Firefox OS", "BlackBerry OS", "Chrome OS", "Kindle", "Windows Phone") |
            grepl("(phone)|(mobile)|(tablet)|(lumia)", device_family, ignore.case = TRUE), "mobile", "desktop")
        ) %>%
        dplyr::group_by(date, device, session) %>%
        dplyr::filter("landing" %in% type) %>%
        dplyr::summarize(clickthrough = any(type == "clickthrough")) %>%
        dplyr::summarize(
          n_sessions = n(),
          clickthrough = sum(clickthrough)
          ) %>%
        dplyr::ungroup()
    },
    mobile_use_us_elsewhere = {
      results %>%
        cbind(purrr::map_df(.$user_agent, ~ wmf::null2na(jsonlite::fromJSON(.x, simplifyVector = FALSE)))) %>%
        dplyr::mutate(
          is_mobile = browser_family %in% c("Opera Mini") | grepl("^Symbian", os_family) |
            os_family %in% c("iOS", "Android", "Firefox OS", "BlackBerry OS", "Chrome OS", "Kindle", "Windows Phone") |
            grepl("(phone)|(mobile)|(tablet)|(lumia)", device_family, ignore.case = TRUE),
          region = dplyr::if_else(grepl("^US:", country), "United States", "Everywhere else")
        ) %>%
        dplyr::group_by(date, region, session) %>%
        dplyr::filter("landing" %in% type) %>%
        dplyr::summarize(is_mobile = all(is_mobile)) %>%
        dplyr::summarize(n_mobile = sum(is_mobile), n_sessions = n())
    }
  )
}

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
