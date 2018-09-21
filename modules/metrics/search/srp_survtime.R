#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("optparse")
  library("glue")
  library("magrittr")
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

yyyymmdd <- format(as.Date(opt$date), "%Y%m%d")
revision_number <- dplyr::case_when(
  as.Date(opt$date) < "2017-02-10" ~ "15922352",
  as.Date(opt$date) < "2017-06-29" ~ "16270835",
  TRUE ~ "16909631"
)

query <- glue("SELECT
  timestamp AS ts, wiki,
  event_uniqueId AS event_id,
  event_searchSessionId AS session_id,
  event_pageViewId AS page_id,
  event_action AS event,
  event_checkin AS checkin
FROM TestSearchSatisfaction2_{revision_number}
WHERE
  LEFT(timestamp, 8) = '{yyyymmdd}'
  AND wiki RLIKE 'wiki$'
  AND NOT wiki RLIKE '^(arbcom)|(be_x_old)'
  AND NOT wiki IN('commonswiki', 'mediawikiwiki', 'metawiki', 'checkuserwiki', 'donatewiki', 'collabwiki', 'foundationwiki', 'incubatorwiki', 'legalteamwiki', 'officewiki', 'outreachwiki', 'sourceswiki', 'specieswiki', 'stewardwiki', 'wikidatawiki', 'wikimania2017wiki', 'movementroleswiki', 'internalwiki', 'otrs_wikiwiki', 'projectcomwiki', 'ombudsmenwiki', 'votewiki', 'chapcomwiki', 'nostalgiawiki', 'otrs_wikiwiki')
  AND event_source = 'autocomplete'
  AND event_subTest IS NULL
  AND event_articleId IS NULL
  AND event_action IN('visitPage', 'checkin');")

# Fetch data from MySQL database:
results <- tryCatch(
  suppressMessages(wmf::mysql_read(query, "log")),
  error = function(e) {
    return(data.frame())
  }
)

empty_df <- function() {
  data.frame(
    date = character(),
    language = character(),
    LD10 = character(),
    LD25 = character(),
    LD50 = character(),
    LD75 = character(),
    LD90 = character(),
    LD95 = character(),
    LD99 = character()
  )
}

if (nrow(results) == 0) {
  # Here we make the script output tab-separated
  # column names, as required by Reportupdater:
  srp_survivorship <- empty_df()
} else {
  results %<>%
    dplyr::mutate(ts = lubridate::ymd_hms(ts)) %>%
    dplyr::arrange(session_id, event_id, ts) %>%
    dplyr::distinct(session_id, event_id, .keep_all = TRUE) %>%
    dplyr::arrange(wiki, session_id, page_id, desc(event), ts) %>%
    dplyr::select(wiki, ts, session_id, page_id, event, checkin) %>%
    dplyr::group_by(session_id, page_id) %>%
    dplyr::filter(event == "visitPage" | (event == "checkin" & checkin == max(checkin, na.rm = TRUE))) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(language = dplyr::case_when(
      wiki == "enwiki" ~ "English",
      wiki %in% c("cawiki", "frwiki") ~ "French and Catalan",
      TRUE ~ "Other languages"
    )) %>%
    data.table::data.table(key = c("wiki", "language", "session_id", "page_id"))

  ## Calculates the median lethal dose (LD50) and other.
  ## LD50 = the time point at which we have lost 50% of our users.
  checkins <- c(0, 10, 20, 30, 40, 50, 60, 90, 120, 150, 180, 210, 240, 300, 360, 420)
  # ^ this will be used for figuring out the interval bounds for each check-in
  # Treat each individual search session as its own thing, rather than belonging
  #   to a set of other search sessions by the same user.
  page_visits <- results[, {
    if (any(.SD$event == "checkin")) {
      last_checkin <- max(.SD$checkin, na.rm = TRUE)
      idx <- which(checkins > last_checkin)
      if (length(idx) == 0) idx <- 16 # length(checkins) = 16
      next_checkin <- checkins[min(idx)]
      status <- ifelse(last_checkin == 420, 0, 3)
      data.table::data.table(
        `last check-in` = as.integer(last_checkin),
        `next check-in` = as.integer(next_checkin),
        status = as.integer(status)
      )
    } else { # If there is no checkin event, that means users leave the page within 10s
      data.table::data.table(
        `last check-in` = 0L,
        `next check-in` = 10L,
        status = 3L
      )
    }
  }, by = c("wiki", "language", "session_id", "page_id")]

  if (nrow(page_visits) == 0) {
    srp_survivorship <- empty_df()
  } else {
    srp_survivorship <- page_visits[, {
      surv <- survival::Surv(
        time = `last check-in`,
        time2 = `next check-in`,
        event = status,
        type = "interval"
      )
      fit <- survival::survfit(surv ~ 1)
      df <- data.frame(date = opt$date, rbind(quantile(fit, probs = c(0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99))$quantile))
      colnames(df) <- c('date', 'LD10', 'LD25', 'LD50', 'LD75', 'LD90', 'LD95', 'LD99')
      df
    }, by = "language"][, c('date', 'language', 'LD10', 'LD25', 'LD50', 'LD75', 'LD90', 'LD95', 'LD99')]
  }

}

write.table(srp_survivorship, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
