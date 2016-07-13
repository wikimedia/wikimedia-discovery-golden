# Per-file config:
base_path <- paste0(write_root, "search/")

main <- function(date = NULL, table = "TestSearchSatisfaction2_15700292"){
  
  # Retrieve data
  data <- wmf::build_query(fields = "SELECT
                             LEFT(timestamp, 8) AS date,
                             timestamp,
                             event_uniqueId AS event_id,
                             event_searchSessionId AS session_id,
                             event_pageViewId AS page_id,
                             event_action AS action,
                             event_checkin AS checkin,
                             event_msToDisplayResults AS load_time",
                           date = date,
                           table = table,
                           conditionals = "event_action IN('searchResultPage','visitPage', 'checkin', 'click')
                                           AND (event_subTest IS NULL OR event_subTest IN ('null','baseline'))
                                           AND event_source = 'fulltext'")
  data <- data[order(data$timestamp, decreasing = FALSE), ]
  data <- data[!duplicated(data$event_id, fromLast = FALSE), ]; data$event_id <- NULL
  data$date <- lubridate::ymd(data$date)
  data$timestamp <- lubridate::ymd_hms(data$timestamp)
  data$action_id <- ifelse(data$action == "searchResultPage", 0, 1)
  data$load_time[data$load_time < 0] <- NA
  
  data <- data.table::as.data.table(data[order(data$session_id, data$action_id, data$page_id, data$timestamp), ])
  
  # For every page visit's check-ins, only keep the last check-in:
  data <- data[!duplicated(data, fromLast = TRUE, by = c("session_id", "page_id", "action")), ]
  
  # Generate the date
  if (is.null(date)) {
    date <- as.Date(data$date[1])
  }
  
  ## Reimplement desktop event counts. Need the following counts:
  # - 'clickthroughs'
  # - 'Form submissions' (I don't think we can figure this out?)
  # - 'Result pages opened'
  # - 'search sessions'
  clickthroughs <- data[data$action %in% c("searchResultPage", "click"), {
    data.frame(clickthrough = any(action == "click", na.rm = TRUE))
  }, by = c("session_id", "page_id")]
  event_data <- data.frame(date = date,
                           clickthroughs = sum(clickthroughs$clickthrough),
                           "Form submissions" = NA,
                           "Result pages opened" = nrow(clickthroughs),
                           "search sessions" = length(unique(clickthroughs$session_id)),
                           check.names = FALSE)
  
  ## Calculate load time percentiles
  load_times <- data[data$action == "searchResultPage", {
    output <- data.frame(t(quantile(load_time, c(0.5, 0.95, 0.99), na.rm = TRUE)))
    names(output) <- c("Median", "95th percentile", "99th Percentile")
    output
  }, by = "date"]
  
  ## Calculates the median lethal dose (LD50) and other.
  ## LD50 = the time point at which we have lost 50% of our users.
  checkins <- c(0, 10, 20, 30, 40, 50, 60, 90, 120, 150, 180, 210, 240, 300, 360, 420)
  # ^ this will be used for figuring out the interval bounds for each check-in
  # Treat each individual search session as its own thing, rather than belonging
  #   to a set of other search sessions by the same user.
  page_visits <- plyr::ddply(data, .(session_id, page_id),
                             function(session) {
                               if (!all(c('visitPage', 'checkin') %in% session$action)) {
                                 return(NULL)
                               }
                               temp <- session[all(c('visitPage', 'checkin') %in% session$action), ]
                               last_checkin <- max(temp$checkin, na.rm = TRUE)
                               idx <- which(checkins > last_checkin)
                               if (length(idx) == 0) idx <- 16 # length(checkins) = 16
                               next_checkin <- checkins[min(idx)]
                               status <- ifelse(last_checkin == 420, 0, 3)
                               return(c(`last check-in` = last_checkin,
                                        `next check-in` = next_checkin,
                                        status = status))
                             })
  surv <- survival::Surv(time = page_visits$`last check-in`,
                         time2 = page_visits$`next check-in`,
                         event = page_visits$status,
                         type = "interval")
  fit <- survival::survfit(surv ~ 1)
  page_visit_survivorship <- data.frame(date = date, rbind(quantile(fit, probs = c(0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99))$quantile))
  colnames(page_visit_survivorship) <- c('date', 'LD10', 'LD25', 'LD50', 'LD75', 'LD90', 'LD95', 'LD99')
  
  # Once we've gotten rid of unnecessary check-ins, let's rearrange the events by timestamp:
  data <- data[order(data$session_id, data$timestamp),,]
  
  ## For debugging and coming up with new thresholds:
  # df <- ortiz:::numeric_check(as.data.frame(data)[,c("session_id", "timestamp")], "timestamp")
  # split_data <- split(df[, "timestamp"], df[, "session_id"])
  # dwell_times <- ortiz:::dwell_time_(split_data)
  # sum(dwell_times > 10)/length(dwell_times)
  
  dwell_data <- ortiz::dwell_time(data = data, id_col = "session_id", ts_col = "timestamp", dwell_threshold = 10)
  
  # Turn it into a data.frame we can write out conditionally, and then do that
  threshold_passing_rate <- data.frame(date = date, threshold_pass = sum(dwell_data)/length(dwell_data))
  
  # Write out the results
  wmf::write_conditional(event_data, file.path(base_path, "desktop_event_counts.tsv"))
  wmf::write_conditional(load_times, file.path(base_path, "desktop_load_times.tsv"))
  wmf::write_conditional(page_visit_survivorship, file.path(base_path, "sample_page_visit_ld.tsv"))
  wmf::write_conditional(threshold_passing_rate, file = file.path(base_path, "search_threshold_pass_rate.tsv"))
  
  return(invisible())
}
