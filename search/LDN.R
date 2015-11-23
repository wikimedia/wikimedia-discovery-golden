## Calculates the median lethal dose (LD50) and other.
## LD50 = the time point at which we have lost 50% of our users.

# Per-file config:
base_path <- paste0(write_root, "search/")

main <- function(date = NULL, table = "TestSearchSatisfaction2_14098806") {
  
  checkins <- c(0, 10, 20, 30, 40, 50, 60, 90, 120, 150, 180, 210, 240, 300, 360, 420)
  # ^ this will be used for figuring out the interval bounds for each check-in
  
  # Ensure we have a date:
  if(is.null(date)) {
    date <- Sys.Date() - 1
  }
  
  # Decide which table to query depending on the date:
  if ( date < as.Date('2015-09-02') ) {
    warning('No data exists prior to 09/02/2015')
    return(invisible())
  }
  
  # Get data and format:
  data <- query_func(fields = "SELECT * ", date = date, table = table)
  data$timestamp <- lubridate::ymd_hms(data$timestamp)
  # Backwards-compatibility:
  if ( table == "TestSearchSatisfaction2_14098806" ) {
    names(data) <- sub('event_pageViewId', 'event_pageId', names(data))
  }
  # Treat each individual search session as its own thing, rather than belonging
  #   to a set of other search sessions by the same user.
  page_visits <- ddply(data, .(event_searchSessionId, event_pageId),
                    function(session) {
                      if (!all(c('visitPage', 'checkin') %in% session$event_action)) {
                        return(NULL)
                      }
                      temp <- session[all(c('visitPage', 'checkin') %in% session$event_action), ]
                      last_checkin <- max(temp$event_checkin, na.rm = TRUE)
                      idx <- which(checkins > last_checkin)
                      if (length(idx) == 0) idx <- 16 # length(checkins) = 16
                      next_checkin <- checkins[min(idx)]
                      status <- ifelse(last_checkin == 420, 0, 3)
                      return(c(`last check-in` = last_checkin,
                               `next check-in` = next_checkin,
                               status = status))
                    }) # %>%
    # Some sessions may have multiple visited pages, let's pick 1 at random:
    # ddply(.(user_id, event_searchSessionId), olivr::sample_dataframe, size = 1)
  
  surv <- Surv(time = page_visits$`last check-in`,
               time2 = page_visits$`next check-in`,
               event = page_visits$status,
               type = "interval")
  fit <- survfit(surv ~ 1)
  output <- data.frame(date = date, rbind(quantile(fit, probs = c(0.10, 0.25, 0.50, 0.75, 0.90, 0.95, 0.99))$quantile))
  colnames(output) <- c('date', 'LD10', 'LD25', 'LD50', 'LD75', 'LD90', 'LD95', 'LD99')
  
  conditional_write(output, file.path(base_path, "sample_page_visit_ld.tsv"))
  
  return(invisible())
}
