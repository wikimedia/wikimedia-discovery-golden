base_path <- paste0(write_root, "portal/")

main <- function(date = NULL, table = "WikipediaPortal_15890769"){
  
  # Read
  data <- wmf::build_query(fields = "
                           SELECT SUBSTRING(timestamp, 1, 8) AS date,
                             event_session_id AS session,
                             UPPER(event_country) AS country,
                             event_destination AS destination,
                             event_event_type AS type,
                             event_section_used AS section_used,
                             timestamp AS ts,
                             userAgent AS user_agent",
                           date = date,
                           table = table,
                           conditionals = "((event_cohort IS NULL) OR (event_cohort IN ('null','baseline')))
                             AND event_country != 'US' AND event_event_type IN('landing', 'clickthrough')")

  # Sanitise
  data$section_used[is.na(data$section_used)] <- "no action"
  data$date <- as.Date(lubridate::ymd(data$date))
  data <- data.table::as.data.table(data)
  
  # Generate dwell time
  data$ts <- lubridate::ymd_hms(data$ts)
  dwell_metric <- data[,j = {
    if(.N > 1){
      sorted_ts <- as.numeric(.SD$ts[order(.SD$ts, decreasing = TRUE)])
      sorted_ts[1] - sorted_ts[2]
    } else {
      NULL
    }
  }, by = c("date","session")]

  dwell_output <- data.frame(t(quantile(dwell_metric$V1, c(0.5, 0.95, 0.99))))
  dwell_output$date <- dwell_metric$date[1]
  names(dwell_output) <- c("Median", "95th percentile", "99th Percentile", "date")
  dwell_output <- dwell_output[, c(4, 1:3)]
  
  # Generate clickthrough rate data
  clickthrough_data <- data[, j = list(events = .N), by = c("date","type")]
  
  # Most common section clicked
  most_common <- as.data.frame(data) %>%
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
  
  # First visit clickthrough rates
  possible_sections <- data.frame(section_used = c("no action", "primary links", "search",
                                                   "secondary links", "other languages", "other projects"),
                                  stringsAsFactors = FALSE)
  first_visits <- as.data.frame(data) %>%
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
    dplyr::mutate(date = data$date[1]) %>%
    dplyr::select(c(date, `no action`, `primary links`, `search`, `secondary links`, `other languages`, `other projects`))
  
  # Generate click breakdown (last action)
  breakdown_data <- data %>%
    dplyr::arrange(ts) %>%
    dplyr::filter(!duplicated(session, fromLast = TRUE)) %>%
    dplyr::group_by(date, section_used) %>%
    dplyr::summarize(events = n()) %>%
    data.table::as.data.table()
  
  # Generate by-country breakdown with regional data for US
  data("ISO_3166_1", package = "ISOcodes")
  us_other_abb <- c("AS", "GU", "MP", "PR", "VI")
  us_other_mask <- match(us_other_abb, ISO_3166_1$Alpha_2)
  regions <- data.frame(abb = c(paste0("US:", c(as.character(state.abb), "DC")), us_other_abb),
                        # ^ need to verify that District of Columbia shows up as DC and not another abbreviation
                        region = paste0("U.S. (", c(as.character(state.region), "South", rep("Other",5)), ")"),
                        state = c(state.name, "District of Columbia", ISO_3166_1$Name[us_other_mask]),
                        stringsAsFactors = FALSE)
  regions$region[regions$region == "U.S. (North Central)"] <- "U.S. (Midwest)"
  regions$region[c(state.division == "Pacific", rep(FALSE, 5))] <- "U.S. (Pacific)" # see https://phabricator.wikimedia.org/T136257#2399411
  countries <- data.frame(abb = c(regions$abb, "GB", "CA",
                                  "DE", "IN", "AU", "CN",
                                  "RU", "PH", "FR"),
                          name = c(regions$region, "United Kingdom", "Canada",
                                   "Germany", "India", "Australia", "China",
                                   "Russia", "Philippines", "France"),
                          stringsAsFactors = FALSE)
  ## BEGIN PROTOTYPE
  # This can be used to test out the processing code before https://gerrit.wikimedia.org/r/#/c/295572/ is merged.
  # data$country[data$country == "US" & !is.na(data$country)] <- sample(unique(regions$abb), sum(data$country == "US", na.rm = TRUE), replace = TRUE)
  ## END PROTOTYPE
  country_data <- as.data.frame(data) %>%
    dplyr::mutate(country = ifelse(country %in% countries$abb, country, "Other")) %>%
    dplyr::left_join(countries, by = c("country" = "abb")) %>%
    dplyr::mutate(name = ifelse(is.na(name), "Other", name)) %>%
    dplyr::select(-country) %>% dplyr::rename(country = name) %>%
    dplyr::group_by(country) %>%
    dplyr::summarize(events = n()) %>%
    dplyr::mutate(date = date) %>%
    dplyr::select(c(date, country, events)) %>%
    dplyr::arrange(desc(country))

  # Experimental: Generate all countries breakdown
  all_countries <- data.frame(abb = c(regions$abb, ISO_3166_1$Alpha_2[-us_other_mask]),
                          name = c(regions$region, ISO_3166_1$Name[-us_other_mask]),
                          stringsAsFactors = FALSE)
  data_w_countryname <- as.data.frame(data) %>%
    dplyr::mutate(country = ifelse(country %in% all_countries$abb, country, "Other")) %>%
    dplyr::left_join(all_countries, by = c("country" = "abb")) %>%
    dplyr::mutate(name = ifelse(is.na(name), "Other", name)) %>%
    dplyr::select(-country) %>% dplyr::rename(country = name)

  ctr_visit <- data_w_countryname %>%
    dplyr::arrange(session, ts) %>%
    dplyr::group_by(session) %>%
    dplyr::mutate(visit = cumsum(type == "landing")) %>%
    dplyr::group_by(date, country, session, visit) %>%
    dplyr::summarize(dummy_clt = sum(type=="clickthrough")>0) %>%
    dplyr::group_by(country) %>%
    dplyr::summarize(n_visit = n(), ctr_visit = round(sum(dummy_clt)/n(), 4))
  ctr_session <- data_w_countryname %>%
    dplyr::group_by(date, country, session) %>%
    dplyr::summarize(dummy_clt = sum(type=="clickthrough")>0) %>%
    dplyr::group_by(country) %>%
    dplyr::summarize(n_session = n(), ctr_session = round(sum(dummy_clt)/n(), 4)) 
  all_country_data <- data_w_countryname %>%
    dplyr::group_by(country) %>%
    dplyr::summarize(events = n(), ctr = round(sum(type=="clickthrough")/n(), 4)) %>%
    dplyr::mutate(date = date) %>%
    dplyr::select(c(date, country, events, ctr)) %>%
    dplyr::arrange(desc(country)) %>%
    dplyr::left_join(ctr_visit, by="country") %>%
    dplyr::left_join(ctr_session, by="country")
  
  # Last action by country
  last_action_country <- data_w_countryname %>%
    dplyr::arrange(ts) %>%
    dplyr::filter(!duplicated(session, fromLast = TRUE)) %>%
    dplyr::group_by(date, section_used, country) %>%
    dplyr::summarize(events = n()) %>%
    dplyr::mutate(proportion = round(events/sum(events), 4))

  # Most common section clicked by country
  most_common_country <- data_w_countryname %>%
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

  # First visit clickthrough rates by country
  first_visits_country <- data_w_countryname %>%
    dplyr::arrange(session, ts) %>%
    dplyr::group_by(session) %>%
    dplyr::mutate(visit = cumsum(type == "landing")) %>%
    dplyr::filter(visit == 1) %>%
    dplyr::group_by(date, section_used, country) %>%
    dplyr::summarize(sessions = n()) %>%
    dplyr::mutate(proportion = round(sessions/sum(sessions), 4))

  # Get user agent data
  wmf::set_proxies() # To allow for the latest YAML to be retrieved.
  uaparser::update_regexes()
  ua_data <- data.table::as.data.table(uaparser::parse_agents(data$user_agent, fields = c("browser","browser_major")))
  ua_data <- ua_data[,j=list(amount = .N), by = c("browser","browser_major")]
  ua_data$date <- data$date[1]
  ua_data$percent <- round((ua_data$amount/sum(ua_data$amount))*100, 2)
  ua_data <- ua_data[ua_data$percent >= 0.5, c("date", "browser", "browser_major", "percent"), with = FALSE]
  data.table::setnames(ua_data, 3, "version")

  wmf::write_conditional(clickthrough_data, file.path(base_path, "clickthrough_rate.tsv"))
  wmf::write_conditional(most_common, file.path(base_path, "most_common_per_visit.tsv"))
  wmf::write_conditional(first_visits, file.path(base_path, "clickthrough_firstvisit.tsv"))
  wmf::write_conditional(breakdown_data, file.path(base_path, "clickthrough_breakdown.tsv"))
  wmf::write_conditional(dwell_output, file.path(base_path, "dwell_metrics.tsv"))
  wmf::write_conditional(country_data, file.path(base_path, "country_data.tsv"))
  wmf::write_conditional(ua_data, file.path(base_path, "user_agent_data.tsv"))

  days_to_keep <- 60
  wmf::rewrite_conditional(all_country_data, file.path(base_path, "all_country_data.tsv"), days_to_keep)
  wmf::rewrite_conditional(last_action_country, file.path(base_path, "last_action_country.tsv"), days_to_keep)
  wmf::rewrite_conditional(most_common_country, file.path(base_path, "most_common_country.tsv"), days_to_keep)
  wmf::rewrite_conditional(first_visits_country, file.path(base_path, "first_visits_country.tsv"), days_to_keep)

  return(invisible())
}
