# Per-file config:
base_path <- paste0(write_root, "maps/")

## This script extracts Vagrant logs and processes them to summarize server-side maps usage.
# Specifically, it generates a dataset containing summaries (avg, median, percentiles) of:
# - total tile requests
# - tile requests per style, e.g. "osm", "osm-intl", ...
# - tile requests per style per zoom, e.g. "osm-z10", "osm-z11", ...

main <- function(date = NULL) {

  # Date subquery
  clause_data <- wmf::date_clause(date)

  # Get the per-user tile usage:
  query <- paste0("SELECT style, zoom, scale, format, user_id, cache, is_automata, country, COUNT(1) AS n
                   FROM (
                     SELECT
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 1) AS style,
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 2) AS zoom,
                       COALESCE(REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 6), '1') AS scale,
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 7) AS format,
                       CONCAT(user_agent, client_ip) AS user_id,
                       cache_status AS cache,
                       geocoded_data['country_code'] AS country,
                       CASE WHEN agent_type = 'spider' THEN 'TRUE' ELSE 'FALSE' END AS is_automata
                     FROM wmf.webrequest", clause_data$date_clause, "
                       AND webrequest_source = 'maps'
                       AND http_status IN('200','304')
                       AND uri_path RLIKE '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$'
                       AND uri_query <> '?loadtesting'
                   ) prepared
                   WHERE zoom != '' AND style != ''
                   GROUP BY style, zoom, scale, format, user_id, cache, is_automata, country;")
  results <- wmf::query_hive(query)
  
  # The zoom sometimes exceeds what we actually allow (18). Yuri said that's acceptable but we
  # enlarge the images, so they're not actually getting zoom level 21-26 tiles.
  results$date <- clause_data$date
  output <- data.table::as.data.table(results[, union('date', names(results))])
  with_automata_output <- output[,list(users = length(user_id), total=sum(n), average = round(mean(n)), median = ceiling(median(n)),
                                       percentile95 = ceiling(quantile(n, 0.95)), percentile99 = ceiling(quantile(n, 0.99))),
                                 by= setdiff(names(output),c("n","user_id", "is_automata", "country"))]
  
  without_automata_output <- output[output$is_automata == FALSE,
                                    list(users = length(user_id), total=sum(n), average = round(mean(n)), median = ceiling(median(n)),
                                    percentile95 = ceiling(quantile(n, 0.95)), percentile99 = ceiling(quantile(n, 0.99))),
                                    by= setdiff(names(output),c("n","user_id", "is_automata", "country"))]
  
  # Work out unique users on a per-country basis
  top_countries <- c("RU", "IT", "US", "UA", "FR", "IN", "DE", "ES", "GB")
  unique_users <- output[, j = list(users = length(unique(user_id))), by = c("date","country")]
  unique_users <- unique_users[order(unique_users$users, decreasing = TRUE),]
  user_output <- rbind(unique_users[unique_users$country %in% top_countries,],
                       data.table(date = date, country = "Other", users = sum(unique_users$users[!unique_users$country %in% top_countries])))
  user_output$users <- round(user_output$users/sum(user_output$users), 2)
  
  # Write out
  wmf::write_conditional(with_automata_output, file.path(base_path, "tile_aggregates_with_automata.tsv"))
  wmf::write_conditional(without_automata_output, file.path(base_path, "tile_aggregates_no_automata.tsv"))
  wmf::write_conditional(user_output, file.path(base_path, "users_by_country.tsv"))
  
  # Handle rolling window
  wmf::rewrite_conditional(with_automata_output, file.path(base_path, "tile_aggregates_with_automata_rolling.tsv"))
  wmf::rewrite_conditional(without_automata_output, file.path(base_path, "tile_aggregates_no_automata_rolling.tsv"))
}
