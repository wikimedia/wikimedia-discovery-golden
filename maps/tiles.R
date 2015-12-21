# Per-file config:
base_path <- paste0(write_root, "maps/")

## This script extracts Vagrant logs and processes them to summarize server-side maps usage.
# Specifically, it generates a dataset containing summaries (avg, median, percentiles) of:
# - total tile requests
# - tile requests per style, e.g. "osm", "osm-intl", ...
# - tile requests per style per zoom, e.g. "osm-z10", "osm-z11", ...

main <- function(date = NULL) {

  # Date handling
  if(is.null(date)) {
    date <- Sys.Date() - 1
  }
  subquery <- date_clause(date)

  # Get the per-user tile usage:
  query <- paste0("SELECT style, zoom, scale, format, user_id, cache, is_automata, COUNT(1) AS n
                   FROM (
                     SELECT
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 1) AS style,
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 2) AS zoom,
                       COALESCE(REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 6), '1') AS scale,
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 7) AS format,
                       CONCAT(user_agent, client_ip) AS user_id,
                       cache_status AS cache,
                       CASE WHEN agent_type = 'spider' THEN 'TRUE' ELSE 'FALSE' END AS is_automata
                     FROM wmf.webrequest", subquery, "
                       AND webrequest_source = 'maps'
                       AND http_status IN('200','304')
                       AND uri_path RLIKE '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$'
                       AND uri_query <> '?loadtesting'
                   ) prepared
                   WHERE zoom != '' AND style != ''
                   GROUP BY style, zoom, scale, format, user_id, cache, is_automata;")
  results <- query_hive(query)
  
  # The zoom sometimes exceeds what we actually allow (18). Yuri said that's acceptable but we
  # enlarge the images, so they're not actually getting zoom level 21-26 tiles.
  results$date <- date
  output <- as.data.table(results[, union('date', names(results))])
  with_automata_output <- output[,list(users = length(user_id), total=sum(n), average = round(mean(n)), median = ceiling(median(n)),
                                       percentile95 = ceiling(quantile(n, 0.95)), percentile99 = ceiling(quantile(n, 0.99))),
                                 by= setdiff(names(output),c("n","user_id", "is_automata"))]
  
  without_automata_output <- output[output$is_automata == FALSE,
                                    list(users = length(user_id), total=sum(n), average = round(mean(n)), median = ceiling(median(n)),
                                    percentile95 = ceiling(quantile(n, 0.95)), percentile99 = ceiling(quantile(n, 0.99))),
                                    by= setdiff(names(output),c("n","user_id", "is_automata"))]
  # Write out
  conditional_write(with_automata_output, file.path(base_path, "tile_aggregates_with_automata.tsv"))
  conditional_write(without_automata_output, file.path(base_path, "tile_aggregates_no_automata.tsv"))
  
}
