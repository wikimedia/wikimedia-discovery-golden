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
  query <- paste0("SELECT style, zoom, scale, format, user_id, cache, COUNT(1) AS n
                   FROM (
                     SELECT
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 1) AS style,
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 2) AS zoom,
                       COALESCE(REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 6), '1') AS scale,
                       REGEXP_EXTRACT(uri_path, '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$', 7) AS format,
                       CONCAT(user_agent, client_ip) AS user_id,
                       cache_status AS cache
                     FROM wmf.webrequest", subquery, "
                       AND webrequest_source = 'maps'
                       AND http_status IN('200','304')
                       AND uri_path RLIKE '^/([^/]+)/([0-9]{1,2})/(-?[0-9]+)/(-?[0-9]+)(@([0-9]\\.?[0-9]?)x)?\\.([a-z]+)$'
                   ) prepared
                   GROUP BY style, zoom, scale, format, user_id, cache;")
  results <- query_hive(query)

  # In my tests, I've gotten rows with blank style and NA zoom. I have no way of finding out whether
  #   that's because of the hive problems I've been experiencing or what. In either case, it's worth
  #   including this line for data sanitation:
  results <- results[!is.na(results$zoom) & results$style != "", ]
  # P.S. The zoom sometimes exceeds what we actually allow (18). Yuri said that's acceptable but we
  # enlarge the images, so they're not actually getting zoom level 21-26 tiles.

  # Summarise the per-user results  by aggregating across styles and zooms:
  results <- plyr::ddply(results, plyr::.(style, zoom, scale, format, cache), function(x) {
    cbind(x[1, c('style' ,'zoom', 'scale', 'format', 'cache'), drop = FALSE],
          users = length(x$n),
          total = sum(x$n),
          average = round(mean(x$n), 2),
          median = ceiling(median(x$n)),
          percentile95 = ceiling(quantile(x$n, 0.95)),
          percentile99 = ceiling(quantile(x$n, 0.99)))
  })
  
  # Clean up those results:
  results <- results[order(results$style, results$zoom, results$scale, results$format, results$cache), ]
  results$date <- date
  output <- results[, union('date', names(results))]
  
  # Write out
  conditional_write(output, file.path(base_path, "tile_aggregates.tsv"))

}
