# Per-file config:
base_path <- "/a/aggregate-datasets/wdqs/"

source("common.R")

# Retrieves data for the WDQS stuff we care about, drops it in the aggregate-datasets directory. Should be run on stat1002, /not/ on the datavis machine.

# Create a script that would produce raw data on usage of
# - query.wikidata.org
# - SPARQL endpoint: query.wikidata.org/bigdata/namespace/wdq/sparql

# Central function
main <- function(date = NULL) {

  # Date handling
  if(is.null(date)) {
    date <- Sys.Date() - 1
  }
  subquery <- paste0(" WHERE year = ", lubridate::year(date),
                     " AND month = ", lubridate::month(date),
                     " AND day = ", lubridate::day(date), " ")

  # Write query and dump to file
  query <- paste0("USE wmf;
                   SELECT year, month, day, uri_path,
                   UPPER(http_status IN('200','304')) as success,
                   COUNT(*) AS n
                   FROM webrequest",
                   subquery,
                  "AND webrequest_source = 'misc'
                   AND uri_host = 'query.wikidata.org'
                   AND uri_path IN('/', '/bigdata/namespace/wdq/sparql')
                   GROUP BY year, month, day, uri_path,
                   UPPER(http_status IN('200','304'));")
                   
  query_dump <- tempfile()
  cat(query, file = query_dump)

  # Query
  results_dump <- tempfile()
  system(paste0("export HADOOP_HEAPSIZE=1024 && hive -f ", query_dump, " > ", results_dump))
  results <- read.delim(results_dump, sep = "\t", quote = "", as.is = TRUE, header = TRUE)
  file.remove(query_dump, results_dump)

  output <- data.frame(timestamp = as.Date(paste(results$year, results$month, results$day, sep = "-")),
                       path = results$uri_path,
                       http_status = results$http_category,
                       events = results$n,
                       stringsAsFactors = FALSE)

  # Write out
  conditional_write(output, file.path(base_path, "wdqs_aggregates.tsv"))

}

# backlog <- function(days) {
#   for (i in days:1) try(main(Sys.Date() - i), silent = TRUE)
# }; backlog(30)

# Run and kill
main()
q(save = "no")
