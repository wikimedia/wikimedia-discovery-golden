# Per-file config:
base_path <- "/a/aggregate-datasets/wdqs/"

source("../common.R")

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
                   SELECT year, month, day,
                   FIND_IN_SET(uri_path, '/bigdata/namespace/wdq/sparql,/,/index.php') AS uri_path,
                   IF(INSTR(uri_query, 'query') > 0, 'query', 'other') AS uri_query,
                   IF(INSTR(content_type, 'sparql-results') > 0, 'sparql results', 'other') AS content_type,
                   COUNT(*) AS n
                   FROM webrequest",
                   subquery,
                  "AND webrequest_source = 'misc' AND FIND_IN_SET(http_status, '200,301,302,303') > 0
                   GROUP BY year, month, day,
                   FIND_IN_SET(uri_path, '/bigdata/namespace/wdq/sparql,/,/index.php'),
                   IF(INSTR(uri_query, 'query') > 0, 'query', 'other'),
                   IF(INSTR(content_type, 'sparql-results') > 0, 'sparql results', 'other');")
  query_dump <- tempfile()
  cat(query, file = query_dump)

  # Query
  results_dump <- tempfile()
  system(paste0("export HADOOP_HEAPSIZE=1024 && hive -f ", query_dump, " > ", results_dump))
  results <- read.delim(results_dump, sep = "\t", quote = "", as.is = TRUE, header = TRUE)
  file.remove(query_dump, results_dump)

  results$uri_path <- factor(results$uri_path, 0:3, c("other", "/bigdata/namespace/wdq/sparql", "/", "/index.php"))

  output <- data.frame(timestamp = as.Date(paste(results$year, results$month, results$day, sep = "-")),
                       path = results$uri_path,
                       query = results$uri_query,
                       content = results$content_type,
                       events = results$n,
                       stringsAsFactors = FALSE)

  # Write out
  conditional_write(output, file.path(base_path, "wdqs_aggregates.tsv"))

}

# Backlog (start date: 2015-07-28):
# backlog <- function(days) {
#   for (i in days:1) try(main(Sys.Date() - i), silent = TRUE)
# }; backlog(30) # as of 2015-08-27

# Run and kill
main()
q(save = "no")
