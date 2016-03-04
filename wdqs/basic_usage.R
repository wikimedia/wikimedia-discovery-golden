# Per-file config:
base_path <- paste0(write_root, "wdqs/")

# Retrieves data for the WDQS stuff we care about, drops it in the aggregate-datasets directory.
# Should be run on stat1002, /not/ on the datavis machine.

# Create a script that would produce raw data on usage of
# - query.wikidata.org
# - SPARQL endpoint: query.wikidata.org/bigdata/namespace/wdq/sparql

# Central function
main <- function(date = NULL) {

  # Date subquery
  clause_data <- wmf::date_clause(date)

  # Write query and run it
  query <- paste0("USE wmf;
                   SELECT year, month, day, uri_path,
                   UPPER(http_status IN('200','304')) as success,
                   CASE WHEN agent_type = 'spider' THEN 'TRUE' ELSE 'FALSE' END AS is_automata,
                   COUNT(*) AS n
                   FROM webrequest",
                  clause_data$date_clause,
                  "AND webrequest_source = 'misc'
                   AND uri_host = 'query.wikidata.org'
                   AND uri_path IN('/', '/bigdata/namespace/wdq/sparql')
                   GROUP BY year, month, day, uri_path, UPPER(http_status IN('200','304')),
                   CASE WHEN agent_type = 'spider' THEN 'TRUE' ELSE 'FALSE' END;")
  results <- wmf::query_hive(query)

  output <- data.frame(date = as.Date(paste(results$year, results$month, results$day, sep = "-")),
                       path = results$uri_path,
                       http_success = results$success,
                       is_automata = results$is_automata,
                       events = results$n,
                       stringsAsFactors = FALSE)

  # Write out
  wmf::write_conditional(output, file.path(base_path, "wdqs_aggregates_new.tsv"))

}
