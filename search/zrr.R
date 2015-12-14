# Per-file config:
base_path <- paste0(write_root, "search/")

# Core functionality
main <- function(date = NULL){
  
  # Identify date and construct date clause
  if(is.null(date)){
    date <- Sys.Date() - 1
  }
  subquery <- date_clause(date)
  
  # Construct main query and run
  query <- paste("ADD JAR /home/ebernhardson/refinery-hive-0.0.21-SNAPSHOT.jar;
          CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
          CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
          CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
          USE wmf_raw;
          SELECT
              wiki_id,
              source,
              has_suggestion,
              requested_suggestion,
              query_type,
              is_automata,
              COUNT(1) AS total,
              SUM(IF(zero_result, 1, 0)) AS zero_results
          FROM (
              SELECT
                  wikiid AS wiki_id,
                  source,
                  length(concat_ws('', requests.suggestion)) > 0 AS has_suggestion,
                  array_contains(requests.suggestionrequested, TRUE) AS requested_suggestion,
                  requests[size(requests)-1].querytype AS query_type,
                  array_sum(requests.hitstotal, -1) = 0 AS zero_result,
                  CASE
                    WHEN ((ua_parser(useragent)['device_family'] = 'Spider') OR (is_spider(useragent))) THEN 'TRUE'
                    ELSE 'FALSE' END AS is_automata
              FROM
                  cirrussearchrequestset",
                 subquery,
                 "
          ) data_source
          GROUP BY
              wiki_id,
              source,
              has_suggestion,
              requested_suggestion,
              query_type,
              is_automata;")
  data <- query_hive(query)
  
  # Remove silly rows
  data <- data[!is.na(data$total),]
  data <- data[!data$query_type == "",]
  
  # Standardise names
  data$query_type <- ifelse(data$query_type %in% c("full_text", "degraded_full_text", "regex", "more_like"),
                            "Full-Text Search", "Prefix Search")
  data$has_suggestion <- (data$has_suggestion == "true")
  
  # Bind in the date
  data <- as.data.table(cbind(data.frame(date = rep(date,nrow(data))),
                              data))
  
  # Data by type
  by_type <- data[,list(total = sum(total), zero_results = sum(zero_results)), by = c("date", "query_type", "is_automata")]
  by_type$rate <- NA
  
  # Overall data
  overall_data <- data[,list(total = sum(total), zero_results = sum(zero_results)), by = c("date", "is_automata")]
  
  # Suggestion data
  suggestion_data <- data[data$has_suggestion == TRUE, list(total = sum(total), zero_results = sum(zero_results)),
                          by = c("date", "is_automata")]
  suggestion_data$rate <- NA
  
  conditional_write(by_type, file.path(base_path, "cirrus_query_breakdowns_new.tsv"))
  conditional_write(overall_data, file.path(base_path, "cirrus_query_aggregates_new.tsv"))
  conditional_write(suggestion_data, file.path(base_path, "cirrus_suggestion_breakdown_new.tsv"))
}
