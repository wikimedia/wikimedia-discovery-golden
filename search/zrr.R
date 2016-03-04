# Per-file config:
base_path <- paste0(write_root, "search/")

# Core functionality
main <- function(date = NULL){
  
  # Date subquery
  clause_data <- wmf::date_clause(date)

  # Construct main query and run
  query <- paste("ADD JAR /home/ebernhardson/refinery-hive-0.0.21-SNAPSHOT.jar;
          CREATE TEMPORARY FUNCTION array_sum AS 'org.wikimedia.analytics.refinery.hive.ArraySumUDF';
          CREATE TEMPORARY FUNCTION is_spider as 'org.wikimedia.analytics.refinery.hive.IsSpiderUDF';
          CREATE TEMPORARY FUNCTION ua_parser as 'org.wikimedia.analytics.refinery.hive.UAParserUDF';
          CREATE TEMPORARY FUNCTION is_wikimedia as 'org.wikimedia.analytics.refinery.hive.IsWikimediaBotUDF';
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
                    WHEN ((ua_parser(useragent)['device_family'] = 'Spider') OR is_spider(useragent) OR is_wikimedia(useragent)) THEN 'TRUE'
                    ELSE 'FALSE' END AS is_automata
              FROM
                  cirrussearchrequestset", clause_data$date_clause, ") data_source
          GROUP BY
              wiki_id,
              source,
              has_suggestion,
              requested_suggestion,
              query_type,
              is_automata;")
  data <- wmf::query_hive(query)
  
  # Remove silly rows
  data <- data[!is.na(data$total),]
  data <- data[!data$query_type == "",]
  
  # Standardise names
  data$query_type <- ifelse(data$query_type %in% c("full_text", "degraded_full_text", "regex", "more_like"),
                            "Full-Text Search", "Prefix Search")
  data$has_suggestion <- (data$has_suggestion == "true")
  
  # Bind in the date
  data <- data.table::as.data.table(cbind(data.frame(date = rep(clause_data$date, nrow(data))), data))
  
  # Data by type
  by_type_with_automata <- data[, list(rate = round(sum(zero_results)/sum(total), 2)),
                                  by = c("date", "query_type")]
  by_type_no_automata <- data[data$is_automata == FALSE,
                              list(rate = round(sum(zero_results)/sum(total), 2)),
                              by = c("date", "query_type")]

  # Overall data
  overall_data_with_automata <- data[, list(rate = round(sum(zero_results)/sum(total), 2)),
                                       by = c("date")]
  overall_data_no_automata <- data[data$is_automata == FALSE,
                                   list(rate = round(sum(zero_results)/sum(total), 2)),
                                   by = c("date")]

  # Suggestion data
  suggestion_data <- data[data$has_suggestion == TRUE,]
  suggestion_data_with_automata <- suggestion_data[, list(rate = round(sum(zero_results)/sum(total), 2)),
                                                     by = c("date")]
  suggestion_data_no_automata <- suggestion_data[suggestion_data$is_automata == FALSE,
                                                 list(rate = round(sum(zero_results)/sum(total), 2)),
                                                 by = c("date")]

  wmf::write_conditional(by_type_with_automata, file.path(base_path, "cirrus_query_breakdowns_with_automata.tsv"))
  wmf::write_conditional(by_type_no_automata, file.path(base_path, "cirrus_query_breakdowns_no_automata.tsv"))

  wmf::write_conditional(overall_data_with_automata, file.path(base_path, "cirrus_query_aggregates_with_automata.tsv"))
  wmf::write_conditional(overall_data_no_automata, file.path(base_path, "cirrus_query_aggregates_no_automata.tsv"))

  wmf::write_conditional(suggestion_data_with_automata, file.path(base_path, "cirrus_suggestion_breakdown_with_automata.tsv"))
  wmf::write_conditional(suggestion_data_no_automata, file.path(base_path, "cirrus_suggestion_breakdown_no_automata.tsv"))

  # Breakdown by Language and Project
  lang_proj <- polloi::parse_wikiid(data$wiki_id)
  data <- cbind(data, lang_proj)
  data <- data[!is.na(data$project), , ]
  data_by_langproj_with_automata <- data[, list(zero_results = sum(zero_results),
                                                total = sum(total)),
                                           by = c("date", "language", "project")]
  data_by_langproj_no_automata <- data[data$is_automata == FALSE,
                                       list(zero_results = sum(zero_results),
                                            total = sum(total)),
                                       by = c("date", "language", "project")]
  days_to_keep <- 30
  wmf::write_conditional(data_by_langproj_with_automata,
                         file.path(base_path, "cirrus_langproj_breakdown_with_automata.tsv"),
                         days_to_keep)
  wmf::write_conditional(data_by_langproj_no_automata,
                         file.path(base_path, "cirrus_langproj_breakdown_no_automata.tsv"),
                         days_to_keep)
  
}
