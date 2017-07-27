#!/usr/bin/env Rscript

source("config.R")
.libPaths(r_library)
suppressPackageStartupMessages({
  library("optparse")
  library("glue")
})

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character",
              help = "Warning: this metric cannot be backfilled."),
  make_option(c("-o", "--output"), default = NA, action = "store", type = "character",
              help = "Available:
                  * maplink
                  * mapframe")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date) || !(opt$output %in% c("mapframe", "maplink")) ) {
  quit(save = "no", status = 1)
}

enabled <- yaml::yaml.load_file("modules/metrics/maps/prevalence.yaml")

prevalence_query <- function(type, wiki) {
  prop_name <- ifelse(type == "maplink", "kartographer_links", "kartographer_frames")
  ns <- ifelse(wiki == "commonswiki", 6, 0)
  query <- glue("SELECT
  COUNT(*) AS total_articles,
  SUM(IF({type}s > 0, 1, 0)) AS {type}_articles,
  SUM(COALESCE({type}s, 0)) AS total_{type}s
FROM (
  SELECT
    page.page_id,
    pp_value AS {type}s
  FROM (
    SELECT pp_page, pp_value
    FROM page_props
    WHERE pp_propname = '{prop_name}' AND pp_value > 0
  ) AS filtered_props
  RIGHT JOIN page ON page.page_id = filtered_props.pp_page AND page.page_namespace = {ns}
) joined_tables;")
  return(query)
}

if (opt$output == "mapframe") {
  wikis <- c(
    enabled$mapframe$wikipedias,
    enabled$mapframe$miscellaneous,
    setdiff(enabled$maplink$wikivyoages, enabled$mapframe$wikivoyages)
  )
} else {
  wikis <- unname(unlist(enabled$maplink))
}

# We can keep the ID format since the full name of each wiki
# won't be shown on the dashboard, just daily aggregates but
# we still want to keep a daily raw per-wiki breakdown.
names(wikis) <- wikis

# Fetch data from MySQL database:
results <- dplyr::bind_rows(lapply(wikis, function(wiki) {
  result <- tryCatch(
    suppressMessages(wmf::mysql_read(
      prevalence_query(type = opt$output, wiki),
      wiki
    )),
    error = function(e) {
      return(data.frame())
    }
  )
  return(result)
}), .id = "wiki")

output <- cbind(
  date = as.Date(opt$date, "%Y%m%d"),
  results[, union("wiki", colnames(results))]
)

write.table(output, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)

