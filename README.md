*Golden* Retriever Scripts
==========================

This repository contains aggregation/acquisition scripts for extracting data from the MySQL/Hive databases for computing metrics for [various teams](https://www.mediawiki.org/wiki/Wikimedia_Discovery#The_team) within [Discovery](https://www.mediawiki.org/wiki/Wikimedia_Discovery). It uses Analytics' [Reportupdater infrastructure](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater). This codebase is maintained by [Product Analytics team](https://www.mediawiki.org/wiki/Product_Analytics):

- [Mikhail Popov](https://meta.wikimedia.org/wiki/User:MPopov_(WMF)) (Data Analyst)
- [Chelsy Xie](https://meta.wikimedia.org/wiki/User:CXie_(WMF)) (Data Analyst)

For questions and comments, contact [Mikhail](mailto:mikhail@wikimedia.org), or [Chelsy](mailto:cxie@wikimedia.org).

## Table of Contents

- [Setup](#setup-and-usage)
- [Dependencies](#dependencies)
- [Modules](#modules)
- [Adding New Metrics Modules](#adding-new-metrics-modules)
    - [MySQL](#mysql)
    - [Hive](#hive)
    - [R](#r)
        - [MySQL in R](#mysql-in-r)
        - [Hive in R](#hive-in-r)
- [Adding New Forecasting Modules](#adding-new-forecasting-modules)
- [Additional Information](#additional-information)

## Setup and Usage

As of [T170494](https://phabricator.wikimedia.org/T170494), the setup and daily runs are Puppetized on [stat1005](https://wikitech.wikimedia.org/wiki/Stat1005) via the [statistics::discovery](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/discovery.pp) module (also mirrored on [GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/discovery.pp)).

## Dependencies

```bash
pip install -r reportupdater/requirements.txt
```

Some of the R packages require C++ libraries, which are installed on [stat1002](https://wikitech.wikimedia.org/wiki/Stat1002) -- that use [compute.pp](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/compute.pp) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/compute.pp)) -- by being listed in [packages](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/packages.pp) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/packages.pp)). See [operations-puppet/modules/statistics/manifests/packages.pp](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/packages.pp;45fb6c9c7fbab57f204772a5da2c0cf923aea8c2$28-31) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/45fb6c9c7fbab57f204772a5da2c0cf923aea8c2/modules/statistics/manifests/packages.pp#L28--L31)) for example.

```R
# Set WMF proxies:
Sys.setenv("http_proxy" = "http://webproxy.eqiad.wmnet:8080")
Sys.setenv("https_proxy" = "http://webproxy.eqiad.wmnet:8080")

# Set path for packages:
lib_path <- "/srv/discovery/r-library"
.libPaths(lib_path)

# Essentials:
install.packages(
  c("devtools", "testthat", "Rcpp",
    "tidyverse", "data.table", "plyr",
    "optparse", "yaml", "data.tree",
    "ISOcodes", "knitr", "glue",
    # For wmf:
    "urltools", "ggthemes", "pwr",
    # For polloi's datavis functions:
    "shiny", "shinydashboard", "dygraphs", "RColorBrewer",
    # For polloi's data manipulation functions:
    "xts", "mgcv", "zoo"
  ),
  repos = c(CRAN = "https://cran.rstudio.com/"),
  lib = lib_path
)

# 'ortiz' is needed for Search team's user engagement calculation | https://phabricator.wikimedia.org/diffusion/WDOZ/
devtools::install_git("https://gerrit.wikimedia.org/r/wikimedia/discovery/ortiz")

# 'wmf' is needed for querying MySQL and Hive | https://phabricator.wikimedia.org/diffusion/1821/
devtools::install_git("https://gerrit.wikimedia.org/r/wikimedia/discovery/wmf")

# 'polloi' is needed for wikiid-splitting | https://phabricator.wikimedia.org/diffusion/WDPL/
devtools::install_git("https://gerrit.wikimedia.org/r/wikimedia/discovery/polloi")
```

Don't forget to add packages to [test.R](test.R) because that script checks that all packages are installed before performing a test run of the reports.

To update packages, use [update-library.R](https://github.com/wikimedia/puppet/blob/production/modules/r/files/update-library.R):

```bash
Rscript /etc/R/update-library.R -l /srv/discovery/r-library
Rscript /etc/R/update-library.R -l /srv/discovery/r-library -p polloi
```

## Testing

If you wish to run all the modules without writing data to files or checking for missingness, use:

```bash
Rscript test.R >> test_`date +%F_%T`.log.md 2>&1
# The test script automatically uses yesterday's date.

# Alternatively:
Rscript test.R --start_date=2017-01-01 --end_date=2017-01-02 >> test_`date +%F_%T`.log.md 2>&1

# And have it include samples of the existing data (for comparison):
Rscript test.R --include_samples >> test_`date +%F_%T`.log.md 2>&1
```

[The testing utility](test.R) finds all the modules, builds a list of the reports, and then performs the appropriate action depending on whether the report is a SQL query or a script. Each module's output will be printed to console. This should go without saying, but _running through all the modules will take **a while**_. The script outputs a Markdown-formatted log that can be saved to file using the commands above. Various statistics on the execution times will be printed at the end, including a table of all the reports' execution times. The table can be omitted using the `--omit_times` option.

## Modules

- [x] **Metrics** ([modules/metrics](modules/metrics))
  - [x] [Search](https://www.mediawiki.org/wiki/Wikimedia_Discovery/Search) ([configuration](modules/metrics/search/config.yaml))
    - [x] [API usage](modules/metrics/search/search_api_usage)
    - [x] Search on Android and iOS apps
        - [x] [Event counts](modules/metrics/search/app_event_counts.sql)
        - [x] [Load times](modules/metrics/search/app_load_times) (invokes [load_times.R](modules/metrics/search/load_times.R))
        - [x] [Invoke source counts](modules/metrics/search/invoke_source_counts) on Android ([T143726](https://phabricator.wikimedia.org/T143726))
        - [x] [Positions of clicked results](modules/metrics/search/click_position_counts) on Android ([T143726](https://phabricator.wikimedia.org/T143726))
    - [x] Search on Mobile Web
        - [x] [Event counts](modules/metrics/search/mobile_event_counts.sql)
        - [x] [Load times](modules/metrics/search/mobile_load_times) (invokes [load_times.R](modules/metrics/search/load_times.R))
        - [x] [Session counts](modules/metrics/search/mobile_session_counts) (invokes [mobile_session_counts.R](modules/metrics/search/mobile_session_counts.R))
    - [x] Search on Desktop
        - [x] [Event counts](modules/metrics/search/desktop_event_counts.sql)
        - [x] [Load times](modules/metrics/search/desktop_load_times) (invokes [load_times.R](modules/metrics/search/load_times.R))
        - [x] [Survival/LDN: Retention of users on visited pages](modules/metrics/search/sample_page_visit_ld) ([T113297](https://phabricator.wikimedia.org/T113297))
        - [x] [Dwell-time: % of users visiting results for more than 10s](modules/metrics/search/search_threshold_pass_rate) ([T113297](https://phabricator.wikimedia.org/T113297), [T113513](https://phabricator.wikimedia.org/T113513), [Change 240593](https://gerrit.wikimedia.org/r/#/c/240593/))
        - [x] [Time spent on search result pages (SRPs)](modules/metrics/search/srp_survtime) (invokes [srp_survtime.R](modules/metrics/search/srp_survtime.R))
        - [x] [PaulScore](modules/metrics/search/paulscore_approximations) ([T144424](https://phabricator.wikimedia.org/T144424))
        - [x] [Bounce rate](modules/metrics/search/desktop_return_rate) (invokes [desktop_return_rate.R](modules/metrics/search/desktop_return_rate.R))
        - Dwell-time, PaulScore, event counts, etc. broken down by language-project (planned, [T150410](https://phabricator.wikimedia.org/T150410))
    - [x] Zero results rate (all invoke [cirrus_aggregates.R](modules/metrics/search/cirrus_aggregates.R))
        - [x] Overall
            - [x] [No automata](modules/metrics/search/cirrus_query_aggregates_no_automata)
            - [x] [With automata](modules/metrics/search/cirrus_query_aggregates_with_automata)
        - [x] Broken down by type
            - [x] [No automata](modules/metrics/search/cirrus_query_breakdowns_no_automata)
            - [x] [With automata](modules/metrics/search/cirrus_query_breakdowns_with_automata)
        - [x] Suggestion data
            - [x] [No automata](modules/metrics/search/cirrus_suggestion_breakdown_no_automata)
            - [x] [With automata](modules/metrics/search/cirrus_suggestion_breakdown_with_automata)
        - [x] Broken down by language-project pairs ([T126244](https://phabricator.wikimedia.org/T126244))
            - [x] [No automata](modules/metrics/search/cirrus_langproj_breakdown_no_automata)
            - [x] [With automata](modules/metrics/search/cirrus_langproj_breakdown_with_automata)
        - Well-behaved searchers (planned, [T150901](https://phabricator.wikimedia.org/T150901))
        - Probable non-bots, as detected by ML (abandoned, [T149440](https://phabricator.wikimedia.org/T149440)
    - [x] Sister search
      - [x] [Prevalence on SRPs](modules/metrics/search/sister_search_prevalence.sql)
      - [x] [Traffic to sister projects from Wikipedia SRPs](modules/metrics/search/sister_search_traffic)
    - [x] [Article pageviews from full-text search](modules/metrics/search/pageviews_from_fulltext_search)
    - [x] [Full-text SRP views by device and agent type](modules/metrics/search/search_result_pages)
  - [x] [Wikidata Query Service](https://www.mediawiki.org/wiki/Wikidata_query_service) ([configuration](modules/metrics/wdqs/config.yaml))
    - [x] [WDQS homepage traffic and SPARQL endpoint usage](modules/metrics/wdqs/basic_usage) ([T109360](https://phabricator.wikimedia.org/T109360))
    - [x] [WDQS LDF endpoint usage](modules/metrics/wdqs/basic_usage) ([T153936](https://phabricator.wikimedia.org/T153936))
  - [x] [Maps](https://www.mediawiki.org/wiki/Maps) ([configuration](modules/metrics/maps/config.yaml))
    - [x] Kartotherian usage
      - [x] [Users by country](modules/metrics/maps/users_by_country) ([T119448](https://phabricator.wikimedia.org/T119448))
      - [x] Tile requests ([T113832](https://phabricator.wikimedia.org/T113832))
        - [x] [No automata](modules/metrics/maps/tile_aggregates_no_automata)
        - [x] [With automata](modules/metrics/maps/tile_aggregates_with_automata)
    - [x] Maps prevalence on wikis ([T170022](https://phabricator.wikimedia.org/T170022))
      - [x] [Maplinks](modules/metrics/maps/maplink_prevalence)
      - [x] [Mapframes](modules/metrics/maps/mapframe_prevalence)
  - [x] External Traffic ([configuration](modules/metrics/external_traffic/config.yaml))
    - [x] [Referer data](modules/metrics/external_traffic/referer_data) ([T116295](https://phabricator.wikimedia.org/T116295), [Change 247601](https://gerrit.wikimedia.org/r/#/c/247601/))

## Adding New Metrics Modules

### Hive queries

The scripts that invoke Hive (e.g. the ones that count [web requests](https://wikitech.wikimedia.org/wiki/Analytics/Data/Webrequest) or use [event logging](https://wikitech.wikimedia.org/wiki/Analytics/EventLogging) data in Hadoop) must follow the conventions described [here](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater#Script_conventions). Use the following template to get started:

```bash
#!/bin/bash

hive -e "USE wmf;
SELECT
  '$1' AS date,
  ...,
  COUNT(*) AS requests
FROM webrequest
WHERE webrequest_source = 'text' -- also available: 'maps' and 'misc'
  AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= '$1'
  AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < '$2'
  ...
GROUP BY
  '$1',
  ...;
" 2> /dev/null | grep -v parquet.hadoop
```

```bash
#!/bin/bash

hive -e "USE event;
SELECT
  '$1' AS date,
  ...
FROM ${SCHEMA_NAME}
WHERE CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= '$1'
  AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < '$2'
  -- optional: specifying revision ID
  ...;
" 2> /dev/null | grep -v parquet.hadoop
```

### R scripts

**A note on paths**: Reportupdater does not `cd` into the query folder. So you'll need to execute scripts relative to the path you're executing Reportupdater from, e.g. `Rscript modules/metrics/search/some_script.R -d $1`

These scripts have 2 parts: the script part that is called by update_reports.py, which must adhere to Reportupdater's [script conventions](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater#Script_conventions):

```bash
#!/bin/bash

Rscript modules/metrics/search/script.R --date=$1
# Alternatively: Rscript modules/metrics/search/script.R -d $1
```

**script.R** that is called should adhere to one of the two templates below. **Note** that in both, we specify `file = ""` in `write.table` because we want to print the data as TSV to console for Reportupdater.

#### MySQL in R

For R scripts that need to fetch (and process) data from MySQL, use the following template:

```R
#!/usr/bin/env Rscript

suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "LEFT(timestamp, 8) = '%Y%m%d'")

query <- paste0("
SELECT
  DATE('", opt$date, "') AS date,
  COUNT(*) AS events
FROM TestSearchSatisfaction2_15922352
WHERE ", date_clause, "
GROUP BY date;
")

# Fetch data from MySQL database:
results <- tryCatch(suppressMessages(wmf::mysql_read(query, "log")), error = function(e) {
  quit(save = "no", status = 1)
})

# ...whatever else you need to do with the data before returning a TSV to console...

write.table(results, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
```

#### Hive in R

For R scripts that need to fetch (and process) data from Hive, use the following template:

```R
#!/usr/bin/env Rscript

suppressPackageStartupMessages(library("optparse"))

option_list <- list(
  make_option(c("-d", "--date"), default = NA, action = "store", type = "character")
)

# Get command line options, if help option encountered print help and exit,
# otherwise if options not found on command line then set defaults:
opt <- parse_args(OptionParser(option_list = option_list))

if (is.na(opt$date)) {
  quit(save = "no", status = 1)
}

# Build query:
date_clause <- as.character(as.Date(opt$date), format = "year = %Y AND month = %m AND day = %d")

query <- paste0("USE wmf;
SELECT
  TO_DATE(ts) AS date,
  COUNT(*) AS pageviews
FROM webrequest
WHERE
  webrequest_source = 'text'
  AND ", date_clause, "
  AND is_pageview
GROUP BY
  TO_DATE(ts);
")

# Fetch data from database using Hive:
results <- tryCatch(wmf::query_hive(query), error = function(e) {
  quit(save = "no", status = 1)
})

# ...whatever else you need to do with the data before returning a TSV to console...

write.table(results, file = "", append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
```

## Additional Information

This repository can be browsed in [Phabricator/Diffusion](https://phabricator.wikimedia.org/diffusion/WDGO/), but is also (read-only) mirrored to [GitHub](https://github.com/wikimedia/wikimedia-discovery-golden/).

Please note that this project is released with a [Contributor Code of Conduct](CONDUCT.md). By participating in this project you agree to abide by its terms.
