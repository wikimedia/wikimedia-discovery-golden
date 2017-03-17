*Golden* Retriever Scripts
==========================

This repository contains aggregation/acquisition scripts for extracting data from the MySQL/Hive databases for computing metrics for [various teams](https://www.mediawiki.org/wiki/Wikimedia_Discovery#The_team) within [Discovery](https://www.mediawiki.org/wiki/Wikimedia_Discovery). It uses Analytics' [Reportupdater infrastructure](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater). This codebase is maintained by [Discovery's Analysis team](https://www.mediawiki.org/wiki/Discovery_Analysis):

- [Deb Tankersley](https://meta.wikimedia.org/wiki/User:DTankersley_(WMF)) (Product Manager)
- [Mikhail Popov](https://meta.wikimedia.org/wiki/User:MPopov_(WMF)) (Data Analyst)
- [Chelsy Xie](https://meta.wikimedia.org/wiki/User:CXie_(WMF)) (Data Analyst)

For questions and comments, contact [Deb](mailto:deb@wikimedia.org?subject=Discovery Analysis data retriever codebase), [Mikhail](mailto:mikhail@wikimedia.org?subject=Golden repo), or [Chelsy](mailto:cxie@wikimedia.org?subject=Golden repo).

## Table of Contents

- [Setup](#setup)
    - [Dependencies](#dependencies)
- [Usage](#usage)
    - [Production](#production)
    - [Testing](#testing)
- [Modules](#modules)
- [Adding New Metrics Modules](#adding-new-metrics-modules)
    - [MySQL](#mysql)
    - [Hive](#hive)
    - [R](#r)
        - [MySQL in R](#mysql-in-r)
        - [Hive in R](#hive-in-r)
- [Adding New Forecasting Modules](#adding-new-forecasting-modules)
- [Additional Information](#additional-information)

## Setup

On [stat1002](https://wikitech.wikimedia.org/wiki/Stat1002):

```bash
cd /a/discovery/
git clone --recursive https://gerrit.wikimedia.org/r/wikimedia/discovery/golden
cd golden

# If already cloned without --recursive:
git submodule update --init --recursive

# Add execution permission to scripts:
chmod -R +x modules/
```

### Dependencies

```bash
pip install -r reportupdater/requirements.txt
```

Some of the R packages require C++ libraries, which are installed on [stat1002](https://wikitech.wikimedia.org/wiki/Stat1002) -- that use [compute.pp](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/compute.pp) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/compute.pp)) -- by being listed in [packages](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/packages.pp) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/packages.pp)). See [operations-puppet/modules/statistics/manifests/packages.pp](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/packages.pp;45fb6c9c7fbab57f204772a5da2c0cf923aea8c2$28-31) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/45fb6c9c7fbab57f204772a5da2c0cf923aea8c2/modules/statistics/manifests/packages.pp#L28--L31)) for example.

```R
# Set WMF proxies:
Sys.setenv("http_proxy" = "http://webproxy.eqiad.wmnet:8080")
Sys.setenv("https_proxy" = "http://webproxy.eqiad.wmnet:8080")

# Set path for packages:
.libPaths("/a/discovery/r-library")

# Essentials:
install.packages(
  c("devtools", "testthat", "Rcpp",
    "tidyverse", "data.table", "plyr",
    "optparse", "yaml", "data.tree",
    "ISOcodes", "knitr",
    # For wmf:
    "urltools", "ggthemes", "pwr",
    # For polloi's datavis functions:
    "shiny", "shinydashboard", "dygraphs", "RColorBrewer",
    # For polloi's data manipulation functions:
    "xts", "mgcv", "zoo",
    # For forecasting modules:
    "bsts", "forecast"
    # ^ see note below
  ),
  repos = "https://cran.rstudio.com/",
  lib = "/a/discovery/r-library"
)

# 'uaparser' requires C++11, and libyaml-cpp 0.3, boost-system, boost-regex C++ libraries
devtools::install_github("ua-parser/uap-r", configure.args = "-I/usr/include/yaml-cpp -I/usr/include/boost")

# 'ortiz' is needed for Search team's user engagement calculation | https://phabricator.wikimedia.org/diffusion/WDOZ/
devtools::install_git("https://gerrit.wikimedia.org/r/wikimedia/discovery/ortiz")

# 'wmf' is needed for querying MySQL and Hive | https://phabricator.wikimedia.org/diffusion/1821/
devtools::install_git("https://gerrit.wikimedia.org/r/wikimedia/discovery/wmf")

# 'polloi' is needed for wikiid-splitting | https://phabricator.wikimedia.org/diffusion/WDPL/
devtools::install_git("https://gerrit.wikimedia.org/r/wikimedia/discovery/polloi")
```

Don't forget to add packages to [test.R](test.R) because that script checks that all packages are installed before performing a test run of the reports.

**Note**: we have had problems installing R package [bsts](https://cran.r-project.org/package=bsts) and its dependencies [Boom](https://cran.r-project.org/package=Boom) and [BoomSpikeSlab](https://cran.r-project.org/package=BoomSpikeSlab) on stat1002 (but not stat1003). Fortunately, [Andrew Otto](https://meta.wikimedia.org/wiki/User:Ottomata) has figured out what to put in [~/.R/Makevars](https://cran.r-project.org/doc/manuals/r-release/R-exts.html#Using-Makevars) to make those packages compile. From [T147682#2837271](https://phabricator.wikimedia.org/T147682#2837271):

```
CXX=g++-4.8
CXX1X=g++-4.8
CXX1XFLAGS=-std=c++11 -g -O2 -fstack-protector --param=ssp-buffer-size=4 -Wformat -Werror=format-security -D_FORTIFY_SOURCE=2 -g
CXX1XPICFLAGS=-fPIC
SHLIB_CXX1XLD=g++-4.8
SHLIB_CXX1XLDFLAGS=-std=c++11 -shared
LDFLAGS=-L/usr/lib/R/lib -Wl,-Bsymbolic-functions -Wl,-z,relro
```

To **update packages**, run `Rscript test.R --update_packages` which will update all the dependencies listed in **test.R**

## Usage

**Note**: You don't need to use the `--config-path` argument if your config file is inside the query folder and is named **config.yaml**, that is the default.

### Production

To use in production, add **main.sh** to `crontab`:

```
$ crontab -e

12 20 * * * cd /a/discovery/golden/ && sh main.sh
```

**main.sh** executes **reportupdater/update_reports.py** on each module and writes data to the respective files in **/a/aggregate-datasets/discovery/**

### Testing

If you wish to run all the modules without writing data to files or checking for missingness, use:

```bash
Rscript test.R >> test_`date +%F_%T`.log.md 2>&1
# The test script automatically uses yesterday's date.

# Alternatively:
Rscript test.R --start_date=2017-01-01 --end_date=2017-01-02 >> test_`date +%F_%T`.log.md 2>&1

# You can disbale forecasting modules:
Rscript test.R --disable_forecasts >> test_`date +%F_%T`.log.md 2>&1

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
    - [x] Search on Desktop
        - [x] [Event counts](modules/metrics/search/desktop_event_counts.sql)
        - [x] [Load times](modules/metrics/search/desktop_load_times) (invokes [load_times.R](modules/metrics/search/load_times.R))
        - [x] [Survival/LDN: Retention of users on visited pages](modules/metrics/search/sample_page_visit_ld) ([T113297](https://phabricator.wikimedia.org/T113297))
        - [x] [Dwell-time: % of users visiting results for more than 10s](modules/metrics/search/search_threshold_pass_rate) ([T113297](https://phabricator.wikimedia.org/T113297), [T113513](https://phabricator.wikimedia.org/T113513), [Change 240593](https://gerrit.wikimedia.org/r/#/c/240593/))
        - [x] [PaulScore](modules/metrics/search/paulscore_approximations) ([T144424](https://phabricator.wikimedia.org/T144424))
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
        - Probable non-bots, as detected by ML (planned, [T149440](https://phabricator.wikimedia.org/T149440)
  - [x] [Wikipedia.org Portal](https://www.mediawiki.org/wiki/Wikipedia.org_Portal) ([configuration](modules/metrics/portal/config.yaml), [T118994](https://phabricator.wikimedia.org/T118994))
    - [x] [Pageviews](modules/metrics/portal/pageviews) ([T125737](https://phabricator.wikimedia.org/T125737), [T143064](https://phabricator.wikimedia.org/T143064), [T143605](https://phabricator.wikimedia.org/T143605))
    - [x] [Referers](modules/metrics/portal/referer_data)
    - [x] [User Agent breakdown](modules/metrics/portal/user_agent_data)
    - [x] Languages
        - [x] [Visited](modules/metrics/portal/language_destination) ([T140816](https://phabricator.wikimedia.org/T140816))
        - [x] [Switching](modules/metrics/portal/language_switching) ([T143149](https://phabricator.wikimedia.org/T143149))
    - [x] Geographic breakdown of visitors
        - [x] [Top 10 countries](modules/metrics/portal/country_data) ([T123347](https://phabricator.wikimedia.org/T123347))
        - [x] [All countries](modules/metrics/portal/all_country_data) ([T138107](https://phabricator.wikimedia.org/T138107))
    - [x] Engagement
        - [x] Overall
            - [x] [Clickthrough rate](modules/metrics/portal/clickthrough_rate)
            - [x] [Last performed action](modules/metrics/portal/clickthrough_breakdown)
            - [x] [Clickthrough on first visit](modules/metrics/portal/clickthrough_firstvisit)
            - [x] [Most commonly clicked section per visit](modules/metrics/portal/most_common_per_visit) ([T141061](https://phabricator.wikimedia.org/T141061))
            - [x] [Sister-project clickthrough breakdown](modules/metrics/portal/clickthrough_sisterprojects) ([T152617](https://phabricator.wikimedia.org/T152617))
        - [x] Broken down by user's country ([T138107](https://phabricator.wikimedia.org/T138107))
            - [x] [Last performed action](modules/metrics/portal/last_action_country)
            - [x] [Most commonly clicked section per visit](modules/metrics/portal/most_common_country)
            - [x] [Clickthrough on first visit](modules/metrics/portal/first_visits_country)
        - [x] [Dwell-time](modules/metrics/portal/dwell_metrics) ([T120432](https://phabricator.wikimedia.org/T120432))
        - [x] [Mobile app links](modules/metrics/portal/app_link_clicks.sql) ([T154634](https://phabricator.wikimedia.org/T154634))
  - [x] [Wikidata Query Service](https://www.mediawiki.org/wiki/Wikidata_query_service) ([configuration](modules/metrics/wdqs/config.yaml))
    - [x] [WDQS homepage traffic and SPARQL endpoint usage](modules/metrics/wdqs/basic_usage) ([T109360](https://phabricator.wikimedia.org/T109360))
    - [x] [WDQS LDF endpoint usage](modules/metrics/wdqs/basic_usage) ([T153936](https://phabricator.wikimedia.org/T153936))
  - [x] [Maps](https://www.mediawiki.org/wiki/Maps) ([configuration](modules/metrics/maps/config.yaml))
    - [x] GeoFeatures ([T112311](https://phabricator.wikimedia.org/T112311))
      - [x] [Actions per tool](modules/metrics/maps/actions_per_tool.sql)
      - [x] [Users per feature](modules/metrics/maps/users_per_feature.sql)
    - [x] Kartographer usage
      - [x] [Users by country](modules/metrics/maps/users_by_country) ([T119448](https://phabricator.wikimedia.org/T119448))
      - [x] Tile requests ([T113832](https://phabricator.wikimedia.org/T113832))
        - [x] [No automata](modules/metrics/maps/tile_aggregates_no_automata)
        - [x] [With automata](modules/metrics/maps/tile_aggregates_with_automata)
    - KPIs (planned)
  - [x] External Traffic ([configuration](modules/metrics/external_traffic/config.yaml))
    - [x] [Referer data](modules/metrics/external_traffic/referer_data) ([T116295](https://phabricator.wikimedia.org/T116295), [Change 247601](https://gerrit.wikimedia.org/r/#/c/247601/))
- [x] **Forecasts** ([modules/forecasts/forecast.R](modules/forecasts/forecast.R), see [T112170](https://phabricator.wikimedia.org/T112170) for more details)
  - [x] Search ([configuration](modules/forecasts/search/config.yaml))
    - [x] Cirrus API usage
        - [x] [ARIMA-modelled forecasts](modules/forecasts/search/api_cirrus_arima)
        - [x] [BSTS-modelled forecasts](modules/forecasts/search/api_cirrus_bsts)
    - [x] Overall zero results rate
        - [x] [ARIMA-modelled forecasts](modules/forecasts/search/zrr_overall_arima)
        - [x] [BSTS-modelled forecasts](modules/forecasts/search/zrr_overall_bsts)
  - Wikipedia.org Portal (planned)
  - [x] WDQS ([configuration](modules/forecasts/wdqs/config.yaml))
    - [x] Homepage traffic
        - [x] [ARIMA-modelled forecasts](modules/forecasts/wdqs/homepage_traffic_arima)
        - [x] [BSTS-modelled forecasts](modules/forecasts/wdqs/homepage_traffic_bsts)
    - [x] SPARQL endpoint usage
        - [x] [ARIMA-modelled forecasts](modules/forecasts/wdqs/sparql_usage_arima)
        - [x] [BSTS-modelled forecasts](modules/forecasts/wdqs/sparql_usage_bsts)
  - Maps (planned)
  - External Traffic (planned)

## Adding New Metrics Modules

### MySQL

For metrics computed from [event logging](https://wikitech.wikimedia.org/wiki/Analytics/EventLogging) [data stored in MySQL](https://wikitech.wikimedia.org/wiki/Analytics/Data_access#EventLogging_data), try to write pure SQL queries whenever possible, using the conventions described [here](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater#SQL_Query_conventions). Use the following template to get started:

```sql
SELECT
  DATE('{from_timestamp}') AS date,
  ...,
  COUNT(*) AS events
FROM {Schema_Revision}
WHERE timestamp >= '{from_timestamp}' AND timestamp < '{to_timestamp}'
GROUP BY date, ...;
```

### Hive

The scripts that invoke Hive (e.g. the ones that count [web requests](https://wikitech.wikimedia.org/wiki/Analytics/Data/Webrequest)) must follow the conventions described [here](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater#Script_conventions). Use the following template to get started:

```bash
#!/bin/bash

hive -e "USE wmf;
SELECT
  '$1' AS date,
  ...,
  COUNT(*) AS requests
FROM webrequest
WHERE
  webrequest_source = 'text' -- also available: 'maps' and 'misc'
  AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) >= '$1'
  AND CONCAT(year, '-', LPAD(month, 2, '0'), '-', LPAD(day, 2, '0')) < '$2'
  ...
GROUP BY
  '$1',
  ...;
" 2> /dev/null | grep -v parquet.hadoop
```

### R

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

## Adding New Forecasting Modules

Forecasting modules assume that all the data is current (hence why they are scheduled to run after the metrics modules in **main.sh**) and the forecast is made for the next day. For example, if backfilling a forecast for 2016-12-01, the model is fit using all available data up to and including 2016-11-30.

There are two model wrappers in [modules/forecasts/models.R](modules/forecasts/models.R):
- `forecast_arima()` which models the time series via [ARIMA](https://en.wikipedia.org/wiki/Autoregressive_integrated_moving_average) and accepts the following inputs:
    - `x`: a 1-column `xts` object
    - `arima_params`: a list w/ order & seasonal components
    - `bootstrap_ci`: whether prediction intervals are computed using simulation with resampled errors
    - `bootstrap_npaths`: number of sample paths used in computing simulated prediction intervals
    - `transformation` = a transformation to apply to the data ("none", "log", "logit", or "in millions"); the function back-transforms the predictions to the original scale depending on the transformation chosen
- `forecast_bsts()` which models the time series via [BSTS](https://en.wikipedia.org/wiki/Bayesian_structural_time_series) and accepts the following inputs:
    - `x`: a 1-column `xts` object
    - `n_iter`: number of MCMC iterations to keep
    - `burn_in`: number of MCMC iterations to throw away as burn-in,
    - `transformation`: a transformation to apply to the data ("none", "log", "logit", or "in millions"); the function back-transforms the predictions to the original scale depending on the transformation chosen
    - `ar_lags`: number of lags ("p") in the AR(p) process, omitted by default so an AR(p) state component is *NOT* added to the state specification

When adding a new forecasting module, add a script-type report to the respective **config.yaml** and use the following template for the script:

```bash
#!/bin/bash

Rscript modules/forecasts/forecast.R --date=$2 --metric=[your forecasted metric] --model=[ARIMA [--bootstrap_ci]|BSTS]
```

Change the `--metric` and `--model` arguments accordingly. The actual data-reading and metric-forecasting calls are in a switch statement in [modules/forecasts/forecast.R](modules/forecasts/forecast.R). Don't forget to add the forecasted metric to the `--metric` option's help text at the top of **forecast.R** and don't forget to subset the data after reading it in (e.g. `dplyr::filter(data, date < as.Date(opt$date))`)

**Note** the `--date=$2` in there instead of `--date=$1`. This is because Reportupdater passes a *start date* and an *end date* to every script it runs, with the goal of generating a report for *start date*. However, with forecasting modules we're actually interested in generating a report for *end date* after observing the latest metric for *start date*.

## Additional Information

This repository can be browsed in [Phabricator/Diffusion](https://phabricator.wikimedia.org/diffusion/WDGO/), but is also (read-only) mirrored to [GitHub](https://github.com/wikimedia/wikimedia-discovery-golden/).

Please note that this project is released with a [Contributor Code of Conduct](CONDUCT.md). By participating in this project you agree to abide by its terms.
