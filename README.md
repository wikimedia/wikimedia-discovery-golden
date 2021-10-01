# *Golden* Retriever Scripts – ARCHIVED

This repository contains archived aggregation/acquisition scripts for extracting data from the MySQL/Hive databases for computing metrics for Search Platform team (formerly Discovery). It uses[Reportupdater infrastructure](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater). This codebase was maintained by [Product Analytics team](https://www.mediawiki.org/wiki/Product_Analytics)'s [Mikhail Popov](https://meta.wikimedia.org/wiki/User:MPopov_(WMF)) and was decommissioned as part of [T227782](https://phabricator.wikimedia.org/T227782).

## Table of Contents

- [Setup](#setup-and-usage)
- [Dependencies](#dependencies)
- [Modules](#modules)
- [Additional Information](#additional-information)

## Setup and Usage

As of [T170494](https://phabricator.wikimedia.org/T170494), the setup and daily runs are Puppetized on [stat1007](https://wikitech.wikimedia.org/wiki/Stat1007) via the [statistics::discovery](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/discovery.pp) module (also mirrored on [GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/discovery.pp)).

## Dependencies

```bash
pip install -r reportupdater/requirements.txt
```

Some of the R packages require C++ libraries, which are installed on [stat1007](https://wikitech.wikimedia.org/wiki/Stat1007) -- that use [compute.pp](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/compute.pp) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/compute.pp)) -- by being listed in [packages](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/packages.pp) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/production/modules/statistics/manifests/packages.pp)). See [operations-puppet/modules/statistics/manifests/packages.pp](https://phabricator.wikimedia.org/diffusion/OPUP/browse/production/modules/statistics/manifests/packages.pp;45fb6c9c7fbab57f204772a5da2c0cf923aea8c2$28-31) ([GitHub](https://github.com/wikimedia/operations-puppet/blob/45fb6c9c7fbab57f204772a5da2c0cf923aea8c2/modules/statistics/manifests/packages.pp#L28--L31)) for example.

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

## Additional Information

Please note that this project is released with a [Contributor Code of Conduct](CONDUCT.md). By participating in this project you agree to abide by its terms.
