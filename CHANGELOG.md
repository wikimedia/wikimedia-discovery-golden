# Change Log (Patch Notes)
All notable changes to this project will be documented in this file.

## 2019/04/17
- Disable Wikipedia.org Portal metrics
- Remove forecasting scripts (previously just disabled)
- Fixed scripts & queries
- Updated testing script
- metrics::search::sister_search_prevalence (SQL->Hive)

## 2019/04/07
- Move queries from SQL to Hive

## 2019/02/15
- Change metrics/portal/app_link_clicks query from SQL to Hive

## 2018/09/28
- Change SQL queries using MobileWikiAppSearch table to Hive queries

## 2018/09/26
- Corrected the following metrics per [T197128](https://phabricator.wikimedia.org/T197128):
  - Survival analysis to correct for no-checkin page visits
  - PaulScore calculation for autocomplete searches
- Fixed WDQS request counts ([T204415](https://phabricator.wikimedia.org/T204415))

## 2017/11/13
- Switched host name from db1047.eqiad.wmnet to db1108.eqiad.wmnet per [T156844](https://phabricator.wikimedia.org/T156844)
- Updated documentation

## 2017/11/02
- Disabled forecasting (per [T112170#3724472](https://phabricator.wikimedia.org/T112170#3724472))

## 2017/10/05
- Changed which hostname the SQL queries are run on ([T176639](https://phabricator.wikimedia.org/T176639))

## 2017/09/22
- Added sister project search results prevalence

## 2017/09/21
- Added new datasets in search and portal ([T172453](https://phabricator.wikimedia.org/T172453)):
  - wikipedia portal pageview by device (desktop vs mobile)
  - wikipedia portal clickthrough rate by device (desktop vs mobile)
  - proportion of wikipedia portal visitors on mobile devices in US vs elsewhere
  - pageviews from full-text search (desktop vs mobile)
  - search return rate on desktop
  - SERPs by access method

## 2017/08/29
- Switched Hive queries to use the "nice" queue ([T156841](https://phabricator.wikimedia.org/T156841)). See [this section](https://wikitech.wikimedia.org/wiki/Analytics/Systems/Cluster/Hive/Queries#Run_long_queries_in_a_screen_session_and_in_the_nice_queue) for additional details.

## 2017/08/28
- Added search results page dwell time ([T170468](https://phabricator.wikimedia.org/T170468))

## 2017/08/01
- Added maplink and mapframe prevalence tracking across wikis ([T170022](https://phabricator.wikimedia.org/T170022))

## 2017/07/27
- Prepared for Puppetized runs ([T170494](https://phabricator.wikimedia.org/T170494))

## 2017/07/05
- Switched TSS2 from Revision 16270835 to 16909631 (due to [change 360851](https://gerrit.wikimedia.org/r/#/c/360851/))

## 2017/05/31
- Changed where datasets are located
- Updated public README

## 2016/12/??-2017/02/??
- Migrated codebase to Analytics' [Reportupdater infrastructure](https://wikitech.wikimedia.org/wiki/Analytics/Reportupdater)
  - Rewrote certain scripts to be pure SQL
  - Rewrote certain R+Hive scripts to be shell+Hive scripts
  - See [T150915](https://phabricator.wikimedia.org/T150915) for more details on the migration
- Updated [Readme](README.md) with complete setup instructions and descriptions of modules
- Updated [CoC](CONDUCT.md) to [Contributor Covenant v1.4.0](http://contributor-covenant.org/version/1/4)
- Added forecasting modules
- Added testing utility

## 2016/08/25
- Uses the [new override_jars](https://gerrit.wikimedia.org/r/#/c/306720/) argument in `wmf::query_hive()` to ensure latest JARs are used
- Removed former team member [Oliver Keyes](https://meta.wikimedia.org/wiki/User:Okeyes_(WMF)) as a maintainer

## 2016/08/23
- Updated Wikipedia.org Portal pageview definition

## 2016/08/19
- Combine [MobileWikiAppSearch](https://meta.wikimedia.org/wiki/Schema:MobileWikiAppSearch) revisions

## 2016/08/08
- Count clicks by language (from Wikipedia.org Portal to Wikipedias)

## 2016/03/07
- Refactor to rely exclusively on the "[wmf](https://phabricator.wikimedia.org/diffusion/1821/)" library for internally developed code rather than "olivr"

## 2016/01/18
- Adds quick backfilling through system command line

## 2016/01/15
- Adds a check for empty dataset returned

## 2015/11/05
- Reset default tables for user satisfaction metrics
- Fix LDN bug

## 2015/11/03
- Changes 'timestamp' to 'date'
- Cleans out some unnecessary code
- Refactors some operations

## 2015/11/02
- Revert the beeline switch

## 2015/10/26
- Switch from hive cli to beeline

## 2015/10/22
- Fixes LDN (survival) code

## 2015/10/20
- Refactored to be more robust and backfilling-friendly

## 2015/09/30
- Added a change log
- Added a contributor code of conduct
- Updated the readme

## 2015/09/29
- Added a script for fetching server-side tile request data
