# Change Log (Patch Notes)
All notable changes to this project will be documented in this file.

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
