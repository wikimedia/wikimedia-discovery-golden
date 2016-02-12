# Config variables and setup
options(scipen = 500)

# Core paths
write_root <- "/a/aggregate-datasets/"
dirs <- c("maps","wdqs", "search", "external_traffic", "portal")
write_dirs <- paste0(write_root, dirs)

# Dependencies
suppressPackageStartupMessages(library(data.table))
suppressMessages({
  library(lubridate)
  library(olivr)
  library(readr)
  library(ortiz)
  library(plyr)
  library(magrittr)
  library(survival)
  library(uaparser)
  library(polloi)
})
