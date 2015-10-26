# Config variables and setup
options(scipen = 500, save = "no")

# Core paths
write_root <- "/a/aggregate-datasets/"
dirs <- c("maps","wdqs", "search", "external_traffic")
write_dirs <- paste0(write_root, dirs)

# Dependencies
library(lubridate)
library(olivr)
suppressPackageStartupMessages(library(data.table))
library(readr)
library(ortiz)
library(plyr)
library(magrittr)
library(survival)
