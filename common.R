source("config.R")

# Dependencies
library(lubridate)
library(olivr)
suppressPackageStartupMessages(library(data.table))

# Query building function
query_func <- function(fields, table, ts_field, date = NULL, conditionals = NULL){
  
  # Ensure we have a date and deconstruct it into a MW-friendly format
  if(is.null(date)){
    date <- Sys.Date()-1
  }
  date <- gsub(x = date, pattern = "-", replacement = "")
  
  # Build the query proper (this will work for EL schemas where the field is always 'timestamp')
  query <- paste(fields, "FROM", table, "WHERE LEFT(timestamp,8) =", date,
                 ifelse(is.null(conditionals), "", "AND"), conditionals)
  
  results <- data.table::as.data.table(olivr::mysql_read(query, "log"))
  return(results)
}

# Conditional write; if the file exists, add x to the end. If it doesn't, write an entirely new file.
conditional_write <- function(x, file){
  if(file.exists(file)){
    write.table(x, file, append = TRUE, sep = "\t", row.names = FALSE, col.names = FALSE, quote = FALSE)
  } else {
    write.table(x, file, append = FALSE, sep = "\t", row.names = FALSE, quote = FALSE)
  }
}

# date_clause; provided with a date it generates an appropriate set of WHERE clauses for HDFS partitioning.
date_clause <- function(date){
  return(paste0(" WHERE year = ", lubridate::year(date),
                " AND month = ", lubridate::month(date),
                " AND day = ", lubridate::day(date), " "))
  
}

# query_hive; provided with a hive query it writes it out to file and then calls Hive over said file, reading the results
# and cleaning up after isself nicely when done.
query_hive <- function(query){
  
  # Write query out to tempfile and create tempfile for results.
  query_dump <- tempfile()
  cat(query, file = query_dump)
  results_dump <- tempfile()
  
  # Query and read in the results
  system(paste0("export HADOOP_HEAPSIZE=1024 && hive -f ", query_dump, " > ", results_dump))
  results <- read.delim(results_dump, sep = "\t", quote = "", as.is = TRUE, header = TRUE)
  
  # Clean up and return
  file.remove(query_dump, results_dump)
  return(results)
}
