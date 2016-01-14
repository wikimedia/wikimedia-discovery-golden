source("config.R")

# Stop with a more informative message when there are no elements returned from the db
stop_on_empty <- function(data){
  if(nrow(data) == 0){
    stop("No rows were returned from the database")
  }
  return(invisible())
}


# Directory creation function
check_dir <- function(dir){
  if (!file.exists(dir)) {
    dir.create(path = dir)
  }
  return(invisible())
}

# Query building function
query_func <- function(fields, table, ts_field, date = NULL, conditionals = NULL){
  
  # Ensure we have a date and deconstruct it into a MW-friendly format
  if (is.null(date)) {
    date <- Sys.Date() - 1
  }
  date <- gsub(x = date, pattern = "-", replacement = "")
  
  # Build the query proper (this will work for EL schemas where the field is always 'timestamp')
  query <- paste(fields, "FROM", table, "WHERE LEFT(timestamp,8) =", date,
                 ifelse(is.null(conditionals), "", "AND"), conditionals)
  
  results <- data.table::as.data.table(olivr::mysql_read(query, "log"))
  stop_on_empty(results)
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
date_clause <- function(date) {
  if (is.null(date)) {
    date <- Sys.Date() - 1
  }
  return(paste0(" WHERE year = ", lubridate::year(date),
                " AND month = ", lubridate::month(date),
                " AND day = ", lubridate::day(date), " "))
  # If we're including spaces in the strings, we should just use paste instead of paste0
}

# query_hive; provided with a hive query it writes it out to file and then calls Hive over said file, reading the results
# and cleaning up after isself nicely when done.
query_hive <- function(query){
  
  # Write query out to tempfile and create tempfile for results.
  query_dump <- tempfile()
  cat(query, file = query_dump)
  results_dump <- tempfile()
  
  # Query and read in the results
  try({
    system(paste0("export HADOOP_HEAPSIZE=1024 && hive -S -f ", query_dump, " > ", results_dump))
    results <- read.delim(results_dump, sep = "\t", quote = "", as.is = TRUE, header = TRUE)
  })
  
  # Clean up and return
  file.remove(query_dump, results_dump)
  stop_on_empty(results)
  return(results)
}
