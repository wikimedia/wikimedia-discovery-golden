args <- commandArgs(trailingOnly = TRUE)
source("common.R") # config.R is sourced by common.R anyway

# Usage: Rscript run.R [start_date [end_date]]
#
#   - To backfill data from a specific date (inclusive) to Sys.Date()-1:
#     $> Rscript run.R YYYY-MM-DD
#
#   - To backfill data from a specific date range (inclusive):
#     $> Rscript run.R YYYY-MM-DD YYYY-MM-DD

# Central function
run <- function(dates = NULL){
  
  # List out source files and read in their text
  source_files <- list.files(file.path(getwd(), dirs), full.names = TRUE, pattern = "\\.R")
  sourcefile_text <- lapply(source_files, function(x){paste(readLines(x, warn = FALSE), collapse = " ")})
  names(sourcefile_text) <- source_files
  
  # If the user has not provided dates, just run each file.
  if(!length(dates)){
    date <- (Sys.Date() - 1)
    file_status <- unlist(lapply(source_files, function(x, date){
      tryCatch({
        source(x)
        check_dir(base_path)
        main(date)
      }, error = function(e){
        print(x)
        print(e$message)
        return(FALSE)
      })
      return(TRUE)
    }, date = date))
  } else {
    # If the user has provided dates, we need to do more clever stuff.
    data_files <- list.files(write_dirs, full.names = TRUE, pattern = "\\.tsv$")
    file_status <- unlist(lapply(data_files, function(filename, dates, sourcefiles){
      
      # Read in the dataset
      data <- readr::read_delim(filename, delim = "\t")
      file_dates <- data[,1]
      
      # Are we missing dates in this dataset?
      missing_dates <- dates[!dates %in% file_dates]
      
      # If so, find the sourcefile that generates that file and run it
      if(length(missing_dates)){
        split_name <- unlist(strsplit(filename, split = "/"))
        file_to_run <- names(sourcefiles[grepl(x = sourcefiles,
                                               pattern = split_name[length(split_name)], fixed = TRUE)])
        source(file_to_run)
        tryCatch({
          sapply(missing_dates, main)
        }, error = function(e){ # On error, return the error message and the file that errored, but don't stop.
          print(e$message)
          print(file_to_run)
          return(FALSE)
        })
      } else {
        return(TRUE)
      }
    }, dates = dates, sourcefiles = sourcefile_text))
  }
  
  sprintf("Run, with %s errors", (!sum(file_status)))
  return(invisible())

}

switch(as.character(length(args)),
       "0" = {
         message("Assuming user wants previous day's data.")
         run()
       },
       "1" = {
         start_date <- as.Date(args[1])
         end_date <- Sys.Date() - 1
         message("Backfilling data from", start_date, "to", end_date)
         run(seq(start_date, end_date, "day"))
       },
       "2" = {
         start_date <- as.Date(args[1])
         end_date <- as.Date(args[2])
         if (start_date < end_date) {
           message("Backfilling data from", start_date, "to", end_date)
           run(seq(start_date, end_date, "day"))
         }
         print("Backfilling start date must be before the end date.")
       })
