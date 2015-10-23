source("common.R") # config.R is sourced by common.R anyway

# Central function
run <- function(dates = NULL){
  
  # List out source files and read in their text
  source_files <- list.files(file.path(getwd(), dirs), full.names = TRUE, pattern = "\\.R")
  sourcefile_text <- lapply(source_files, function(x){paste(readLines(x, warn = FALSE), collapse = " ")})
  names(sourcefile_text) <- source_files
  
  # If the user has not provided dates, just run each file.
  if(!length(dates)){
    lapply(source_files, function(x){
      tryCatch({
        source(x)
        check_dir(base_path)
        main()
      }, error = function(e){
        print(x)
        print(e$message)
      })

    })
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
          main(missing_dates)
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

run()
q()
