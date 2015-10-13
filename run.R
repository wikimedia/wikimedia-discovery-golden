source("config.R")
source("common.R")

# Central function
run <- function(dates = NULL){
  
  # List out source files
  source_files <- list.files(dirs, full.names = TRUE, pattern = "\\.R")
  
  # Read them in
  # source_text <- lapply(source_files, readLines, encoding = "UTF-8")
  
  # If the user has not provided dates, just run each file.
  if(!length(dates)){
    lapply(source_files, function(x){
      tryCatch({
        source(x)
        check_dir(base_path)
        main()
      }, error = function(e){
        print(x)
        stop(e)
      })

    })
  }
  
  return(invisible())
}

run()
q()
