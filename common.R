source("config.R")

# Directory creation function
check_dir <- function(dir){
  if (!file.exists(dir)) {
    dir.create(path = dir)
  }
  return(invisible())
}
