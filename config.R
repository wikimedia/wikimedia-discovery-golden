# Config variables and setup:
options(scipen = 500, q = "no")

# base_path : This is set on a per-script level
#               (before sourcing common.R)

if(!file.exists(base_path)) {
  dir.create(path = base_path)
}
