#!/bin/bash

# Check if modules/forecasts/forecast.R has execution permission for Reportupdater
# (If it doesn't, then other R and shell scripts in modules/ probably don't either.)
if [ `ls -l modules/forecasts | grep -e forecast.R | grep -e "-rwxrwxr-x" | wc -l` == "0" ]; then
  echo "Warning: modules do not have execution permission; granting now..."
  chmod +x -R modules/
fi

# Check if Reportupdater git submodule is set up
if [ ! -f reportupdater/update_reports.py ]; then
  echo "Warning: Reportupdater needs to be initialized and updated..."
  git submodule init && git submodule update
fi

# Metrics
for module in "external_traffic" "wdqs" "maps" "search" "portal"
do
 echo "Running Reportupdater on ${module} metrics..."
 reportupdater/update_reports.py "modules/metrics/${module}" "/a/aggregate-datasets/discovery/${module}"
done

# Forecasts (dependent on latest metrics)
# for module in "search" "wdqs"
# do
#  echo "Running Reportupdater on ${module} forecasts..."
#  reportupdater/update_reports.py "modules/forecasts/${module}" "/a/aggregate-datasets/discovery-forecasts/${module}"
# done
