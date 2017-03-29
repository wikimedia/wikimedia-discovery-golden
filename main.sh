#!/bin/bash

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
for module in "search" "wdqs"
do
 echo "Running Reportupdater on ${module} forecasts..."
 reportupdater/update_reports.py "modules/forecasts/${module}" "/a/aggregate-datasets/discovery-forecasts/${module}"
done
