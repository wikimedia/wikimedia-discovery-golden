#!/bin/bash

# Check if Reportupdater git submodule is set up
if [ ! -f reportupdater/update_reports.py ]; then
  echo "Warning: Reportupdater needs to be initialized and updated..."
  git submodule init && git submodule update
fi

# Sync README
rsync -c docs/README.md /a/published-datasets/discovery/README.md

# Metrics
for module in "external_traffic" "wdqs" "maps" "search" "portal"
do
 echo "Running Reportupdater on ${module} metrics..."
 nice ionice reportupdater/update_reports.py "modules/metrics/${module}" "/a/published-datasets/discovery/metrics/${module}"
done

# Forecasts (dependent on latest metrics)
for module in "search" "wdqs"
do
 echo "Running Reportupdater on ${module} forecasts..."
 nice -n 17 ionice -c 2 -n 6 reportupdater/update_reports.py "modules/forecasts/${module}" "/a/published-datasets/discovery/forecasts/${module}"
done
