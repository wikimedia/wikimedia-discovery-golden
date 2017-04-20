#!/bin/bash

# Check if Reportupdater git submodule is set up
if [ ! -f reportupdater/update_reports.py ]; then
  echo "Warning: Reportupdater needs to be initialized and updated..."
  git submodule init && git submodule update
fi

# Metrics
## Sync README
rsync -c docs/discovery.md /a/aggregate-datasets/discovery/README.md
## Run Reportupdater
for module in "external_traffic" "wdqs" "maps" "search" "portal"
do
 echo "Running Reportupdater on ${module} metrics..."
 nice ionice reportupdater/update_reports.py "modules/metrics/${module}" "/a/aggregate-datasets/discovery/${module}"
done

# Forecasts (dependent on latest metrics)
## Sync README
rsync -c docs/discovery-forecasts.md /a/aggregate-datasets/discovery-forecasts/README.md
## Run Reportupdater
for module in "search" "wdqs"
do
 echo "Running Reportupdater on ${module} forecasts..."
 nice -n 17 ionice -c 2 -n 6 reportupdater/update_reports.py "modules/forecasts/${module}" "/a/aggregate-datasets/discovery-forecasts/${module}"
done
