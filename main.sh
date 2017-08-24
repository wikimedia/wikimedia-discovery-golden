#!/bin/bash

# files created / touched by report updater need to be rw for user and group
umask 002

# Sync README
rsync -c docs/README.md /srv/published-datasets/discovery/README.md

# Metrics
for module in "external_traffic" "wdqs" "maps" "search" "portal"
do
 echo "Running Reportupdater on ${module} metrics..."
 nice ionice reportupdater/update_reports.py "modules/metrics/${module}" "/srv/published-datasets/discovery/metrics/${module}"
done

# Forecasts (dependent on latest metrics)
for module in "search" "wdqs"
do
 echo "Running Reportupdater on ${module} forecasts..."
 nice -n 17 ionice -c 2 -n 6 reportupdater/update_reports.py "modules/forecasts/${module}" "/srv/published-datasets/discovery/forecasts/${module}"
done
