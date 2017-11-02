#!/bin/bash

# files created / touched by report updater need to be rw for user and group
umask 002

# Sync README
rsync -c docs/README.md /srv/published-datasets/discovery/README.md

# Metrics
for module in "external_traffic" "wdqs" "maps" "search" "portal"
do
 echo "Running Reportupdater on ${module} metrics..."
 nice ionice reportupdater/update_reports.py -l info "modules/metrics/${module}" "/srv/published-datasets/discovery/metrics/${module}"
done
