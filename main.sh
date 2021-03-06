#!/bin/bash

VENV_DIR="/srv/discovery/venv"
DEPLOYMENT_DIR="/srv/discovery/golden"
DATASETS_DIR="/srv/published/datasets/discovery"

# files created / touched by report updater need to be rw for user and group
umask 002

# Sync README
/usr/bin/rsync -c $DEPLOYMENT_DIR/docs/README.md $DATASETS_DIR/README.md

# Metrics
for module in "external_traffic" "wdqs"
do
 echo "Running Reportupdater on ${module} metrics..."
 /usr/bin/nice /usr/bin/ionice $VENV_DIR/bin/python $DEPLOYMENT_DIR/reportupdater/update_reports.py -l info "${DEPLOYMENT_DIR}/modules/metrics/${module}" "${DATASETS_DIR}/metrics/${module}"
done
