R CMD BATCH ./search/desktop.R &&
R CMD BATCH ./search/mobile.R &&
R CMD BATCH ./search/app.R &&
R CMD BATCH ./search/api.R &&
R CMD BATCH ./wdqs/basic_usage.R &&
python ./search/core.py &&
rm -rf .RData
