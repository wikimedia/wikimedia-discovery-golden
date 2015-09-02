R CMD BATCH search/desktop.R &&
R CMD BATCH search/mobile.R &&
R CMD BATCH search/app.R &&
R CMD BATCH search/api.R &&
R CMD BATCH search/failures.R &&
R CMD BATCH wdqs/basic_usage.R
python core.py &&
rm -rf .RData
