Golden Retriever Scripts
========================

This repository contains aggregation/acquisition scripts for extracting data from the MySQL/Hive databases.

- **search** contains scripts for getting usage data on search features, incl. APIs. The generated datasets are accessible at http://datasets.wikimedia.org/aggregate-datasets/search/
- **wdqs** contains scripts for getting usage data on Wikidata Query Service. The generated datasets are accessible at http://datasets.wikimedia.org/aggregate-datasets/wdqs/
- **maps** contains scripts for getting usage data on Wikimedia Maps. The generated datasets are accessible at http://datasets.wikimedia.org/aggregate-datasets/maps/

## Dependencies

```
install.packages(c('lubridate', 'devtools'))
devtools::install_github('Ironholds/olivr')
```

## Additional Information

Please note that this project is released with a [Contributor Code of Conduct](CONDUCT.md). By participating in this project you agree to abide by its terms.

### Contacts

- [Oliver Keyes](https://meta.wikimedia.org/wiki/User:Okeyes_(WMF))
- [Mikhail Popov](https://meta.wikimedia.org/wiki/User:MPopov_(WMF))
