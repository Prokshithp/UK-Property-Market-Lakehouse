# UK Property Market Lakehouse & HPC Benchmark — Project Report
_Generated 2026-11-01 23:06_

## 1. Dataset
HM Land Registry *Price Paid Data*, years 2016–2026 (https://www.gov.uk/government/statistical-data-sets/price-paid-data-downloads), Open Government Licence v3. Every residential property sale registered in England and Wales; one CSV per year.

## 2. Key metrics (gold layer)
|   Transactions |   Distinct postcodes | First sale          | Last sale           | Median residential price   | Total value transacted   |
|---------------:|---------------------:|:--------------------|:--------------------|:---------------------------|:-------------------------|
|     10,666,409 |            1,243,481 | 2016-01-01 00:00:00 | 2026-09-30 00:00:00 | £261,000                   | £3,984 bn                |

## 3. Market analytics
### 3.1 England & Wales by year
|   year |   sales |   median_price |
|-------:|--------:|---------------:|
|   2016 |  922724 |         219950 |
|   2017 |  906861 |         228500 |
|   2018 |  875536 |         235000 |
|   2019 |  842981 |         239995 |
|   2020 |  753528 |         257500 |
|   2021 | 1087015 |         276000 |
|   2022 |  910013 |         290000 |
|   2023 |  708444 |         286500 |
|   2024 |  764317 |         291000 |
|   2025 |  822625 |         299950 |
|   2026 |  392461 |         290000 |

![Monthly volume and median price — note the stamp-duty-holiday spike](figures/03_monthly_market.png)

*Monthly volume and median price — note the stamp-duty-holiday spike*

### 3.2 Liverpool vs other cities
Liverpool's median price grew **61%** between 2016 and 2026, versus **32%** for England & Wales overall.

|   year |   liverpool_sales |   liverpool_median |
|-------:|------------------:|-------------------:|
|   2016 |              9500 |             130250 |
|   2017 |             10343 |             137000 |
|   2018 |             10616 |             140000 |
|   2019 |             10013 |             144995 |
|   2020 |              7865 |             160000 |
|   2021 |             10414 |             175000 |
|   2022 |              9290 |             179625 |
|   2023 |              7961 |             175000 |
|   2024 |              8210 |             191998 |
|   2025 |              8481 |             210000 |
|   2026 |              3950 |             210000 |

|   year |   BIRMINGHAM |   BRISTOL |   LEEDS |   LIVERPOOL |   LONDON |   MANCHESTER |   NEWCASTLE UPON TYNE |
|-------:|-------------:|----------:|--------:|------------:|---------:|-------------:|----------------------:|
|   2016 |       150000 |    250000 |  161375 |      130748 |   508000 |       149950 |                160000 |
|   2017 |       160000 |    266748 |  168000 |      137000 |   531250 |       160000 |                164950 |
|   2018 |       173000 |    275000 |  175000 |      140000 |   525000 |       170000 |                169000 |
|   2019 |       179950 |    280000 |  182500 |      144972 |   537500 |       176000 |                170000 |
|   2020 |       190000 |    300000 |  200000 |      160000 |   565000 |       197500 |                185000 |
|   2021 |       205000 |    319950 |  217000 |      175000 |   570000 |       218000 |                190000 |
|   2022 |       220000 |    342500 |  229950 |      179625 |   585000 |       235000 |                199995 |
|   2023 |       222340 |    345000 |  230000 |      175000 |   585000 |       235000 |                200000 |
|   2024 |       225000 |    350000 |  239995 |      191998 |   588000 |       249000 |                200000 |
|   2025 |       235000 |    350000 |  245000 |      210000 |   579000 |       250500 |                212000 |
|   2026 |       236000 |    355000 |  242000 |      210000 |   550000 |       250000 |                205000 |

![Median price by city](figures/01_city_comparison.png)

*Median price by city*

### 3.3 Liverpool postcode districts — top 10 by growth
| postcode_district   |   median_first_year |   median_last_year |   growth_pct |   total_sales |
|:--------------------|--------------------:|-------------------:|-------------:|--------------:|
| L6                  |               62500 |             155000 |        148   |          2791 |
| L4                  |               65000 |             132000 |        103.1 |          3943 |
| L20                 |               72250 |             143000 |         97.9 |          3377 |
| L1                  |               72200 |             142025 |         96.7 |          3021 |
| L13                 |               87000 |             165000 |         89.7 |          3618 |
| L27                 |               83000 |             156750 |         88.9 |           605 |
| L28                 |               68000 |             128000 |         88.2 |           341 |
| L7                  |               85000 |             160000 |         88.2 |          1772 |
| L21                 |               95000 |             175000 |         84.2 |          2681 |
| L8                  |               86250 |             155000 |         79.7 |          3224 |

![Fastest-growing Liverpool districts](figures/02_liverpool_districts.png)

*Fastest-growing Liverpool districts*

### 3.4 Property types
![Median price by property type](figures/04_property_types.png)

*Median price by property type*

### 3.5 Counties — highest and lowest growth
| county                              |   median_first_year |   median_last_year |   growth_pct |   total_sales |
|:------------------------------------|--------------------:|-------------------:|-------------:|--------------:|
| BLAENAU GWENT                       |               80000 |             140000 |         75   |          8934 |
| BOURNEMOUTH, CHRISTCHURCH AND POOLE |              195000 |             335000 |         71.8 |         48748 |
| MERTHYR TYDFIL                      |               90375 |             155000 |         71.5 |          7180 |
| LEICESTER                           |              145000 |             245000 |         69   |         29168 |
| CAERPHILLY                          |              120000 |             200000 |         66.7 |         24230 |
| WEST BERKSHIRE                      |              330000 |             385000 |         16.7 |         24862 |
| GREATER LONDON                      |              442000 |             515000 |         16.5 |        948553 |
| WOKINGHAM                           |              415000 |             475000 |         14.5 |         30559 |
| WINDSOR AND MAIDENHEAD              |              460000 |             512000 |         11.3 |         22109 |
| WESTMORLAND AND FURNESS             |              347500 |             235000 |        -32.4 |         13752 |

![County growth league](figures/05_county_league.png)

*County growth league*

### 3.6 New-build premium
|   year |   median_new_build |   median_existing |   premium_pct |
|-------:|-------------------:|------------------:|--------------:|
|   2016 |             264995 |            210000 |          26.2 |
|   2017 |             280000 |            220000 |          27.3 |
|   2018 |             289950 |            225000 |          28.9 |
|   2019 |             295000 |            230000 |          28.3 |
|   2020 |             310000 |            249000 |          24.5 |
|   2021 |             319995 |            270000 |          18.5 |
|   2022 |             339995 |            280000 |          21.4 |
|   2023 |             350000 |            277000 |          26.4 |
|   2024 |             350000 |            285000 |          22.8 |
|   2025 |             361000 |            290000 |          24.5 |
|   2026 |             390000 |            290000 |          34.5 |

![New-build premium](figures/06_new_build_premium.png)

*New-build premium*

## 4. Data quality
**24 / 27 checks passed.** Raw-layer failures are expected and document the defects the silver layer fixes (missing postcodes, non-residential 'Other' property types, implausible prices). All silver and gold checks must pass or the pipeline stops.

| layer   | table                  | check                    | column                | status   | details                                                          |
|:--------|:-----------------------|:-------------------------|:----------------------|:---------|:-----------------------------------------------------------------|
| raw     | pp-2026.csv            | min_rows                 | nan                   | PASS     | 457,963 rows (minimum 10,000)                                    |
| raw     | pp-2026.csv            | unique                   | transaction_id        | PASS     | 0 duplicate values                                               |
| raw     | pp-2026.csv            | not_null                 | postcode              | FAIL     | 625 null values                                                  |
| raw     | pp-2026.csv            | accepted_values          | property_type         | FAIL     | 14,456 unexpected values: ['O']                                  |
| raw     | pp-2026.csv            | range                    | CAST(price AS BIGINT) | FAIL     | 439 values outside [10000, 20000000]                             |
| silver  | sales (parquet)        | min_rows                 | nan                   | PASS     | 10,666,409 rows (minimum 550,000)                                |
| silver  | sales (parquet)        | unique                   | transaction_id        | PASS     | 0 duplicate values                                               |
| silver  | sales (parquet)        | not_null                 | price                 | PASS     | 0 null values                                                    |
| silver  | sales (parquet)        | not_null                 | date_of_transfer      | PASS     | 0 null values                                                    |
| silver  | sales (parquet)        | not_null                 | property_type_name    | PASS     | 0 null values                                                    |
| silver  | sales (parquet)        | range                    | price                 | PASS     | 0 values outside [1, None]                                       |
| silver  | sales (parquet)        | accepted_values          | property_type         | PASS     | 0 unexpected values: []                                          |
| silver  | sales (parquet)        | accepted_values          | duration              | PASS     | 0 unexpected values: []                                          |
| silver  | sales (parquet)        | accepted_values          | price_band            | PASS     | 0 unexpected values: []                                          |
| silver  | sales (parquet)        | regex_match              | postcode              | PASS     | 0 values not matching /^[A-Z]{1,2}[0-9][A-Z0-9]? [0-9][A-Z]{2}$/ |
| silver  | sales (parquet)        | partition_matches_year   | year                  | PASS     | rows whose transfer year disagrees with their partition          |
| gold    | dim_date               | unique                   | date_key              | PASS     | 0 duplicate values                                               |
| gold    | dim_location           | unique                   | location_key          | PASS     | 0 duplicate values                                               |
| gold    | dim_property           | unique                   | property_key          | PASS     | 0 duplicate values                                               |
| gold    | fact_sale              | unique                   | sale_key              | PASS     | 0 duplicate values                                               |
| gold    | fact_sale              | unique                   | transaction_id        | PASS     | 0 duplicate values                                               |
| gold    | fact_sale              | row_count_matches_source | nan                   | PASS     | 10,666,409 vs 10,666,409                                         |
| gold    | fact_sale              | referential_integrity    | date_key              | PASS     | 0 orphan rows                                                    |
| gold    | fact_sale              | referential_integrity    | location_key          | PASS     | 0 orphan rows                                                    |
| gold    | fact_sale              | referential_integrity    | property_key          | PASS     | 0 orphan rows                                                    |
| gold    | vw_city_comparison     | min_rows                 | nan                   | PASS     | 77 rows (minimum 50)                                             |
| gold    | vw_liverpool_districts | min_rows                 | nan                   | PASS     | 39 rows (minimum 10)                                             |

## 5. HPC benchmark
Workload: count and average price by year × property type over 11 CSV files (1,856 MB). The silver Parquet layer is 334 MB (5.6× smaller). Best-of-N wall-clock time; peak RSS of the driver process.

| engine          |   workers |   seconds |   peak_rss_mb |   speedup_vs_pandas |   speedup_vs_1_worker |   amdahl_parallel_fraction |
|:----------------|----------:|----------:|--------------:|--------------------:|----------------------:|---------------------------:|
| pandas          |         1 |      7.64 |          1727 |                1    |                  1    |                    nan     |
| pandas-chunked  |         1 |      7.52 |           690 |                1.02 |                  1    |                    nan     |
| multiprocessing |         1 |      8.56 |           172 |                0.89 |                  1    |                    nan     |
| multiprocessing |         2 |      6.62 |           172 |                1.15 |                  1.29 |                      0.45  |
| multiprocessing |         4 |      5.9  |           172 |                1.29 |                  1.45 |                      0.414 |
| multiprocessing |         8 |      6.7  |           172 |                1.14 |                  1.28 |                      0.25  |
| dask            |         1 |      9.87 |           408 |                0.77 |                  1    |                    nan     |
| dask            |         2 |      7.64 |           645 |                1    |                  1.29 |                      0.45  |
| dask            |         4 |      6.25 |          1090 |                1.22 |                  1.58 |                      0.489 |
| dask            |         8 |      5.87 |          1864 |                1.3  |                  1.68 |                      0.463 |
| duckdb-csv      |         1 |      6.42 |           951 |                1.19 |                  1    |                    nan     |
| duckdb-csv      |         2 |      4.4  |          1542 |                1.74 |                  1.46 |                      0.63  |
| duckdb-csv      |         4 |      2.51 |          1885 |                3.04 |                  2.56 |                      0.812 |
| duckdb-csv      |         8 |      2.47 |           989 |                3.09 |                  2.6  |                      0.703 |
| duckdb-parquet  |         1 |      0.39 |           396 |               19.59 |                  1    |                    nan     |
| duckdb-parquet  |         2 |      0.21 |           396 |               36.38 |                  1.86 |                      0.925 |
| duckdb-parquet  |         4 |      0.17 |           403 |               44.94 |                  2.29 |                      0.751 |
| duckdb-parquet  |         8 |      0.13 |           416 |               58.77 |                  3    |                      0.762 |
| pyspark         |         1 |     26.4  |          1053 |                0.29 |                  1    |                    nan     |
| pyspark         |         2 |     20.52 |          1023 |                0.37 |                  1.29 |                      0.45  |
| pyspark         |         4 |     19.4  |          1217 |                0.39 |                  1.36 |                      0.353 |
| pyspark         |         8 |     18.5  |          1355 |                0.41 |                  1.43 |                      0.344 |

![Time by engine](figures/07_benchmark_engines.png)

*Time by engine*

![Strong scaling](figures/08_benchmark_scaling.png)

*Strong scaling*

![Peak memory](figures/09_benchmark_memory.png)

*Peak memory*

**Fastest configuration:** `duckdb-parquet` with 8 workers — 0.13 s, **59× faster than single-process pandas** (7.6 s).

**Interpretation.** Single-process pandas must parse every byte of CSV in one thread and hold the whole dataset in memory. Chunking removes the memory problem but not the time. Multiprocessing and Dask give near-linear speed-up while there are files to hand out, then flatten (Amdahl's law — the serial parse of each file bounds the gain). DuckDB parses CSV with vectorised, multi-threaded code and wins on CSV; on the columnar Parquet layer it only reads the four columns it needs, so it is faster still. Spark carries JVM start-up and scheduling overhead that only pays off on data larger than one machine.

## 6. Architecture
```
Land Registry CSVs (2016–2026) ──► data/raw (bronze, incremental manifest)
        ──► data/processed/sales/year=YYYY/*.parquet (silver: DuckDB clean, ZSTD, partitioned)
        ──► data/analytics/property.duckdb (gold: dim_date, dim_location, dim_property, fact_sale, 6 views)
quality.py gates every layer · benchmark.py measures engines/scaling · visualize.py + report.py publish
```
