# Sales Data Pipeline - Technical Assignment

## Overview

For this assignment, I built a PySpark pipeline that reads all 12 monthly sales CSV files in one run, cleans and deduplicates the data, applies the final data types, and writes the result to a cleansed layer.

I built and tested it in Databricks Free Edition. Since I did not have an ADLS environment available, I used a Unity Catalog Volume to represent the raw, cleansed, and quarantine areas.

I kept the solution simple for the size of the dataset, while making the main design decisions in a way that could scale later.

## Approach

The pipeline has six stages:

1. **Read**: read all 12 CSV files using one `spark.read`, initially keeping the columns as strings.
2. **Clean**: remove repeated header rows and completely blank rows.
3. **Deduplicate**: remove exact duplicates using a Window function with `row_number()`.
4. **Transform**: identify conflicting order lines, cast data types, parse the order timestamp, derive `year` and `month`, and rename columns to snake_case.
5. **Write**: write the cleansed data as both a partitioned dataset and a single-file version. Conflicting records are written to `quarantine/`.
6. **Validate**: read the outputs back and run the final data quality checks.

Each stage includes logging and error handling. If a stage fails, the pipeline stops instead of continuing with partially processed data.

## Row counts

| Step | Rows |
|---|---:|
| Raw input | 186,850 |
| After removing header and blank rows | 185,950 |
| After removing exact duplicates | 185,686 |
| Cleansed output | 185,592 |
| Quarantine output | 94 |

Final reconciliation:

```text
185,592 cleansed + 94 quarantined = 185,686 deduplicated rows
```

## Data exploration

The source contains six columns: Order ID, Product, Quantity Ordered, Price Each, Order Date, and Purchase Address.

I found:

- **355 repeated header rows**
- **545 completely blank rows**
- **264 exact duplicate rows**
- **47 conflicting order lines**, represented by 94 rows
- **34 records from 1 January 2020**, even though the source files represent 2019 sales

An Order ID can contain several products, so Order ID alone is not unique. For this dataset, the order-line grain is:

```text
Order ID + Product
```

The 47 conflicting order lines have the same Order ID, Product, Price, Order Date, and Purchase Address, but different quantities.

I could not find a reliable pattern showing which quantity was correct, so I did not automatically choose one.

## Answers to the assignment questions

### 1. Read all files, combine them into a single file, and write to the cleansed folder

All 12 source files are read using one `spark.read`, so they are processed together as one DataFrame in a single run.

After cleaning and transformation, I write the same DataFrame in two ways:

```text
cleansed/sales_combined/
cleansed/sales_partitioned/
```

`sales_combined` contains the full cleansed dataset as one physical data file, matching the single-file requirement.

`sales_partitioned` contains the same data partitioned by year and month. This is the version I would recommend for production.

I kept both because a single physical file and a partitioned dataset are different layouts. Validation confirms that both contain the same number of rows and that the combined version contains one data file.

At larger volumes, I would keep only the partitioned version because forcing Spark to write one file limits the benefit of distributed processing.

### 2. File format: Delta Lake

I chose Delta Lake because this is a cleansed reporting layer and I wanted to preserve the schema and final data types instead of writing everything back to CSV.

Delta uses Parquet underneath, which gives columnar and compressed storage suitable for analytical workloads.

It also provides useful functionality for a production pipeline, including:

- transactional writes
- schema enforcement
- version history and time travel
- `MERGE` support

The columns are also renamed to snake_case to make them easier to use in PySpark and SQL.

### 3. Partitioning strategy: year and month

I chose `year` and `month` as the partition columns because sales reporting is normally filtered by time periods.

This allows Spark to skip unrelated partitions instead of scanning the full dataset.

I included the year because the source already crosses a year boundary. There are 34 records from January 2020, so partitioning only by month would mix January 2019 and January 2020.

I avoided high-cardinality columns such as Order ID, Product, or Purchase Address because they could create a large number of small partitions.

For this sample dataset, partitioning is not necessary for performance. I chose this strategy based on how I would expect the table to be queried if it grew in production.

### 4. Implementing the partitioning strategy

The implementation is:

1. Parse Order Date into a timestamp using `MM/dd/yy HH:mm`.
2. Derive `year` and `month`.
3. Repartition the DataFrame using those columns.
4. Write using `partitionBy("year", "month")`.
5. Read the result back and validate row counts, parsing, and uniqueness.

The resulting layout follows this pattern:

```text
year=2019/month=1/
year=2019/month=2/
...
year=2020/month=1/
```

## Partitioning considerations

### Date parsing

If the source date does not match the expected format, Spark can produce a null value.

I check for unexpected nulls after casting and stop the run if any are found.

### Records outside 2019

I kept the 34 January 2020 records because there was nothing indicating that they were invalid.

Including `year` in the partition key handles them naturally.

### Small files

January 2020 is much smaller than the normal monthly partitions, but at this data size that is not an issue.

At larger scale, I would monitor file sizes and use repartitioning or Databricks `OPTIMIZE` where needed.

The quarantine output is not partitioned because it contains only 94 rows.

### Reprocessing

For this assignment, I use a full overwrite.

This makes the pipeline idempotent for the supplied input, so rerunning it does not append duplicate data.

For recurring monthly loads, I would process only new or affected data and use partition-level processing or Delta `MERGE`.


## Deduplication and conflicting records

Exact duplicates are removed using a Window function with `row_number()`.

`dropDuplicates()` would also work for exact copies, but I used the Window approach because it is easier to extend later if the deduplication rule becomes more complex.

The conflicting order lines are handled differently because they are not exact duplicates.

I did not want to guess which quantity was correct. Keeping both would inflate sales totals, while choosing one without a business rule would hide a real data quality issue.

So both versions are moved to `quarantine/`, together with a reason and processing timestamp.

This leaves one row per valid order line in the cleansed output.

If the business later provides a resolution rule, the corrected records can be merged back into the Delta dataset using `MERGE`.

A disabled example is included in the notebook:

```python
RUN_RESOLUTION_DEMO = False
```

## Data types

The main fields are converted to:

```text
quantity_ordered -> int
price_each       -> decimal(10,2)
order_date       -> timestamp
```

I used `decimal` for Price because it is monetary data and should not rely on floating-point precision.

Order Date is kept as a timestamp because the source includes the time of day and there was no reason to remove it.

## Data quality checks

The pipeline includes checks throughout the process:

- **Cleaning reconciliation**: removed rows must equal repeated headers plus blank rows.
- **Deduplication**: exact duplicates removed are counted and logged.
- **Casting**: unexpected nulls after casting cause the pipeline to fail.
- **Output reconciliation**: cleansed rows plus quarantine rows must equal the deduplicated input.
- **Partitioned vs combined**: both outputs must contain the same number of rows, and the combined version must contain one data file.
- **Uniqueness**: the cleansed output must have no duplicate `order_id + product` keys.

For this run:

```text
355 + 545 = 900 rows removed during cleaning
185,950 - 264 = 185,686 rows after deduplication
185,592 + 94 = 185,686 final reconciliation
```

## Assumptions and considerations

I treated this as a one-time batch load, so the outputs are overwritten on each run.

For recurring monthly loads, I would move to incremental ingestion using Auto Loader and either partition-level processing or Delta `MERGE`, depending on whether historical records can change.

For the assignment, `raw`, `cleansed`, and `quarantine` are folders inside one Unity Catalog Volume. I would separate them only if different permissions, ownership, or retention rules required it.

In production, I would also expose the cleansed Delta dataset as a Unity Catalog table so it can be queried and governed directly through SQL.

## Output structure

```text
sales_data/                       (Unity Catalog Volume)
├── raw/
│   └── Sales_Data_2019/          12 source CSV files
│
├── cleansed/
│   ├── sales_partitioned/        Delta, partitioned by year/month
│   └── sales_combined/           Delta, single data file
│
└── quarantine/                   Delta, conflicting order lines
```

## How to run

1. Upload the 12 CSV files to:

```text
/Volumes/workspace/default/sales_data/raw/Sales_Data_2019/
```

2. Open the notebook in Databricks.
3. Attach compute. Serverless also works.
4. Check the paths in the first configuration cell.
5. Select **Run All**.

The logs show progress through:

```text
Read
Clean
Deduplicate
Transform
Write
Validate
```

If a stage or validation check fails, the notebook stops and reports the error.

## Running outside Databricks

The notebook uses Databricks-specific functionality, mainly `/Volumes` paths and Delta Lake.

To run the same pipeline on plain Apache Spark, I would change the storage paths and either install `delta-spark` or write the cleansed output as Parquet.

The main PySpark transformation logic would remain the same.
