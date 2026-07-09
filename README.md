# End-to-End Azure Data Engineering Platform

An end-to-end batch and streaming data platform on Azure. It takes a Spotify-style operational dataset from Azure SQL, moves it through a Bronze / Silver / Gold medallion lakehouse, and serves an analytics-ready star schema. The focus is on the things that make a pipeline real rather than a demo: incremental loading, backfilling, dynamic and reusable components, governance, monitoring, and CI/CD.

<img width="3000" height="1240" alt="architecture" src="https://github.com/user-attachments/assets/589a59a6-bd8a-4aac-9e3e-f660a82790fd" />


## At a glance

| | |
|---|---|
| **Pattern** | Medallion lakehouse (Bronze / Silver / Gold) |
| **Ingestion** | Incremental, dynamic, metadata-driven (single pipeline for all tables) |
| **Transform** | Spark Structured Streaming + Autoloader in Databricks |
| **Model** | Delta Live Tables with SCD Type 1 and Type 2 |
| **Governance** | Unity Catalog (`spotify_cat`) |
| **Monitoring** | Logic Apps email alerts on failure |
| **CI/CD** | Git, ADF ARM templates, Databricks Asset Bundles |

## Contents

- [What it does](#what-it-does)
- [Tech stack](#tech-stack)
- [Data model](#data-model)
- [Engineering highlights](#engineering-highlights)
- [Repository structure](#repository-structure)
- [How it works](#how-it-works)
- [Running it](#running-it)
- [Concepts demonstrated](#concepts-demonstrated)

## What it does

The source is an Azure SQL Database holding a small Spotify-style OLTP model. From there the platform:

1. Ingests each table into the **Bronze** layer of an ADLS Gen2 lake with Azure Data Factory, loading only new rows on each run.
2. Cleans, standardises, and streams the data into a **Silver** layer in Databricks using Spark Structured Streaming and Autoloader.
3. Builds curated **Gold** tables with Delta Live Tables, applying slowly changing dimensions and data quality rules to produce a star schema in the `spotify_cat` catalog.
4. Exposes Gold through a Databricks SQL Warehouse for BI and ad-hoc analysis.

Governance runs through Unity Catalog, monitoring through Logic Apps, and deployment through Git plus Databricks Asset Bundles.

## Tech stack

- **Azure Data Factory** for orchestration and incremental ingestion
- **Azure SQL Database** as the source system
- **Azure Data Lake Storage Gen2** (hierarchical namespace) for the medallion layers
- **Azure Databricks** with **Unity Catalog** for transformation and governance
- **Apache Spark Structured Streaming** and **Autoloader** for incremental reads
- **PySpark** and a reusable transformations module
- **Jinja2** for metadata-driven SQL generation
- **Delta Lake** and **Delta Live Tables** for the curated layer
- **Azure Logic Apps** for failure alerting
- **GitHub**, **ARM templates**, and **Databricks Asset Bundles** for CI/CD
- **Databricks SQL Warehouse** / Power BI for serving

## Data model

The Gold layer is a star schema. The grain of the fact is one row per stream event.

```
                        +----------------+
                        |    dimdate     |
                        +----------------+
                                |
        +----------------+      |      +----------------+
        |    dimuser     |------+------|    dimtrack    |----+
        +----------------+      |      +----------------+    |
                                |                            |
                       +---------------------+       +----------------+
                       |     factstream      |       |    dimartist   |
                       |---------------------|       +----------------+
                       | stream_id (PK)      |
                       | user_id  (FK)       |
                       | track_id (FK)       |
                       | date_key (FK)       |
                       | listen_duration     |
                       +---------------------+
```

- `dimuser`, `dimtrack`, `dimdate` are **SCD Type 2** dimensions, so historical changes are kept with `__START_AT` and `__END_AT` validity columns.
- `dimartist` is snowflaked off `dimtrack` through `artist_id`.
- `factstream` uses an **SCD Type 1** upsert, since each event is already a point in time.

## Engineering highlights

The parts that go beyond a straight copy pipeline.

**Incremental loading with a JSON watermark.** Instead of a watermark control table, each table keeps a small `cdc.json` file in the lake holding the last processed change-data-capture timestamp. The Copy activity reads it, pulls only newer rows, then a Script activity writes the new max back. Initial and incremental loads use the same pipeline, the watermark just starts at `1900-01-01`.

**Backfilling in the same pipeline.** A `from_date` parameter replays a specific window on demand. Empty means normal incremental behaviour, a value overrides the watermark filter, so backfilling needs no second pipeline.

**One dynamic pipeline for every table.** Ingestion is fully parameterised (schema, table, CDC column, from_date) and driven by an array of dictionaries in a ForEach loop. Adding a table is a config change, not a new pipeline.

**Storage optimisation.** An `If` condition checks `dataRead > 0`. When a run finds no new rows, the empty Parquet file it would leave behind is deleted, keeping the lake clean.

**Streaming ingestion with Autoloader.** Silver reads Bronze with `cloudFiles`, giving exactly-once processing through checkpointing plus schema evolution, so new source columns never break the run.

**Reusable transformation utilities.** Shared logic (dedup, column drops) lives in `transformations.py` and is imported into notebooks after appending the project root to `sys.path`, keeping notebooks thin.

**Metadata-driven SQL with Jinja2.** Business views are generated from a parameter array describing tables, columns, aliases, and joins, rendered through a Jinja2 template into a full `SELECT ... LEFT JOIN ...`. New views are a parameter change, not hand-written SQL.

**Declarative modelling with Delta Live Tables.** Dimensions are built with `create_auto_cdc_flow` and `sequence_by` decides the current record. Data quality is enforced with Expectations (`expect_all_or_drop`) on the streaming tables.

**Governance and monitoring.** Unity Catalog manages the metastore, credentials, and external locations for each container. A Logic App triggered by a Web activity emails on failure with the pipeline name and run ID.

**CI/CD.** ADF changes flow feature branch to PR to main and publish through ARM templates. Databricks code is packaged as an Asset Bundle and deployed to `dev` and `prod` targets.

## Repository structure

```
spotify-azure-de/
├── README.md
├── docs/
│   └── architecture.svg
├── source_scripts/
│   ├── initial_load.sql              # creates and seeds source tables
│   └── incremental_load.sql          # inserts new rows for incremental testing
├── adf/                              # exported Azure Data Factory assets
│   ├── linkedService/                # Azure SQL + ADLS Gen2 connections
│   ├── dataset/                      # parameterised JSON / Parquet datasets
│   └── pipeline/
│       ├── incremental_ingestion     # single-table dynamic pipeline
│       └── incremental_loop          # metadata-driven ForEach pipeline
└── databricks/
    └── spotify_dab/                  # Databricks Asset Bundle
        ├── databricks.yml            # bundle definition (dev / prod targets)
        ├── resources/
        │   └── gold_pipeline.pipeline.yml
        └── src/
            ├── silver/
            │   └── silver_Dimensions          # Autoloader + PySpark transforms
            ├── gold/
            │   └── dlt/                        # Delta Live Tables pipeline
            │       ├── transformations/        # the pipeline DAG source
            │       │   ├── DimUser.py          # SCD2
            │       │   ├── DimTrack.py         # SCD2
            │       │   ├── DimDate.py          # SCD2
            │       │   └── FactStream.py       # SCD1 upsert
            │       ├── explorations/           # scratch notebooks
            │       └── utilities/
            ├── jinja/
            │   └── jinja_notebook              # metadata-driven view generator
            └── utils/
                └── transformations.py          # reusable transform functions
```

## How it works

### Bronze (Azure Data Factory)

Data Factory connects to Azure SQL through a linked service and lands each table as Parquet in the `bronze` container. The `incremental_loop` pipeline is incremental, dynamic, and self-cleaning:

- A `Lookup` reads the table's `cdc.json` watermark from the lake.
- A `Copy` (`AzureSQLtoLake`) runs `SELECT * FROM {schema}.{table} WHERE {cdc_column} > {last_cdc}` and writes a timestamped Parquet file.
- An `If` condition (`IfIncrementalData`) checks whether rows were read. If yes, a `Script` activity (`max_cdc`) fetches the new max and a `Copy` (`update_last_cdc`) overwrites the watermark. If no, a `Delete` removes the empty file.
- A `from_date` parameter enables backfilling without touching the pipeline.
- Everything runs inside a `ForEach` over a metadata array, so all tables load from one pipeline.
- On failure a `Web` activity calls a Logic App that emails an alert.

### Silver (Azure Databricks)

The `silver_Dimensions` notebook reads Bronze with Autoloader and applies cleaning:

- Standardise text (for example uppercasing usernames).
- Regex clean-up on track names.
- Derived flags (for example a duration band with `when / otherwise`).
- Deduplication on primary keys.
- Drop the Autoloader `_rescued_data` column via the shared utility.

Output is written as Delta with `trigger(once=True)`, checkpointed for idempotency, and registered under `spotify_cat.silver`. A separate Jinja2 notebook generates business views on demand.

### Gold (Delta Live Tables)

The `gold_pipeline` is declarative. Each dimension has a staging view over Silver (for example `dimuser_stg`), then `create_auto_cdc_flow` builds the SCD Type 2 table with `sequence_by` picking the current record. The fact uses SCD Type 1. Expectations enforce data quality (for example non-null keys) and can warn, drop, or fail. Output lands in `spotify_cat.gold` as a star schema, with the incremental run upserting only changed rows.

## Running it

> You need an Azure subscription. SQL and Databricks are tuned for minimal spend and meant to be deleted after the run.

1. **Provision** one resource group with ADLS Gen2 (hierarchical namespace on), Azure Data Factory, Azure SQL Database (serverless, 1 vCore), and Azure Databricks (premium / trial for Unity Catalog).
2. **Create containers** `bronze`, `silver`, `gold`, plus `databricks-metastore`.
3. **Seed the source** by running `source_scripts/initial_load.sql`.
4. **Wire up Data Factory**: connect Git, create the Azure SQL and ADLS linked services, import the pipelines, and run `incremental_loop`.
5. **Set up governance**: create an access connector, grant it `Storage Blob Data Contributor`, create a Unity Catalog metastore, credential, and external locations for each container, then create the `spotify_cat` catalog with `silver` and `gold` schemas.
6. **Run Silver** (`silver_Dimensions`) to populate the Silver Delta tables.
7. **Run the Gold pipeline** (`gold_pipeline`) to build the star schema.
8. **Test incrementally** with `incremental_load.sql`, rerun ingestion, rerun the DLT pipeline, and confirm only changed rows flow through and SCD history is recorded.
9. **Deploy** with `databricks bundle deploy --target dev`.

## Concepts demonstrated

A quick reference, useful for interviews:

- Medallion architecture (Bronze / Silver / Gold)
- Incremental loading and watermarking without stored procedures
- Backfilling as a first-class pipeline feature
- Parameterised, metadata-driven pipelines
- Spark Structured Streaming, Autoloader, checkpointing, idempotency
- Schema evolution
- PySpark transformations and modular, reusable code
- Jinja2 templating for dynamic SQL
- Star schema and dimensional modelling
- Slowly changing dimensions (Type 1 and Type 2)
- Delta Lake and Delta Live Tables with data quality Expectations
- Unity Catalog governance (metastore, credentials, external locations)
- Monitoring and alerting with Logic Apps
- Git branching, ARM template publishing, and Databricks Asset Bundles
