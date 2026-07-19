# de-tvmaze: Data Engineering Pipeline for TV Maze API

A comprehensive data engineering solution that fetches TV show and episode data from the [TV Maze API](https://www.tvmaze.com/api), processes it through multiple transformation layers using Apache Spark, and orchestrates the entire pipeline using Azure Data Factory.

## 📋 Project Overview

This project implements a modern medallion architecture (Bronze → Silver → Gold) data pipeline on Azure Databricks. It extracts real-time TV show data from the TV Maze API and transforms it into clean, enriched datasets for analytics and reporting.

### Architecture

The pipeline follows a **medallion (layered) architecture**:

- **Bronze Layer**: Raw data ingestion from TV Maze API
- **Silver Layer**: Cleaned and deduplicated data with validation
- **Gold Layer**: Aggregated, business-ready analytics data

## 🏗️ Project Structure

```
de-tvmaze/
├── README.md                              # This file
├── conftest.py                            # Pytest configuration for Databricks environment
├── test_clean_episodes_quality.py         # Data quality tests for episode cleaning
├── src/
│   └── transformation.py                  # Core transformation logic (PySpark)
├── adf/
│   ├── pl_tvmaze_end_to_end.json         # Azure Data Factory pipeline orchestration
│   ├── linkservices_AzureDatabricks1.json # Azure Databricks linked service config
│   └── ntebooks/                          # Databricks notebooks
└── ntebooks/                              # Workspace for notebook development
```

## 🔧 Components

### 1. **Data Pipeline (ADF)**
Located in `adf/pl_tvmaze_end_to_end.json`

Orchestrates 4 sequential Databricks notebook activities:

1. **nb_tvmaze_api_data_fatch** - Fetches raw data from TV Maze API
2. **nb_tvmaze_bronze_dp** - Bronze layer processing (raw data storage)
3. **nb_tvmaze_silver_dp** - Silver layer processing (data cleaning & validation)
4. **nb_tvmaze_gold_dp** - Gold layer processing (aggregated analytics data)

Each activity:
- Runs on Azure Databricks cluster
- Has a 12-hour timeout
- Requires successful completion of the previous step
- Executes independently isolated notebooks

### 2. **Data Transformations**
Located in `src/transformation.py`

Contains the core PySpark transformation logic:

- **`clean_episodes(df: DataFrame) -> DataFrame`**
  - Casts data types (id → int, season → int, runtime → int)
  - Trims whitespace from episode names
  - Converts airdate to proper date format
  - Filters out invalid runtimes (≤ 0)
  - Removes duplicate episodes by episode_id
  - Aliases columns for consistency

### 3. **Data Quality Tests**
Located in `test_clean_episodes_quality.py`

Pytest-based tests for the cleaning transformation:

- ✅ `test_required_fields_not_null` - Ensures episode_id and show_id are never null
- ✅ `test_runtime_positive_or_null` - Validates runtime values are positive or null
- ✅ `test_ids_unique` - Confirms episode_id uniqueness after deduplication

### 4. **Databricks Configuration**
Located in `conftest.py`

Pytest configuration that handles:
- Module registration for Databricks Serverless environment
- Dynamic import of transformation modules
- Compatibility with both notebook and CLI execution

## 🚀 Getting Started

### Prerequisites

- Azure subscription with Databricks workspace
- Azure Data Factory instance
- TV Maze API access (free, no authentication required)
- Python 3.7+
- Apache Spark 2.4+

### Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/BK-VARIA/de-tvmaze.git
   cd de-tvmaze
   ```

2. **Configure Azure Databricks**
   - Upload notebooks to Databricks workspace
   - Update notebook paths in `adf/pl_tvmaze_end_to_end.json` if needed

3. **Deploy Azure Data Factory**
   - Import the pipeline JSON files into your ADF instance
   - Configure linked service for Azure Databricks

4. **Run Tests**
   ```bash
   pytest test_clean_episodes_quality.py -v
   ```

## 📊 Data Flow

```
TV Maze API
    ↓
nb_tvmaze_api_data_fatch (Bronze Ingestion)
    ↓
Bronze Layer (Raw Data)
    ↓
nb_tvmaze_bronze_dp (Validation)
    ↓
Silver Layer (Cleaned Data)
    ↓
nb_tvmaze_silver_dp (Enrichment)
    ↓
Gold Layer (Analytics Ready)
    ↓
nb_tvmaze_gold_dp (Aggregation)
    ↓
Ready for BI & Analytics
```

## 📝 Key Features

- **Modular Design**: Separate transformation logic in `src/` for reusability
- **Data Quality**: Comprehensive pytest tests ensure data integrity
- **Scalable**: Leverages Spark for distributed processing
- **Orchestrated**: Azure Data Factory manages pipeline execution and monitoring
- **Cloud-Native**: Built on Azure Databricks for serverless compute
- **Documented**: Clear code structure with type hints and comments

## 🧪 Testing

Run the data quality tests to validate transformations:

```bash
pytest test_clean_episodes_quality.py -v
```

The tests verify:
- Required fields are populated
- Data type conversions are correct
- Invalid records are filtered
- Duplicates are removed

## 🔍 Sample Data

The test suite uses sample TV show episode data:

| id | show_id | name | season | airdate | runtime |
|----|---------|------|--------|---------|---------|
| 1 | 10 | Pilot | 1 | 2024-01-05 | 45 |
| 2 | 10 | Ep2 | 1 | 2024-01-12 | 45 |

After `clean_episodes()`:
- Whitespace is trimmed from episode names
- Invalid runtimes (≤ 0) are filtered out
- Duplicates are removed

## 📚 Technologies Used

- **Apache Spark** (PySpark) - Data processing engine
- **Azure Databricks** - Managed Spark platform
- **Azure Data Factory** - Pipeline orchestration
- **Python 3** - Transformation logic
- **pytest** - Testing framework

## 📖 API Reference

### TV Maze API
- Endpoint: `https://api.tvmaze.com`
- No authentication required
- Free tier available
- Comprehensive TV show and episode data

## 🤝 Contributing

Contributions are welcome! Please ensure:
1. All tests pass: `pytest`
2. New transformations include test cases
3. Code follows existing style conventions
4. Notebooks are properly documented

## 📄 License

This project is open source and available under the MIT License.

## 👤 Author

**BK-VARIA** - Data Engineering Portfolio

---

**Last Updated**: 2026-07-19  
**Language**: Python | Apache Spark
