# PGAA Lakekeeper Data Loader

The **Lakekeeper Data Loader** is a Python-based tool designed to load Parquet files into an Iceberg catalog. It supports both local Parquet files and files stored in an S3 bucket.

## Features

- Load Parquet files into an Iceberg catalog.
- Support for both local Parquet files and S3-based Parquet files.
- Automatically create namespaces and tables in the Iceberg catalog.
- Fix `DECIMAL` column types in Parquet files to ensure compatibility with Iceberg.
- List Parquet files in an S3 bucket without downloading them.

## Requirements

- **Python**: 3.8+
- **AWS credentials** (if accessing private S3 buckets)
- **Dependencies**:
  - `boto3`
  - `pyarrow`
  - `pyiceberg`
  - `argparse`

## Installation

1. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

## Usage

### Command-Line Arguments

| Short Flag | Long Flag       | Description                                                                 |
|------------|-----------------|-----------------------------------------------------------------------------|
| `-L`       | `--local-path`  | Local directory containing Parquet files to load into the catalog. Overrides S3 bucket options. |
| `-b`       | `--bucket`      | Source S3 bucket.                                                          |
| `-p`       | `--prefix`      | Source S3 bucket prefix.                                                   |
| `-E`       | `--endpoint`    | Lakekeeper endpoint.                                                       |
| `-T`       | `--token`       | Lakekeeper token.                                                          |
| `-w`       | `--warehouse`   | Lakekeeper warehouse name.                                                 |
| `-N`       | `--namespace`   | Target namespace.                                                          |
| `-t`       | `--table-name`  | Target table name.                                                         |
| `-D`       | `--directory`   | Directory to store downloaded Parquet files. Uses a temporary directory if not specified. |
| `-l`       | `--list-only`   | List Parquet files in the specified S3 bucket and prefix without downloading or processing them. |

### Examples

#### Load Parquet Files from S3
```bash
python lakekeeper_loader.py -b my-bucket -p my-prefix -E my-endpoint -T my-token -W my-warehouse -N my-namespace -t my-table-name
```

#### Load Local Parquet Files
```bash
python lakekeeper_loader.py -L /path/to/local/parquet/files -E my-endpoint -T my-token -W my-warehouse -N my-namespace -t my-table-name
```

#### List Parquet Files in S3
```bash
python lakekeeper_loader.py -b my-bucket -p my-prefix -l
```

## How It Works

### Iceberg Catalog Integration
- The script connects to the Iceberg catalog using the provided `--endpoint`, `--token`, and `--warehouse`.
- It creates the specified namespace and table.

### Data Loading
- The script processes each Parquet file, fixing `DECIMAL` column types if necessary, and appends the data to the Iceberg table.

### List-Only Mode
- If `--list-only` is specified, the script lists the Parquet files in the S3 bucket without downloading or processing them.
