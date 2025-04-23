import argparse
import boto3
import os
import tempfile
import pyarrow.parquet as pq
import pyarrow as pa
from botocore import UNSIGNED
from botocore.client import Config
from pyiceberg.catalog.rest import RestCatalog

PARQUET_EXTENSION = ".parquet"


def parse_arguments():
    parser = argparse.ArgumentParser(
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )

    parser.add_argument(
        "-L",
        "--local-path",
        action="store",
        help="Local directory containing Parquet files to load into the catalog. This will override the S3 bucket and prefix options.",
        type=str,
        required=False,
        default=None,
    )

    parser.add_argument(
        "-b",
        "--bucket",
        action="store",
        help="Source S3 bucket",
        type=str,
        required=False,
        default=None,
    )

    parser.add_argument(
        "-p",
        "--prefix",
        action="store",
        help="Source S3 bucket prefix",
        type=str,
        required=False,
        default=None,
    )

    parser.add_argument(
        "-E",
        "--endpoint",
        action="store",
        help="Lakekeeper endpoint",
        type=str,
        required=False,
        default=argparse.SUPPRESS,
    )

    parser.add_argument(
        "-T",
        "--token",
        action="store",
        help="Lakekeeper token",
        type=str,
        required=False,
        default=argparse.SUPPRESS,
    )

    parser.add_argument(
        "-w",
        "--warehouse",
        action="store",
        help="Lakekeeper warehouse name",
        type=str,
        required=False,
        default=argparse.SUPPRESS,
    )

    parser.add_argument(
        "-N",
        "--namespace",
        action="store",
        help="Target namespace",
        type=str,
        required=False,
        default=argparse.SUPPRESS,
    )

    parser.add_argument(
        "-t",
        "--table-name",
        action="store",
        help="Target table name",
        type=str,
        required=False,
        default=argparse.SUPPRESS,
    )

    parser.add_argument(
        "-d",
        "--directory",
        action="store",
        help="Optional directory path to store the downloaded Parquet files. A temporary directory will be used if not specified.",
        type=str,
        required=False,
    )

    parser.add_argument(
        "-l",
        "--list-only",
        action="store_true",
        help="List Parquet files in the specified S3 bucket and prefix without downloading or processing them.",
        required=False,
    )

    return parser.parse_args()


def fix_decimal_physical_type(parquet_file):
    """
    Reads a Parquet file and rewrites it with the correct physical type for DECIMAL columns.
    """
    table = pq.read_table(parquet_file)
    schema = table.schema

    fixed_columns = []
    for field in schema:
        if pa.types.is_decimal(field.type):
            if field.type.precision <= 18:
                fixed_columns.append(field.name)

    if fixed_columns:
        print(
            f"[FIXING] Rewriting {parquet_file} to fix DECIMAL columns: {fixed_columns}"
        )
        pq.write_table(table, parquet_file, use_deprecated_int96_timestamps=True)


def list_parquets_in_s3(source_bucket, prefix):
    s3client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    resp = s3client.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
    parquet_files = []
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if key.endswith(PARQUET_EXTENSION):
            full_path = f"s3://{source_bucket}/{key}"
            parquet_files.append(full_path)
    return parquet_files


def get_parquet_files_from_local(local_path):
    return [
        os.path.join(local_path, f)
        for f in os.listdir(local_path)
        if f.endswith(PARQUET_EXTENSION)
    ]


def download_from_s3(source_bucket, prefix, download_dir):
    s3client = boto3.client("s3", config=Config(signature_version=UNSIGNED))
    resp = s3client.list_objects_v2(Bucket=source_bucket, Prefix=prefix)
    for obj in resp.get("Contents", []):
        key = obj["Key"]
        if not key.endswith(PARQUET_EXTENSION) or not key.startswith(f"{prefix}/"):
            continue
        local_path = os.path.join(download_dir, os.path.basename(key))
        s3client.download_file(source_bucket, key, local_path)
        print(f"[DOWNLOAD] {key} -> {local_path}")
    return [
        os.path.join(download_dir, f)
        for f in os.listdir(download_dir)
        if f.endswith(PARQUET_EXTENSION)
    ]


def add_parquets_to_catalog(catalog, parquet_files, namespace, table_name):
    if not parquet_files:
        print(f"[ERROR] No Parquet files found on storage/directory.")
        exit(1)

    catalog.create_namespace_if_not_exists((namespace,))
    print(f"[CREATE NAMESPACE] Namespace '{namespace}' created.")

    table_name = f"{namespace}.{table_name}"

    if catalog.table_exists(table_name):
        print(f"[ERROR] Catalog table '{table_name}' already exists. Aborting...")
        exit(1)

    # Create the table using the schema from the first Parquet file
    first_parquet = pq.read_table(parquet_files[0])
    iceberg_table = catalog.create_table(
        identifier=table_name, schema=first_parquet.schema
    )
    print(
        f"[CREATE CATALOG TABLE] Table '{table_name}' created with schema from {parquet_files[0]}."
    )

    # Append the rest of the Parquet files to the table
    for file in parquet_files:
        fix_decimal_physical_type(file)
        print(f"[UPLOAD] Appending {file} to Iceberg table {table_name}")
        iceberg_table.add_files(file_paths=[file])


if __name__ == "__main__":
    args = parse_arguments()

    if args.list_only:
        if not args.bucket or not args.prefix:
            print(f"[ERROR] Both --bucket and --prefix must be specified to list files.")
            exit(1)

        parquet_files = list_parquets_in_s3(args.bucket, args.prefix)
        print(f"[LIST ONLY] Found {len(parquet_files)} parquet files in S3:")
        for file in parquet_files:
            print(file)
        exit(0)

    if not args.namespace or not args.table_name:
        print(f"[ERROR] Both --namespace and --table-name must be specified to create the catalog table.")
        exit(1)

    catalog = RestCatalog(
        name="iceberg",
        warehouse=args.warehouse,
        uri=args.endpoint,
        token=args.token,
    )

    if args.local_path:
        parquet_files = get_parquet_files_from_local(args.local_path)
    elif args.bucket and args.prefix:
        if args.directory:
            os.makedirs(args.directory, exist_ok=True)
            parquet_files = download_from_s3(args.bucket, args.prefix, args.directory)
            add_parquets_to_catalog(catalog, parquet_files, args.namespace, args.table_name)
        else:
            with tempfile.TemporaryDirectory() as temp_dir:
                parquet_files = download_from_s3(args.bucket, args.prefix, temp_dir)
                add_parquets_to_catalog(catalog, parquet_files, args.namespace, args.table_name)
    else:
        print(f"[ERROR] Either --local-path or both --bucket and --prefix must be specified.")
        exit(1)