# Databricks notebook source
# Above comment line required for importing into Databricks notebooks using %run magic command
import os
import calcbench as cb
from pyspark.sql import SparkSession

from typing import Sequence, TypeVar, Callable, Optional, List, Literal
import pandas as pd
from tqdm.auto import tqdm

# Set spark config
spark = (
    SparkSession.builder.appName("CalcbenchDeltaLake")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID"))
    .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY"))
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .getOrCreate()
)


def iterate_and_save_delta(
    arguments: Sequence[TypeVar],
    f: Callable[[TypeVar], pd.DataFrame],
    table_name: str,
    bucket_name: str,
    partition_cols: Optional[List[str]] = ["ticker"],
    write_mode: Literal["overwrite", "append"] = "overwrite",
):
    """
    Apply the arguments to a function and save to a Delta table.
    :param arguments: Each item in this sequence will be passed to f
    :param f: Function that generates a pandas dataframe that will be called on arguments
    :param table_name: Name of the Delta table to create or append to
    :param bucket_name: Name of the S3 bucket where the Delta table is stored
    :param partition_cols: Columns to partition the Delta table by
    :param write_mode: "overwrite" to overwrite the table, "append" to append to the table.
    """

    # Set S3 bucket and Delta Lake table root path
    root_path = f"s3://{bucket_name}/{table_name}/"

    # Used to infer Delta Lake schema from first DataFrame iteration
    schema_defined = False

    for argument in tqdm(list(arguments)):
        try:
            df = f(argument)
            if df.empty:
                continue
        except KeyboardInterrupt:
            raise
        except Exception as e:
            tqdm.write(f"Exception getting {argument} {e}")
        else:
            if not schema_defined:
                # Infer the schema from the first DataFrame
                schema = spark.createDataFrame(df).schema

                # Create the Delta table with the inferred schema and partitioning
                spark.sql(
                    f"""
                    CREATE TABLE IF NOT EXISTS {table_name} ({', '.join([f'{field.name} {field.dataType.simpleString()}' for field in schema.fields])})
                    USING DELTA
                    PARTITIONED BY ({', '.join(partition_cols)})
                    LOCATION '{root_path}'
                """
                )

                schema_defined = True

            # Write the data for the current ticker to the Delta table
            spark.createDataFrame(df).write.format("delta").mode(
                write_mode
            ).partitionBy(partition_cols).save(root_path)

            # Refresh the table to pick up new data
            spark.sql(f"REFRESH TABLE {table_name}")


# Class for interacting with Calcbench API
class CalcbenchAPI:
    def __init__(
        self,
        username=os.environ.get("CB_USERNAME"),
        password=os.environ.get("CB_PASSWORD"),
        bucket_name="DATABRICKS-WORKSPACE-BUCKET",
        table_name="CALCBENCH-DATA",
    ):
        if username and password:
            cb.set_credentials(username, password)
        else:
            # Assume environment variables are set
            pass
        cb.enable_backoff(backoff_on=True)

        self.bucket_name = bucket_name
        self.table_name = table_name

    # Helper function to fetch the data for a given ticker
    def fetch_for_ticker(self, ticker):
        data = cb.standardized(
            company_identifiers=[ticker], point_in_time=True
        ).reset_index()

        if data is None:
            print(f"No data found for ticker: {ticker}")
            return

        return data

    # Main method to fetch data and update Delta Lake table
    def update_data(self):
        tickers = ["AAPL", "MSFT", "NVDA"]
        iterate_and_save_delta(
            arguments=tickers,
            f=self.fetch_for_ticker,
            table_name=self.table_name,
            bucket_name=self.bucket_name,
            partition_cols=["ticker"],
            # Delta Lake write mode uses "overwrite" or "append" instead of "w" or "a"
            write_mode="append",
        )


# Fetch data
cb_api = CalcbenchAPI()
cb_api.update_data()
