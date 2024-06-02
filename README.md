# Calcbench Iterate and Save to Delta Lake
Function to iterate and save to Delta Lake format using Calcbench API.

Tested on Databricks + AWS but should work for other platforms supporting Delta Lake. You will need to manually set spark.config, as well as S3 bucket name and Delta Lake table name.

Logic is adapted from and largely similar to Calcbench's other iterate_and_save functions.