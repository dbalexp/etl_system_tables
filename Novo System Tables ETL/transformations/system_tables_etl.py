from pyspark import pipelines as dp
from pyspark.sql import functions as F

# Read configuration from pipeline settings
# These values are set in the pipeline Configuration tab
ACCOUNT_IDS = spark.conf.get("account_ids").split(",")
SOURCE_SCHEMA = spark.conf.get("source_schema")
TARGET_CATALOG = spark.conf.get("target_catalog")
TARGET_SCHEMA = spark.conf.get("target_schema")
TABLE_NAMES = spark.conf.get("table_names").split(",")

# Dynamically create filtered tables for each table in TABLE_NAMES
# Each table will filter by the account_ids defined in pipeline configuration

def create_filtered_table(table_name):
    """Factory function to create a filtered table for a given table name"""
    @dp.table(
        name=f"{TARGET_CATALOG}.{TARGET_SCHEMA}.{table_name}",
        comment=f"Filtered version of {SOURCE_SCHEMA}.{table_name} for specified account_ids"
    )
    def filtered_table():
        return spark.read.table(f"{SOURCE_SCHEMA}.{table_name}").filter(
            F.col("account_id").isin(ACCOUNT_IDS)
        )
    return filtered_table

# Create filtered tables for all tables in TABLE_NAMES
for table_name in TABLE_NAMES:
    create_filtered_table(table_name)
