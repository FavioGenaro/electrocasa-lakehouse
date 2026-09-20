from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from pyspark.sql.functions import (
    current_timestamp,
    lit
)


def read_file_stream(
    spark,
    config: dict,
    schema: StructType
) -> DataFrame:

    reader = (
        spark.readStream
            .format("cloudFiles")
            .option(
                "cloudFiles.format",
                config["format"]
            )
            .schema(schema)
    )

    # opciones de configuración
    options = config.get("options", {})

    for key, value in options.items():
        reader = reader.option(key, value)

    return reader.load(
        config["landing_path"]
    )



def add_audit_columns(df: DataFrame, config: dict) -> DataFrame:

    return (
        df
        .withColumn(
            "_ingestion_timestamp",
            current_timestamp()
        )
        .withColumn(
            "_source_file",
            df["_metadata.file_path"]
        )
        .withColumn(
            "_source_system",
            lit(config["name"])
        )
    )