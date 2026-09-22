from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType

from pyspark.sql.functions import (
    current_timestamp,
    lit
)

def get_table_path(config: dict, clave: str) -> str:
    # Validar que exista la clave solicitada
    if clave not in config:
        raise KeyError(f"La clave '{clave}' no existe en el diccionario configurado.")
    
    info = config[clave]
    
    catalog = info.get("catalog", "")
    schema = info.get("schema", "")
    table = info.get("table", "")

    # Validar que los campos necesarios 
    if not all([catalog, schema, table]):
        raise ValueError("El diccionario debe contener 'catalog', 'schema' y 'table'.")

    return f"{catalog}.{schema}.{table}"

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


def read_file(
    spark,
    config: dict,
    schema: StructType
) -> DataFrame:

    reader = (
        spark.read
            .format(config["format"])
            .schema(schema)
    )

    # opciones de configuración
    options = config.get("options", {})

    for key, value in options.items():
        reader = reader.option(key, value)

    return reader.load(
        config["landing_path"]
    )

def read_file_jdbc(
    spark,
    config: dict,
    schema: StructType
) -> DataFrame:

    reader = (
        spark.read
            .format("jdbc")
            .schema(schema)
    )

    # opciones de configuración
    options = config.get("options", {})

    for key, value in options.items():
        reader = reader.option(key, value)

    return reader.load()

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



def create_auto_cdc_from_metadata(
    metadata: dict,
    target: str,
    source_view: str,
    name: str,
):

    historization = metadata.get("historization", {})

    # por defecto 1
    scd_type = historization.get("type", 1)

    kwargs = {
        "target": target,
        "source": source_view,
        "keys": historization["keys"],
        "sequence_by": historization["sequence_by"],
        "stored_as_scd_type": scd_type,
        "name": (
            f"cdc_{name}"
            f"_scd{scd_type}"
        )
    }

    # --------------------------------
    # Lista de columnas
    # --------------------------------

    column_list = historization.get("column_list")

    if column_list:
        kwargs["column_list"] = column_list


    track_history = historization.get("track_history_column_list")

    if scd_type == 2 and track_history:
        kwargs["track_history_column_list"] = track_history

    # --------------------------------
    # Crear AUTO CDC
    # --------------------------------

    dp.create_auto_cdc_flow(
        **kwargs
    )