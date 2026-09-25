from pyspark import pipelines as dp
from pyspark.sql.functions import (col, trim, lower, current_timestamp, regexp_replace)

from src.electrocasa.utils.metadata import (load_metadata, get_source)
from src.electrocasa.utils.schemas import (build_schema)
from src.electrocasa.utils.ingestion import (read_file_stream, add_audit_columns, get_table_path, create_auto_cdc_from_metadata)
from src.electrocasa.utils.utils import (parse_date)


METADATA_PATH = (
    "/Volumes/electrocasa/bronze/landing/"
    "metadata/silver/silver.json"
)

metadata = load_metadata(METADATA_PATH)
tracking_config = get_source(metadata, "tracking")
tracking_schema = build_schema(tracking_config["schema"])
valid_expects = tracking_config.get("expectations", {})

source = get_table_path(tracking_config, "source")
target = get_table_path(tracking_config, "target")

@dp.temporary_view(
    name="view_tracking",
    comment="Tabla de tracking con validaciones aplicadas"
)
@dp.expect_all(valid_expects)
def staging_tracking():

    df = (
        spark.readStream
        .option("ignoreChanges", "true")
        .table(source)
        .withColumn("tracking_id", trim(col("tracking_id")))
        .withColumn("pedido_id", trim(col("pedido_id")))
        .withColumn("courier", lower(trim(col("courier"))))
        .withColumn("estado_entrega", lower(regexp_replace(trim(col("estado_entrega")), " ", "_")))
        .withColumn("sucursal_origen", trim(col("sucursal_origen")))
        .withColumn("fecha_actualizacion", parse_date("fecha_actualizacion").cast("date"))
        .withColumn("updated_at", current_timestamp())
        .select("tracking_id", "pedido_id", "courier", "estado_entrega", "fecha_actualizacion", "sucursal_origen", "updated_at")
    )

    return df

# SEGUNDA PARTE

dp.create_streaming_table(
    name=target,
    comment="Tabla de tracking en Silver",
    schema=tracking_schema,
    table_properties=tracking_config["properties"]
)
create_auto_cdc_from_metadata(
    metadata = tracking_config,
    target = target,
    source_view = "view_tracking",
    name = "tracking_flow"
)

