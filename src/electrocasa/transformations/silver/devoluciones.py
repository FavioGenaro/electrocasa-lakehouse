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
devoluciones_config = get_source(metadata, "devoluciones")
devoluciones_schema = build_schema(devoluciones_config["schema"])
valid_expects = devoluciones_config.get("expectations", {})

source = get_table_path(devoluciones_config, "source")
target = get_table_path(devoluciones_config, "target")

@dp.temporary_view(
    name="view_devoluciones",
    comment="Tabla de devoluciones con validaciones aplicadas"
)
@dp.expect_all(valid_expects)
def staging_devoluciones():

    df = (
        spark.readStream.table(source)
        .withColumn("devolucion_id", trim(col("devolucion_id")))
        .withColumn("pedido_id", trim(col("pedido_id")))
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("motivo", lower(regexp_replace(trim(col("motivo")), " ", "_")))
        .withColumn("monto_reembolso", col("monto_reembolso").cast("decimal(18,2)"))
        .withColumn("fecha_devolucion", parse_date("fecha_devolucion").cast("date"))
        .withColumn("updated_at", current_timestamp())
        .select("devolucion_id", "pedido_id", "sucursal_id", "producto_id", "motivo", "monto_reembolso", "fecha_devolucion", "updated_at")
    )

    return df

# SEGUNDA PARTE

dp.create_streaming_table(
    name=target,
    comment="Tabla de devoluciones en Silver",
    schema=devoluciones_schema,
    table_properties=devoluciones_config["properties"]
)
create_auto_cdc_from_metadata(
    metadata = devoluciones_config,
    target = target,
    source_view = "view_devoluciones",
    name = "devoluciones_flow"
)

