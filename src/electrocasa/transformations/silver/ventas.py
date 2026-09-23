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
ventas_config = get_source(metadata, "ventas")
ventas_schema = build_schema(ventas_config["schema"])
valid_expects = ventas_config.get("expectations", {})

source = get_table_path(ventas_config, "source")
target = get_table_path(ventas_config, "target")

@dp.temporary_view(
    name="view_ventas",
    comment="Tabla de ventas con validaciones aplicadas"
)
@dp.expect_all(valid_expects)
def staging_ventas():

    df = (
        spark.readStream.table(source)
        .withColumn("venta_id", trim(col("venta_id")))
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("metodo_pago", lower(regexp_replace(trim(col("metodo_pago")), " ", "_")))
        .withColumn("fecha_venta", parse_date("fecha_venta").cast("date"))
        .withColumn("canal", lower(trim(col("canal"))))
        .withColumn("cantidad", col("cantidad").cast("integer"))
        .withColumn("monto_total", col("monto_total").cast("decimal(18,2)"))
        .withColumn("updated_at", current_timestamp())
        .select("venta_id", "sucursal_id", "producto_id", "cantidad", "monto_total", "metodo_pago", "fecha_venta", "canal", "updated_at")
    )

    return df

# SEGUNDA PARTE

dp.create_streaming_table(
    name=target,
    comment="Tabla de ventas en Silver",
    schema=ventas_schema,
    table_properties=ventas_config["properties"]
)
create_auto_cdc_from_metadata(
    metadata = ventas_config,
    target = target,
    source_view = "view_ventas",
    name = "ventas_flow"
)

