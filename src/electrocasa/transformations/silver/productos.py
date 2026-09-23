from pyspark import pipelines as dp
from pyspark.sql.functions import (col, trim, lower, current_timestamp, regexp_replace)

from src.electrocasa.utils.metadata import (load_metadata, get_source)
from src.electrocasa.utils.schemas import (build_schema)
from src.electrocasa.utils.ingestion import (read_file_stream, add_audit_columns, get_table_path, create_auto_cdc_from_metadata)


METADATA_PATH = (
    "/Volumes/electrocasa/bronze/landing/"
    "metadata/silver/silver.json"
)

metadata = load_metadata(METADATA_PATH)
productos_config = get_source(metadata, "productos")
productos_schema = build_schema(productos_config["schema"])
valid_expects = productos_config.get("expectations", {})

source = get_table_path(productos_config, "source")
target = get_table_path(productos_config, "target")

@dp.temporary_view(
    name="view_productos",
    comment="Tabla de productos con validaciones aplicadas"
)
@dp.expect_all(valid_expects)
def staging_productos():

    df = (
        spark.readStream.table(source)
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("nombre_producto", trim(col("nombre_producto")))
        .withColumn("categoria", lower(regexp_replace(trim(col("categoria")), " ", "_")))
        .withColumn("marca", lower(trim(col("marca"))))
        .withColumn("precio_lista", col("precio_lista").cast("decimal(18,2)"))
        .withColumn("updated_at", current_timestamp())
        .select("producto_id", "nombre_producto", "categoria", "marca", "precio_lista", "updated_at")
    )

    return df

# SEGUNDA PARTE

dp.create_streaming_table(
    name=target,
    comment="Tabla de productos en Silver",
    schema=productos_schema,
    table_properties=productos_config["properties"]
)
create_auto_cdc_from_metadata(
    metadata = productos_config,
    target = target,
    source_view = "view_productos",
    name = "productos_flow"
)

