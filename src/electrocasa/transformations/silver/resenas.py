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
resenas_config = get_source(metadata, "resenas")
resenas_schema = build_schema(resenas_config["schema"])
valid_expects = resenas_config.get("expectations", {})

source = get_table_path(resenas_config, "source")
target = get_table_path(resenas_config, "target")

@dp.temporary_view(
    name="view_resenas",
    comment="Tabla de resenas con validaciones aplicadas"
)
@dp.expect_all(valid_expects)
def staging_resenas():

    df = (
        spark.readStream.table(source)
        .withColumn("resena_id", trim(col("resena_id")))
        .withColumn("producto_id", trim(col("producto_id")))
        .withColumn("cliente_id", trim(col("cliente_id")))
        .withColumn("calificacion", col("calificacion").cast("integer"))
        .withColumn("comentario", lower(trim(col("comentario"))))
        .withColumn("tags", col("tags"))
        .withColumn("respuestas", col("respuestas"))
        .withColumn("fecha_resena", parse_date("fecha_resena").cast("date"))
        .withColumn("updated_at", current_timestamp())
        .select("resena_id", "producto_id", "cliente_id", "calificacion", "comentario", "tags", "respuestas", "fecha_resena", "updated_at")
    )

    return df

# SEGUNDA PARTE

dp.create_streaming_table(
    name=target,
    comment="Tabla de resenas en Silver",
    schema=resenas_schema,
    table_properties=resenas_config["properties"]
)
create_auto_cdc_from_metadata(
    metadata = resenas_config,
    target = target,
    source_view = "view_resenas",
    name = "resenas_flow"
)

