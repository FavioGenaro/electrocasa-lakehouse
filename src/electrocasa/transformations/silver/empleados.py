from pyspark import pipelines as dp
from pyspark.sql.functions import (col, trim, lower, current_timestamp, regexp_replace, initcap)

from src.electrocasa.utils.metadata import (load_metadata, get_source)
from src.electrocasa.utils.schemas import (build_schema)
from src.electrocasa.utils.ingestion import (read_file_stream, add_audit_columns, get_table_path, create_auto_cdc_from_metadata)
from src.electrocasa.utils.utils import (parse_date)


METADATA_PATH = (
    "/Volumes/electrocasa/bronze/landing/"
    "metadata/silver/silver.json"
)

metadata = load_metadata(METADATA_PATH)
empleados_config = get_source(metadata, "empleados")
empleados_schema = build_schema(empleados_config["schema"])
valid_expects = empleados_config.get("expectations", {})

source = get_table_path(empleados_config, "source")
target = get_table_path(empleados_config, "target")

@dp.temporary_view(
    name="view_empleados",
    comment="Tabla de empleados con validaciones aplicadas"
)
@dp.expect_all(valid_expects)
def staging_empleados():

    df = (
        spark.readStream.table(source)
        .withColumn("id_empleado", trim(col("id_empleado")))
        .withColumn("nombre", trim(initcap(col("nombre"))))
        .withColumn("dni", trim(col("dni")))
        .withColumn("email", trim(col("email")))
        .withColumn("salario", col("salario").cast("decimal(18,2)"))
        .withColumn("sucursal_id", trim(col("sucursal_id")))
        .withColumn("cargo", trim(col("cargo")))
        .withColumn("tipo_evento", trim(lower(regexp_replace(col("tipo_evento"), " ", "_"))))
        .withColumn("fecha_evento", parse_date("fecha_evento").cast("date"))
        .withColumn("updated_at", current_timestamp())
        .select("id_empleado", "nombre", "dni", "email", "salario", "sucursal_id", "cargo", "tipo_evento", "fecha_evento", "updated_at")
    )

    return df

# SEGUNDA PARTE

dp.create_streaming_table(
    name=target,
    comment="Tabla de empleados en Silver",
    schema=empleados_schema,
    table_properties=empleados_config["properties"]
)
create_auto_cdc_from_metadata(
    metadata = empleados_config,
    target = target,
    source_view = "view_empleados",
    name = "empleados_flow"
)

