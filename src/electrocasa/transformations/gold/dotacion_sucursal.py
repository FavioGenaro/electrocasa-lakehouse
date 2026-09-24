from pyspark import pipelines as dp
from pyspark.sql.functions import (col, countDistinct, desc)

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.dotacion_sucursal",
    comment="Cantidad de empleados activos por sucursal",
    table_properties={
        "quality": "gold",
        "delta.appendOnly": "false",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def dotacion_sucursal():

    empleados = (
        spark.read.table(f"{catalog}.{schema_silver}.empleados_silver")
        .filter(
            col("__END_AT").isNull()
        )
    )

    return (
        empleados
        .groupBy("sucursal_id")
        .agg(
            countDistinct(
                "id_empleado"
            ).alias("empleados_activos")
        )
        .orderBy(desc("empleados_activos"))

    )