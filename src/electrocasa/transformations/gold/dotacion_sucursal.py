from pyspark import pipelines as dp
from pyspark.sql.functions import (col, countDistinct)

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.dotacion_sucursal",
    comment="Cantidad de empleados activos por sucursal",
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
    )