from pyspark import pipelines as dp
from pyspark.sql.functions import (col, date_trunc, count, sum, round, asc, desc)

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.ventas_sucursal_mes",
    comment="Ventas por sucursal y mes",
    table_properties={
        "quality": "gold",
        "delta.appendOnly": "false",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def ventas_sucursal_mes():

    # ventas = dp.read("electrocasa.silver.ventas")
    ventas = spark.read.table(f"{catalog}.{schema_silver}.ventas_silver")

    return (
        ventas
        .filter(col("sucursal_id").isNotNull())
        .filter(col("cantidad").isNotNull())
        .filter(col("monto_total").isNotNull())
        .withColumn(
            "periodo",
            date_trunc("month", col("fecha_venta")).cast("date")
        )
        .groupBy(
            "periodo",
            "sucursal_id"
        )
        .agg(
            count("*").alias("cantidad_ventas").cast("int"),
            sum("cantidad").alias("unidades_vendidas").cast("int"),
            sum("monto_total").alias("ventas_totales").cast("decimal(18,2)")
        )
        .orderBy(
            asc("periodo"),
            asc("sucursal_id"),
            desc("unidades_vendidas"),
            desc("ventas_totales"),
        )
    )