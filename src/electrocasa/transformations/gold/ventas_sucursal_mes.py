from pyspark import pipelines as dp
from pyspark.sql.functions import (col, date_trunc, count, sum, round)

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.ventas_sucursal_mes",
    comment="Ventas por sucursal y mes",
)
def ventas_sucursal_mes():

    # ventas = dp.read("electrocasa.silver.ventas")
    ventas = spark.read.table(f"{catalog}.{schema_silver}.ventas_silver")

    return (
        ventas
        .withColumn(
            "periodo",
            date_trunc("month", col("fecha_venta"))
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
        .withColumn(
            "ticket_promedio",
            round(
                col("ventas_totales") /
                col("cantidad_ventas"),
                2
            )
            .cast("decimal(18,2)")
        )
    )