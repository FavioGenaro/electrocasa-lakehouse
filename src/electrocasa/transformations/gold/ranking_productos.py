from pyspark import pipelines as dp
from pyspark.sql.functions import (col, date_trunc, desc, count, sum, round, dense_rank)
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")


@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.ranking_productos",
    comment="Ranking de productos por ventas y devoluciones",
)
def ranking_productos():

    ventas = (
        # dp.read("electrocasa.silver.ventas")
        spark.read.table(f"{catalog}.{schema_silver}.ventas_silver")
        .withColumn(
            "periodo",
            date_trunc("month", col("fecha_venta"))
        )
        .groupBy(
            "periodo",
            "producto_id"
        )
        .agg(
            sum("cantidad").alias("unidades_vendidas"),
            sum("monto_total").alias("ventas_totales")
        )
    )

    devoluciones = (
        # dp.read("electrocasa.silver.devoluciones")
        spark.read.table(f"{catalog}.{schema_silver}.devoluciones_silver")
        .withColumn(
            "periodo",
            date_trunc(
                "month",
                col("fecha_devolucion")
            )
        )
        .groupBy(
            "periodo",
            "producto_id"
        )
        .agg(
            count("*").alias(
                "cantidad_devoluciones"
            ),
            sum("monto_reembolso").alias(
                "monto_devoluciones"
            )
        )
    )

    productos = spark.read.table(f"{catalog}.{schema_silver}.productos_silver")

    resultado = (
        ventas
        .join(
            devoluciones,
            ["periodo", "producto_id"],
            "full"
        )
        .join(
            productos,
            "producto_id",
            "left"
        )
        .fillna({
            "unidades_vendidas": 0,
            "ventas_totales": 0,
            "cantidad_devoluciones": 0,
            "monto_devoluciones": 0
        })
    )

    return (
        resultado
        .withColumn(
            "ranking_ventas",
            dense_rank().over(
                Window.partitionBy("periodo")
                .orderBy(
                    desc("unidades_vendidas")
                )
            )
        )
        .withColumn(
            "ranking_devoluciones",
            dense_rank().over(
                Window.partitionBy("periodo")
                .orderBy(
                    desc("cantidad_devoluciones")
                )
            )
        )
    )