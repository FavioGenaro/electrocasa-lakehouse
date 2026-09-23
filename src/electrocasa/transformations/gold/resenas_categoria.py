from pyspark import pipelines as dp
from pyspark.sql.functions import (col, date_trunc, when, count, sum, avg, round)

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")


@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.resenas_categoria",
    comment="Cantidad reseñas negativas por categoria de producto",
)
def resenas_categoria():

    resenas = (
        spark.read.table(f"{catalog}.{schema_silver}.resenas_silver")
        .withColumn(
            "periodo",
            date_trunc(
                "month",
                col("fecha_resena")
            )
        )
    )

    productos = spark.read.table(f"{catalog}.{schema_silver}.productos_silver")
    
    return (
        resenas
        .join(
            productos.select(
                "producto_id",
                "categoria"
            ),
            "producto_id",
            "left"
        )
        .withColumn(
            "es_negativa",
            when(
                col("calificacion") <= 2,
                1
            ).otherwise(0)
        )
        .groupBy(
            "periodo",
            "categoria"
        )
        .agg(
            count("*").alias("total_resenas"),

            sum("es_negativa")
             .alias("resenas_negativas"),

            round(
                avg("calificacion"),
                2
            ).alias("calificacion_promedio")
        )
        .withColumn(
            "tasa_resenas_negativas",
            round(
                col("resenas_negativas") /
                col("total_resenas"),
                4
            )
        )
    )