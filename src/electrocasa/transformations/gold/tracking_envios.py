from pyspark import pipelines as dp
from pyspark.sql.functions import (col, countDistinct, row_number, asc, desc)
from pyspark.sql.window import Window

catalog = spark.conf.get("catalog")
schema_silver = spark.conf.get("schema_silver")
schema_gold = spark.conf.get("schema_gold")

@dp.materialized_view(
    name=f"{catalog}.{schema_gold}.tracking_envios",
    comment="Lista de pedidos por sucursal y estado de entrega",
    table_properties={
        "quality": "gold",
        "delta.appendOnly": "false",
        "pipelines.reset.allowed": "true",
        "delta.autoOptimize.optimizeWrite": "true",
        "delta.autoOptimize.autoCompact": "true"
    }
)
def tracking_envios():

    tracking = spark.read.table(f"{catalog}.{schema_silver}.tracking_silver")

    window_pedido = (
        Window
        .partitionBy("pedido_id")
        .orderBy(
            col("fecha_actualizacion").desc()
        )
    )

    # ultimo estado en base a la fecha de actualización
    ultimo_estado = (
        tracking
        .withColumn(
            "_rn",
            row_number().over(window_pedido)
        )
        .filter(
            col("_rn") == 1
        )
        .drop("_rn")
    )

    return (
        ultimo_estado
        .groupBy(
            "sucursal_origen",
            "estado_entrega"
        )
        .agg(
            countDistinct("pedido_id").alias("cantidad_pedidos")
        )
        .orderBy(
            asc("sucursal_origen"),
            desc("cantidad_pedidos")
        )
    )