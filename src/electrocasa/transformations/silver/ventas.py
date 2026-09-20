from pyspark import pipelines as dp

from pyspark.sql.functions import (
    col,
    trim,
    lower,
    current_timestamp
)


valid_expects = {
    "warning_venta_id_null":
        "venta_id IS NOT NULL",

    "warning_sucursal_id_null":
        "sucursal_id IS NOT NULL",

    "warning_producto_id_null":
        "producto_id IS NOT NULL",

    "warning_monto_total_valido":
        "monto_total IS NOT NULL AND monto_total > 0",

    "warning_cantidad_valida":
        "cantidad IS NOT NULL AND cantidad > 0"
}


@dp.temporary_view(
    name="view_ventas"
)
@dp.expect_all(valid_expects)
def staging_ventas():

    df = (
        spark.readStream
        .table(
            "electrocasa.bronze.ventas_bronze"
        )

        .withColumn(
            "venta_id",
            trim(col("venta_id"))
        )

        .withColumn(
            "sucursal_id",
            trim(col("sucursal_id"))
        )

        .withColumn(
            "producto_id",
            trim(col("producto_id"))
        )

        .withColumn(
            "metodo_pago",
            lower(trim(col("metodo_pago")))
        )

        .withColumn(
            "canal",
            lower(trim(col("canal")))
        )

        .withColumn(
            "cantidad",
            col("cantidad").cast("integer")
        )

        .withColumn(
            "monto_total",
            col("monto_total").cast("decimal(18,2)")
        )

        .withColumn(
            "updated_at",
            current_timestamp()
        )
    )

    return df

# SEGUNDA PARTE
# @dp.table(
#     name="electrocasa.silver.ventas_silver",
#     comment="Ventas limpias, estandarizadas y validadas"
# )
# def ventas_silver():

#     return (
#         spark.readStream
#         .table("view_ventas")
#         .dropDuplicates(["venta_id"])
#     )

dp.create_streaming_table(
    name="dbassociate.silver.clientes",
    comment="Estado actual de clientes VÁLIDOS (SCD Tipo 1)", # Es metadata/documentación.
    schema=schema_clientes() # proporcionando explícitamente el esquema.
)

#  source es un delta table o vista y el target si o si es un delta table
dp.create_auto_cdc_flow(
    target="dbassociate.silver.clientes", # tabla de destino, es la tabla streaming
    source="view_clientes", # la vista temporal que definimos arriba, es el origen de los datos
    # AUTO CDC
    keys=["id_cliente"], # el id para comparar los registros e identificarlos
    sequence_by="updated_at", # Esto indica qué columna se utilizará para determinar el orden de los cambios. En caso de duplicados, se usa esta columna para controlar cual seleccionar (de menor a mayor)
    column_list = ["nombre", "email", "ciudad", "fecha_registro", "updated_at"], # columnas que insertará o actualizará. Columnas del source que deben mantenerse/aplicarse en el target como columnas de datos.
    stored_as_scd_type=1, # esto indica que es un SCD tipo 1
    name="clientes_cdc_flow" # nombre del auto scdc, el evento log monitoria los expectation y el cdc y para identificarlo dentro del eventlog necesita el nombre
)