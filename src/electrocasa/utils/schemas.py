from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    DoubleType,
    BooleanType,
    DateType,
    TimestampType,
    DecimalType,
    ArrayType
)


def parse_data_type(data_type: str):

    data_type = data_type.lower()

    if data_type == "string":
        return StringType()

    if data_type == "integer":
        return IntegerType()

    if data_type == "long":
        return LongType()

    if data_type == "double":
        return DoubleType()

    if data_type == "boolean":
        return BooleanType()

    if data_type == "date":
        return DateType()

    if data_type == "timestamp":
        return TimestampType()

    if data_type.startswith("decimal"):
        precision, scale = map(
            int,
            data_type.replace("decimal(", "")
                      .replace(")", "")
                      .split(",")
        )

        return DecimalType(precision, scale)

    if data_type.startswith("array<string>"):
        return ArrayType(StringType())
    
    if data_type.startswith("array<objectrespuestaresena>"):
        return ArrayType(
            StructType([
                StructField("autor", StringType(), True),
                StructField("texto", StringType(), True)
            ]),
            True
        )
    
    # if data_type.startswith("array<string>"):
    #     return ArrayType(StringType())
    

    raise ValueError(
        f"Tipo de dato no soportado: {data_type}"
    )


def build_schema(schema_config: dict):

    fields = []

    for column_name, data_type in schema_config.items():

        fields.append(
            StructField(
                column_name,
                parse_data_type(data_type),
                True
            )
        )

    return StructType(fields)