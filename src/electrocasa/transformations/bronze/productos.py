from pyspark import pipelines as dp

from src.electrocasa.utils.metadata import (
    load_metadata,
    get_source
)

from src.electrocasa.utils.schemas import (
    build_schema
)

from src.electrocasa.utils.ingestion import (
    read_file,
    add_audit_columns,
    get_table_path
)

METADATA_PATH = (
    "/Volumes/electrocasa/bronze/landing/"
    "metadata/bronze/sources.json"
)

metadata = load_metadata(METADATA_PATH)

productos_config = get_source(metadata, "productos")

productos_schema = build_schema(
    productos_config["schema"]
)

target = get_table_path(productos_config, "target")


@dp.table(
    name=target,
    comment="Bronze de la tabla productos",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.appendOnly": "false",
    },
)
def productos_bronze():

    df = read_file(
        spark,
        productos_config,
        productos_schema
    )

    return add_audit_columns(
        df,
        productos_config
    )