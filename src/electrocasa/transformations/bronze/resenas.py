from pyspark import pipelines as dp

from src.electrocasa.utils.metadata import (
    load_metadata,
    get_source
)

from src.electrocasa.utils.schemas import (
    build_schema
)

from src.electrocasa.utils.ingestion import (
    read_file_stream,
    add_audit_columns,
    get_table_path
)

METADATA_PATH = (
    "/Volumes/electrocasa/bronze/landing/"
    "metadata/bronze/sources.json"
)

metadata = load_metadata(METADATA_PATH)

resenas_config = get_source(metadata, "resenas")

resenas_schema = build_schema(
    resenas_config["schema"]
)

target = get_table_path(resenas_config, "target")


@dp.table(
    name=target,
    comment="Bronze de la tabla resenas",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "false",
        "delta.appendOnly": "true",
    },
)
def resenas_bronze():

    df = read_file_stream(
        spark,
        resenas_config,
        resenas_schema
    )

    return add_audit_columns(
        df,
        resenas_config
    )