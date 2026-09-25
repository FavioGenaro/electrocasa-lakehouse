from pyspark import pipelines as dp

from src.electrocasa.utils.metadata import (
    load_metadata,
    get_source
)

from src.electrocasa.utils.schemas import (
    build_schema
)

from src.electrocasa.utils.ingestion import (
    read_file_jdbc,
    add_audit_columns,
    get_table_path
)

import dlt

METADATA_PATH = (
    "/Volumes/electrocasa/bronze/landing/"
    "metadata/bronze/sources.json"
)

metadata = load_metadata(METADATA_PATH)

tracking_config = get_source(metadata, "tracking")

tracking_schema = build_schema(
    tracking_config["schema"]
)

target = get_table_path(tracking_config, "target")


@dp.table(
    name=target,
    comment="Bronze de la tabla tracking",
    table_properties={
        "quality": "bronze",
        "pipelines.reset.allowed": "true",
        "delta.appendOnly": "false",
    },
)
def tracking_bronze():

    options_secret = tracking_config.get("options_secret", {})
    scope_secret = options_secret.get("scope_secret", "")

    user_db = dbutils.secrets.get(scope=scope_secret, key="user_db")
    password_db = dbutils.secrets.get(scope=scope_secret, key="password_db")

    df = read_file_jdbc(
        spark,
        tracking_config,
        tracking_schema,
        user_db,
        password_db
    )

    return add_audit_columns(
        df,
        tracking_config
    )