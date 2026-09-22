# from pyspark import pipelines as dp

# from src.electrocasa.utils.metadata import (
#     load_metadata,
#     get_source
# )

# from src.electrocasa.utils.schemas import (
#     build_schema
# )

# from src.electrocasa.utils.ingestion import (
#     read_file_jdbc,
#     add_audit_columns,
#     get_table_path
# )

# METADATA_PATH = (
#     "/Volumes/electrocasa/bronze/landing/"
#     "metadata/bronze/sources.json"
# )

# metadata = load_metadata(METADATA_PATH)

# tracking_config = get_source(metadata, "tracking")

# tracking_schema = build_schema(
#     tracking_config["schema"]
# )

# target = get_table_path(tracking_config, "target")


# @dp.table(
#     name=target,
#     comment="Bronze de la tabla tracking",
#     table_properties={
#         "quality": "bronze",
#         "pipelines.reset.allowed": "false",
#         "delta.appendOnly": "true",
#     },
# )
# def tracking_bronze():

#     df = read_file_jdbc(
#         spark,
#         tracking_config,
#         tracking_schema
#     )

#     return add_audit_columns(
#         df,
#         tracking_config
#     )