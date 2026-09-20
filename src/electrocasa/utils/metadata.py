import json


def load_metadata(path: str) -> dict:
    """
    Lee la configuración de fuentes desde un archivo JSON.
    """

    with open(path, "r") as file:
        return json.load(file)


def get_sources(metadata: dict) -> list:
    """
    Devuelve las fuentes habilitadas.
    """

    return [
        source
        for source in metadata["sources"]
        if source.get("enabled", True)
    ]


def get_source(metadata: dict, source_name: str) -> dict:

    for source in metadata["sources"]:
        if source["name"] == source_name:
            return source

    raise ValueError(
        f"No existe configuración para la fuente: {source_name}"
    )