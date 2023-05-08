# Load schemas from schemas folder
import json
import os

from singer import metadata


def get_abs_path(path):
    """
    Get the absolute path for the schema files.
    """
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas():
    schemas = {}
    field_metadata = {}

    for filename in os.listdir(get_abs_path('schemas')):
        path = os.path.join(get_abs_path('schemas'), filename)
        stream_name = filename.replace('.json', '')
        with open(path) as file:
            schema = json.load(file)
        schemas[stream_name] = schema
        field_metadata[stream_name] = metadata.get_standard_metadata(schema=schema)

    return schemas, field_metadata
