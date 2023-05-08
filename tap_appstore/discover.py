import singer
from singer.catalog import Catalog, CatalogEntry, Schema
from appstoreconnect import Api
from appstoreconnect.api import APIError

from tap_appstore.schema import load_schemas
from tap_appstore.streams import STREAMS

LOGGER = singer.get_logger()


def do_discover(client: Api):
    LOGGER.info("Running discover")
    catalog = discover(client)
    LOGGER.info("Completed discover")
    return catalog


def discover(client: Api):
    """
    Run the discovery mode, prepare the catalog file and return catalog.
    """
    schemas, field_metadata = load_schemas()
    catalog = Catalog([])
    for schema_name, schema_dict in schemas.items():
        LOGGER.info("Discovering schema for %s", schema_name)

        try:
            schema = Schema.from_dict(schema_dict)
            mdata = field_metadata[schema_name]
        except Exception as err:
            LOGGER.error(err)
            LOGGER.error('schema_name: %s', schema_name)
            LOGGER.error('type schema_dict: %s', type(schema_dict))
            raise err

        try:
            STREAMS[schema_name](client, {}).get_report()
        except APIError as e:
            raise Exception(f'Report test failed for {schema_name}! \n{e}')

        # create and add catalog entry
        catalog_entry = CatalogEntry(
            stream=schema_name,
            tap_stream_id=schema_name,
            schema=schema,
            key_properties=[],
            metadata=mdata
        )
        catalog.streams.append(catalog_entry)

    if len(catalog.streams) == 0:
        LOGGER.warning("Could not find any reports types to download for the input configuration.")

    return catalog.to_dict()
