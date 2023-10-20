from typing import List

import singer
from appstoreconnect import Api
from singer import CatalogEntry, Catalog
from tenacity import stop_after_attempt, wait_fixed, retry

from tap_appstore.streams import STREAMS

LOGGER = singer.get_logger()


def get_selected_streams(catalog: Catalog) -> List[CatalogEntry]:
    selected_streams = []
    for stream in catalog.streams:
        for entry in stream.metadata:
            # Stream metadata will have an empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                selected_streams.append(stream)
    return selected_streams


@retry(
    stop=stop_after_attempt(5),
    wait=wait_fixed(1),
)
def sync(client: Api, config, state, catalog: Catalog):
    for catalog_entry in get_selected_streams(catalog):
        LOGGER.info("Syncing stream %s", catalog_entry.tap_stream_id)
        stream_name = catalog_entry.tap_stream_id
        schema_dict = catalog_entry.schema.to_dict()
        stream_obj = STREAMS[stream_name](stream_name, client, config, state)
        singer.write_schema(stream_name, schema_dict, catalog_entry.key_properties)
        stream_obj.query_report(schema_dict)
