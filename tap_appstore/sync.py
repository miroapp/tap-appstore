import singer
from appstoreconnect import Api

from tap_appstore.streams import STREAMS


def get_selected_streams(catalog):
    selected_streams = []
    for stream in catalog.streams:
        stream_metadata = stream.metadata
        for entry in stream_metadata:
            # Stream metadata will have an empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                selected_streams.append((stream.tap_stream_id, stream.to_dict()))

    return selected_streams


def sync(client: Api, config, state, catalog):
    # Write all schemas and init count to 0
    for stream_name, catalog_entry in get_selected_streams(catalog):
        stream_obj = STREAMS[stream_name](client, config, state)
        singer.write_schema(stream_name, catalog_entry['schema'], catalog_entry['key_properties'])
        stream_obj.query_report(catalog_entry)
