import singer
from appstoreconnect import Api

from tap_appstore.streams import STREAMS


def get_selected_streams(catalog):
    selected_streams = []
    for stream in catalog['streams']:
        for entry in stream['metadata']:
            # Stream metadata will have an empty breadcrumb
            if not entry['breadcrumb'] and entry['metadata'].get('selected', None):
                selected_streams.append(stream)
    return selected_streams


def sync(client: Api, config, state, catalog):
    for catalog_entry in get_selected_streams(catalog):
        stream_name = catalog_entry['tap_stream_id']
        stream_obj = STREAMS[stream_name](client, config, state)
        singer.write_schema(stream_name, catalog_entry['schema'], catalog_entry['key_properties'])
        stream_obj.query_report(catalog_entry)
