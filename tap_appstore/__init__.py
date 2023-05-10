#!/usr/bin/env python3
import json

import singer
from appstoreconnect import Api
from singer import utils

from tap_appstore.discover import discover, do_discover
from tap_appstore.sync import sync

REQUIRED_CONFIG_KEYS = [
    'key_id',
    'key_file',
    'issuer_id',
    'vendor',
    'start_date'
]

LOGGER = singer.get_logger()


@utils.handle_top_exception(LOGGER)
def main():
    # Parse command line arguments
    args = utils.parse_args(REQUIRED_CONFIG_KEYS)

    config = args.config
    client = Api(config['key_id'], config['key_file'], config['issuer_id'], submit_stats=False)

    state = {}
    if args.state:
        state = args.state

    # If discover flag was passed, run discovery mode and dump output to stdout
    if args.discover:
        catalog = do_discover(client)
        print(json.dumps(catalog.to_dict(), indent=2))
    else:
        catalog = args.catalog if args.catalog else do_discover(client)
        sync(client, config, state, catalog)


if __name__ == '__main__':
    main()
