# tap-appstore

This is a [Singer](https://singer.io) tap that produces JSON-formatted 
data following the [Singer spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md) 
from App Store Connect API results.

Sample config:
```$json
{
  "key_id": "AAAAAAAAA",
  "key_file": "./AuthKey_AAAAAAAAA.p8",
  "issuer_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
  "vendor": "3333333",
  "start_date": "2019-02-01T00:00:00Z"
}
```
