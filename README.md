# tap-stripe

This is a [Singer](https://singer.io) tap that produces JSON-formatted data
following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/SPEC.md).

## Installation

See the getting-started guide:

https://github.com/singer-io/getting-started

## Usage

This section dives into basic usage of `tap-stripe` by walking through extracting
data from the api.

### Create the configuration file

Create a config file containing the stripe credentials, e.g.:

```json
{
  "client_secret": "sk_live_xxxxxxxxxxxxxxxxxxxxxxxx",
  "account_id": "acct_xxxxxxxxxxxxxxxx",
  "start_date": "2017-01-01T00:00:00Z"
}
```

### Discovery mode

The tap can be invoked in discovery mode to find the available stripe entities.

```bash
$ tap-stripe --config config.json --discover

```

A discovered catalog is output, with a JSON-schema description of each table. A
source table directly corresponds to a Singer stream.

### Field selection

In sync mode, `tap-stripe` consumes the catalog and looks for streams that have been
marked as _selected_ in their associated metadata entries.

Redirect output from the tap's discovery mode to a file so that it can be
modified:

```bash
$ tap-stripe --config config.json --discover > catalog.json
```

Then edit `catalog.json` to make selections. The stream's metadata entry (associated
with `"breadcrumb": []`) gets a top-level `selected` flag, as does its columns' metadata
entries.

```diff
[
  {
    "breadcrumb": [],
    "metadata": {
      "valid-replication-keys": [
        "created"
      ],
      "table-key-properties": [
        "id"
      ],
      "forced-replication-method": "INCREMENTAL",
+      "selected": "true"
    }
  },
]
```

### Sync mode

With a `catalog.json` that describes field and table selections, the tap can be invoked in sync mode:

```bash
$ tap-stripe --config config.json --catalog catalog.json
```

Messages are written to standard output following the Singer specification. The
resultant stream of JSON data can be consumed by a Singer target.

---

Copyright &copy; 2018 Stitch
