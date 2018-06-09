# python-tools
This repository contains source code for tools used by coinmetrics.io to collect data from Bitcoin and its clones and forks. Currently, we support BTC, LTC, DOGE, ZEC, DCR, PIVX, XVG, DASH, VTC and DGB.

## Prerequisites
Python 2.7, PostgreSQL 9 or 10, Python modules `psycopg2`, `requests`, `python-dateutil` and `future`.

## Reproducing coinmetrics.io data 
We'll use LTC as an example, due to relatively small size of its blockchain. We presume that the tool, postgresql database and LTC node live on the same machine.

* LTC node should be installed and synced with the network. It is essential to launch the node with `txindex=1` flag set.
* Clone this repository and create file `config.json` in the root folder.
* Specify database's and node's parameters in `config.json`:
```json
{
  "db": {
    "user": "your database user",
    "password": "password for specified user",
    "database": "name of the database",
    "host": "localhost"
  },
  "nodes": {
    "ltc": {
      "user": "username set via node's rpcuser parameter",
      "password": "password set via node's rpcpassword parameter",
      "port": "port set via node's rpcport parameter",
      "host": "localhost"
    }
  }
}
```
* Export blockchain to postgres database by running `python export.py ltc`, the process can take a while.
* After initial export is completed, create database indices by running `python export.py ltc --add-index`.
* Create CSV with aggregated daily statistics by running `python csvgen.py ltc`. 
