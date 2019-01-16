# python-tools
This repository contains source code for tools used by coinmetrics.io to collect data from Bitcoin and its clones and forks. Currently, we support BTC, BCH, LTC, DOGE, ZEC, DCR, PIVX, XVG, DASH, VTC, DGB, BTG, BSV and two assets based on Omni protocol: USDT and MAID.

## Prerequisites
Python 3.6, PostgreSQL 9 or 10, Python modules `psycopg2`, `requests`, `python-dateutil`.

## Reproducing coinmetrics.io data 
We'll use LTC as an example, due to relatively small size of its blockchain. We presume that the tool, postgresql database and LTC node live on the same machine.

* LTC node should be installed and synced with the network. It is essential to launch the node with `txindex=1` flag set.
* Clone this repository and launch the following command from the root directory: ```python3 -m coinmetrics.bitsql.applications.export ltc localhost:db_port:db_name:db_user:db_password localhost:node_rpc_port:node_rpc_user:node_rpc_password```. This will export node data to PostgreSQL database and may take a while.
* After initial export is completed, vacuum tables by running `python3 -m coinmetrics.bitsql.applications.dbcontrol ltc localhost:db_port:db_name:db_user:db_password --vacuum` and then create database indices by running `python3 -m coinmetrics.bitsql.applications.dbcontrol ltc localhost:db_port:db_name:db_user:db_password --add-index`.
* Compute metrics and store them in PostgreSQL tables by running `python3 -m coinmetrics.bitsql.applications.metricmaker ltc localhost:db_port:db_name:db_user:db_password --save`. 
* Optionally, create CSV from metric tables: `python3 -m coinmetrics.applications.utxo_csvmaker ltc localhost:db_port:db_name:db_user:db_password`.
* Produced CSV contains only on-chain data denominated in satoshis. CSVs available at coinmetrics.io can be obtained by combining on-chain and price data collected from, for instance, coinmarketcap.com.
