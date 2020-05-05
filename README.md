# Debezium Partial Snapshots

Gain fine-grained control over your PostgreSQL snapshots with this plugin! 

Problems with managing your evolving Debezium production data?  This plugin was designed to add flexibility on what data gets imported througn Debezium.  For example:

* Decide to add new tables to the whitelist after the initial snapshot but don't want to have to snapshot the entire database again? Avoid resnapshotting everything and skip all or selectively resnapshot a subset of tables.
* A developer performs a data migration that breaks Avro schema migration compatibility? Once the Kafka topics and Schema Registry have been patched to handle the migration, resnapshot only the affected tables.

## Requirements
* Debezium >= 1.1

## Installation

1. Download the appropriate version of the partial snapshotter to match the current version of Debezium installed. If the connector is already running, the Debezium version can be checked by hitting the `GET /connector-plugins` endpoint.
2. Place the download within the Debezium postgres connector directory that is loaded by connect. See  [`plugin.path`](https://kafka.apache.org/documentation/#plugin.path).
3. Start or restart the Kafka Connect service.
4. Start the Debezium postgres connector and let the snapshot complete.
5. Once the snapshot finishes, verify the snapshot tracker table was created.

## Operation

TODO: How the connector tracks already snapshot tables  
TODO: Structure of the tracker table   
TODO: Example of skipping a snapshot