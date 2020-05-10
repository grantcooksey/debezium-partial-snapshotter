# Debezium Partial Snapshots

Gain fine-grained control over your PostgreSQL snapshots with this plugin! 

Problems with managing your evolving Debezium production data?  This plugin was designed to add flexibility on what data gets imported througn Debezium.  For example:

* Decide to add new tables to the whitelist after the initial snapshot but don't want to have to snapshot the entire database again? Avoid resnapshotting everything and skip all or selectively resnapshot a subset of tables.
* A data migration was performed that breaks Avro schema migration compatibility and a snapshot is  needed for recovery? Once the Kafka topics and Schema Registry have been patched to handle the migration, resnapshot only the affected tables.

## Requirements
* Debezium >= 1.1

## Configuration

To use the partial snapshotter the following properties must be set:

```
"snapshot.mode": "custom",
"snapshot.custom.class": "io.debezium.connector.postgresql.snapshot.PartialSnapshotter"
```

The following configs have been added in addition to the Debezium configuration options.

Property							| Default	| Description
----- 								| ----- 	| ----
snapshot.partial.table.name	| public.snapshot_tracker | Name of table used to track snapshot status for each table.
snapshot.partial.pk.name		| snapshot\_tracker_pk | Name of primary key for the snapshot tracker table.

Example connector configuration:

```
{
    "name": "local-testing-partial-snapshot-connector",
    "config": {
	"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
	"database.hostname": "localhost", 
	"database.port": "5432", 
	"database.user": "postgres", 
	"database.password": "postgres", 
	"database.dbname" : "postgres", 
	"database.server.name": "test",
	"plugin.name": "pgoutput",
	"slot.drop.on.stop": "true",
	"table.blacklist": "public.snapshot_tracker",
	"snapshot.mode": "custom",
	"snapshot.custom.class": "io.debezium.connector.postgresql.snapshot.PartialSnapshotter"
    }
}
```

## Installation

1. Download the appropriate version of the partial snapshotter to match the current version of Debezium installed. If the connector is already running, the Debezium version can be checked by hitting the `GET /connector-plugins` endpoint.
2. Place the downloaded jar within the Debezium postgres connector directory that is loaded by connect. See  [`plugin.path`](https://kafka.apache.org/documentation/#plugin.path).
3. Start or restart the Kafka Connect service and start the Debezium connector with the partial snapshotter configured.

## Operation

The partial snapshotter uses a table, aka the snapshot tracker table, on the source database to determine what tables need to be snapshot. This table uses the Debezium connector property `database.server.name` and the table name (including the schema) as a compound primary to identify snapshot table. This key allows storing multiple Debezium connector snapshot records in the same table.

The snapshotter uses the postgres exported snapshot feature to take lockless snapshot and queries the table to determine what tables need a snapshot. The first time the partial snapshotter is started, it will create the snapshot tracker table and rows for every table that is snapshot. The `needs_snapshot` column controls determining which tables need a snapshot. This column should be manually updated to perform a snapshot. Kick off the snapshot by deleting and restarting the appropriate Debezium connector.

### Common Scenarios

#### New Connector - Snapshot Everything

TODO

#### Existing Connector - Resnapshot One Table

TODO


TODO: How the connector tracks already snapshot tables  
TODO: Structure of the tracker table   
TODO: Example of skipping a snapshot