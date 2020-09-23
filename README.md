[![Actions Status](https://github.com/grantcooksey/debezium-partial-snapshotter/workflows/build-master/badge.svg)](https://github.com/grantcooksey/debezium-partial-snapshotter/actions)

# Debezium Partial Snapshots

Gain fine-grained control over your PostgreSQL snapshots with this plugin! 

Problems with managing your evolving Debezium production data?  This plugin was designed to add flexibility on what data gets imported througn Debezium.  For example:

* Decide to add new tables to the whitelist after the initial snapshot but don't want to have to snapshot the entire database again? Avoid resnapshotting everything and skip all or selectively resnapshot a subset of tables.
* A data migration was performed that breaks Avro schema migration compatibility and a snapshot is  needed for recovery? Once the Kafka topics and Schema Registry have been patched to handle the migration, resnapshot only the affected tables.


## Requirements
The plugin requires the running Debezium connector of a version greater than 1.3.0.Beta2.

## Configuration

To use the partial snapshotter the following properties must be set:

```
"snapshot.mode": "custom",
"snapshot.custom.class": "io.debezium.connector.postgresql.snapshot.PartialSnapshotter"
```

The following configuration options are available in addition to what Debezium provides.

Property							| Default	| Description
----- 								| ----- 	| ----
snapshot.partial.table.name	| public.snapshot_tracker | Name of table used to track snapshot status for each table.
snapshot.partial.pk.name		| snapshot\_tracker_pk | Name of primary key for the snapshot tracker table.
snapshot.partial.skip.existing.connector | false | If the partial snapshotter plugin is added to an existing connector, this flag will skip performing a snapshot and instead only create the snapshot tracker table. Assumes the current include.list/exclude.list is monitoring at least one table.

The postgres role that Debezium uses must have create table pri

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
		"slot.drop.on.stop": "false",
		"table.exclude.list": "public.snapshot_tracker",
		"snapshot.mode": "custom",
		"snapshot.custom.class": "io.debezium.connector.postgresql.snapshot.PartialSnapshotter"
    }
}
```

**Note:** it is _strongly_ recommended to use permanent replication slots in production, ie `slot.drop.on.stop` is `false`. The partial snapshotter cannot guarentee to prevent data loss if the replication slot is dropped and only a subset of tables are snapshot.

## Installation

1. Download the appropriate version of the partial snapshotter to match the current version of Debezium installed. If the connector is already running, the Debezium version can be checked by hitting the `GET /connector-plugins` endpoint.
2. Place the downloaded jar within the Debezium postgres connector directory that is loaded by connect. See  [`plugin.path`](https://kafka.apache.org/documentation/#plugin.path).
3. Start or restart the Kafka Connect service and start the Debezium connector with the partial snapshotter configured.

## Operation

The partial snapshotter uses a table (the snapshot tracker table) on the source database to determine what needs to be snapshot. This table uses the Debezium connector property `database.server.name` and the table name (including the schema) as a compound primary key to identify snapshot table. This table schema allows storing multiple Debezium connector snapshot records in the same table.

The first time the partial snapshotter is started, it will create the snapshot tracker table and insert rows for each table that are snapshot. The `needs_snapshot` column controls determining which tables need a snapshot. The intent for this project is to manually update the `needs_snapshot` column for each table that a snapshot is desired. Although streaming will still pause during the snapshot phase, by only performing a snapshot on a subset of tables, the snapshot operation should be less expensive. Snapshots are kicked off by the connector by deleting and recreating the connector.

The query to create the snapshot tracker table is:

```
create table public.snapshot_tracker
(
    table_name     text    not null,
    server_name    text    not null,
    needs_snapshot boolean not null,
    under_snapshot boolean not null,
    constraint snapshot_tracker_pk primary key (table_name, server_name)
);
```

Both the snapshot tracker table name and primary key name are [configurable](#configuration).

This table creation query can be used to precreate the table, with the consideration that the table name must match `snapshot.partial.table.name`.  This is helpful to set granular priviledges for the role that debezium uses without needing to alter default priviledges for a tightly scoped role for Debezium. If the table is precreated, the role will need insert and update priviledges on the snapshot tracker table and execute priviledge on the `to_regclass(text)` function. This function is used to determine the existance of the tracker table.



The snapshotter uses the postgres exported snapshot feature to take a lockless snapshot and queries the table to determine what tables need a snapshot. 

### Common Scenarios

For all examples, we assume we are running the connector on Kafka Connect and are using the following connector configuration:

```
{
    "name": "test-connector",
    "config": {
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname": "localhost", 
		"database.port": "5432", 
		"database.user": "postgres", 
		"database.password": "postgres", 
		"database.dbname" : "postgres", 
		"database.server.name": "test",
		"plugin.name": "pgoutput",
		"slot.drop.on.stop": "false",
		"table.exclude.list": "public.snapshot_tracker",
		"snapshot.mode": "custom",
		"snapshot.custom.class": "io.debezium.connector.postgresql.snapshot.PartialSnapshotter"
    }
}
```

#### Snapshot Everything For A New Connector 

`POST` the connector to connect and once the snapshot is complete, view that snapshot tracker table exists and all `needs_snapshot` columns are `false`.

#### Resnapshot One Table For An Existing Connector

Lets say we want to resnapshot the table `my_table` for a connector that has already been using the connector. `DELETE` the existing connector from the connect cluster. Once the connector is stopped, execute

```
update public.snapshot_tracker
set needs_snapshot = true
where table_name like 'my_table'
  and server_name like 'test';
```

Restart the connector by `POST`ing the config back to the cluster. Once the snapshot is complete, verify that all the `needs_snapshot` columns are `false`.

#### Adding Partial Snapshot Plugin To An Existing Connector Without Performing Full Snapshot

When adding the partial snapshot plugin for an existing connector, it might be desirable to skip performing a snapshot of the entire database just to add the plugin. We can skip the snapshot and onyl create the snapshot tracker table by using the `snapshot.partial.skip.existing.connector` property. 

`DELETE` the existing connector from the connect cluster and modify the connector config to add the `snapshot.partial.skip.existing.connector = true` property. The new config should look like

```
{
    "name": "test-connector",
    "config": {
		"connector.class": "io.debezium.connector.postgresql.PostgresConnector",
		"database.hostname": "localhost", 
		"database.port": "5432", 
		"database.user": "postgres", 
		"database.password": "postgres", 
		"database.dbname" : "postgres", 
		"database.server.name": "test",
		"plugin.name": "pgoutput",
		"slot.drop.on.stop": "false",
		"table.exclude.list": "public.snapshot_tracker",
		"snapshot.mode": "custom",
		"snapshot.custom.class": "io.debezium.connector.postgresql.snapshot.PartialSnapshotter",
		"snapshot.partial.skip.existing.connector": "true"
    }
}
```

Restart the connector by `POST`ing the config back to the cluster. The snapshot will skip but the snapshot tracker table will have been created if it does not yet exist and all the `needs_snapshot` columns for the monitored tables will be `false`.


