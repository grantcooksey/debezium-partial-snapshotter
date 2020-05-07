package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.config.Configuration;
import io.debezium.config.Field;
import org.apache.kafka.common.config.ConfigDef;

public class PartialSnapshotConfig {

    public static final String DEFAULT_TRACKER_TABLE_SCHEMA = "public";

    private final String trackerTableName;
    private final String trackerTablePrimaryKeyName;

    public PartialSnapshotConfig(Configuration configuration) {
        this.trackerTableName = configuration.getString(SNAPSHOT_TRACKER_TABLE_NAME);
        this.trackerTablePrimaryKeyName = configuration.getString(SNAPSHOT_TRACKER_PRIMARY_KEY_NAME);
    }

    public String getTrackerTableName() {
        String[] dbObjectSplit = split(trackerTableName);
        return dbObjectSplit[dbObjectSplit.length - 1];
    }

    public String getTackerTableSchemaName() {
        String[] dbObjectSplit = split(trackerTableName);
        return dbObjectSplit.length == 1 ? DEFAULT_TRACKER_TABLE_SCHEMA : dbObjectSplit[0];
    }

    public String getTrackerTablePrimaryKeyName() {
        return trackerTablePrimaryKeyName;
    }

    public static final Field SNAPSHOT_TRACKER_TABLE_NAME = Field.create("snapshot.partial.table.name")
            .withDisplayName("Partial snapshotter tracker table name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Name of table used to track snapshot status for each table.")
            .withDefault("public.snapshot_tracker");

    public static final Field SNAPSHOT_TRACKER_PRIMARY_KEY_NAME = Field.create("snapshot.partial.pk.name")
            .withDisplayName("Partial snapshotter tracker table primary key name")
            .withType(ConfigDef.Type.STRING)
            .withWidth(ConfigDef.Width.LONG)
            .withImportance(ConfigDef.Importance.LOW)
            .withDescription("Name of primary key for the snapshot tracker table.")
            .withDefault("snapshot_tracker_pk");

    private String[] split(String dbObjectName) {
        return dbObjectName.split("\\.", 2);
    }
}
