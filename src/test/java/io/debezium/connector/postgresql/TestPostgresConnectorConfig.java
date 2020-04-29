package io.debezium.connector.postgresql;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.snapshot.PartialSnapshotter;
import io.debezium.jdbc.JdbcConfiguration;
import io.debezium.relational.RelationalDatabaseConnectorConfig;

public class TestPostgresConnectorConfig extends PostgresConnectorConfig {

    public static final String DATABASE_CONFIG_PREFIX = "database.";
    public static final String TEST_SERVER = "test_server";
    public static final String TEST_DATABASE = "postgres";
    public static final String SNAPSHOT_TRACKER_TABLE = "public.snapshot_tracker";

    public TestPostgresConnectorConfig(Configuration config) {
        super(config);
    }

    public static JdbcConfiguration defaultJdbcConfig() {
        return JdbcConfiguration.copy(Configuration.fromSystemProperties(DATABASE_CONFIG_PREFIX))
                .withDefault(JdbcConfiguration.DATABASE, TEST_DATABASE)
                .withDefault(JdbcConfiguration.HOSTNAME, "localhost")
                .withDefault(JdbcConfiguration.PORT, 5432)
                .withDefault(JdbcConfiguration.USER, "postgres")
                .withDefault(JdbcConfiguration.PASSWORD, "postgres")
                .build();
    }

    public static Configuration.Builder defaultConfig() {
        JdbcConfiguration jdbcConfiguration = defaultJdbcConfig();
        Configuration.Builder builder = Configuration.create();
        jdbcConfiguration.forEach((field, value) -> builder.with(DATABASE_CONFIG_PREFIX + field, value));
        builder.with(RelationalDatabaseConnectorConfig.SERVER_NAME, TEST_SERVER)
                .with(PostgresConnectorConfig.DROP_SLOT_ON_STOP, true)
                .with(PostgresConnectorConfig.STATUS_UPDATE_INTERVAL_MS, 100)
                .with(PostgresConnectorConfig.PLUGIN_NAME, PostgresConnectorConfig.LogicalDecoder.PGOUTPUT)
                .with(PostgresConnectorConfig.SSL_MODE, PostgresConnectorConfig.SecureConnectionMode.DISABLED)
                .with(PostgresConnectorConfig.INCLUDE_UNKNOWN_DATATYPES, false)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE, PostgresConnectorConfig.SnapshotMode.CUSTOM)
                .with(PostgresConnectorConfig.SNAPSHOT_MODE_CLASS, PartialSnapshotter.class.getName())
                .with(PostgresConnectorConfig.TABLE_BLACKLIST, SNAPSHOT_TRACKER_TABLE);

        return builder;
    }
}
