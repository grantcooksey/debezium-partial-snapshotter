package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.relational.TableId;

public class JdbcFilterHandler implements FilterHandler {
    String jdbcConnection;

    public JdbcFilterHandler(PostgresConnectorConfig config) {
        // TODO
        this.jdbcConnection = "Connection";
    }

    @Override
    public boolean shouldSnapshot(TableId tableId) {
        return !tableId.identifier().equals("Bad");
    }
}
