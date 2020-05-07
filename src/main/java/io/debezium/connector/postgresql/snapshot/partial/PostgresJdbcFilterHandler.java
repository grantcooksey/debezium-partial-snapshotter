package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

public class PostgresJdbcFilterHandler implements FilterHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(PostgresJdbcFilterHandler.class);

    private static final String CREATE_SNAPSHOT_TABLE = "create table \"%s\".\"%s\" (" +
            "collection_name text not null, " +
            "server_name text not null, " +
            "needs_snapshot boolean not null, " +
            "under_snapshot boolean not null, " +
            "constraint \"%s\" primary key (collection_name, server_name));";
    private static final String CHECK_IF_SNAPSHOT_TABLE_EXISTS = "select to_regclass::text as oid from to_regclass(?);";
    private static final String CHECK_FOR_NEEDS_SNAPSHOT = "select collection_name, needs_snapshot, under_snapshot " +
            "from \"%s\".\"%s\" " +
            "where collection_name like ? and server_name like ?";
    private static final String MARK_COLLECTION_FOR_SNAPSHOT = "update \"%s\".\"%s\" " +
            "set under_snapshot=true " +
            "where collection_name like ? and server_name like ?;";
    private static final String INSERT_TRACKER_ROW = "insert into \"%s\".\"%s\" " +
        "(collection_name, server_name, needs_snapshot, under_snapshot) values (?, ?, true, true)";
    private static final String SNAPSHOT_COMPLETED = "update \"%s\".\"%s\" " +
            "set needs_snapshot=false, under_snapshot=false " +
            "where under_snapshot=true and server_name like ?;";


    private JdbcConnection jdbcConnection;
    private final PostgresConnectorConfig postgresConnectorConfig;
    private final PartialSnapshotConfig partialSnapshotConfig;
    private boolean snapshotTrackerTableExists;

    public PostgresJdbcFilterHandler(PostgresConnectorConfig postgresConnectorConfig, PartialSnapshotConfig partialSnapshotConfig) {
        this.postgresConnectorConfig = postgresConnectorConfig;
        this.partialSnapshotConfig = partialSnapshotConfig;
        this.snapshotTrackerTableExists = false;
    }

    @Override
    public boolean shouldSnapshot(TableId tableId) {
        try {
            Connection connection;
            if (jdbcConnection == null) {
                jdbcConnection = new PostgresConnection(postgresConnectorConfig.jdbcConfig());
                connection = jdbcConnection.connection();
                createTable(connection);
            } else {
                connection = jdbcConnection.connection();
            }

            connection.setAutoCommit(false);

            boolean needsSnapshot = false;
            String checkForNeedsSnapshotQuery = buildQueryString(
                    CHECK_FOR_NEEDS_SNAPSHOT,
                    partialSnapshotConfig.getTackerTableSchemaName(),
                    partialSnapshotConfig.getTrackerTableName()
            );
            String insertTrackerRowQuery = buildQueryString(
                    INSERT_TRACKER_ROW,
                    partialSnapshotConfig.getTackerTableSchemaName(),
                    partialSnapshotConfig.getTrackerTableName()
            );
            String markCollectionForSnapshotQuery = buildQueryString(
                    MARK_COLLECTION_FOR_SNAPSHOT,
                    partialSnapshotConfig.getTackerTableSchemaName(),
                    partialSnapshotConfig.getTrackerTableName()
            );
            try (PreparedStatement queryRow = connection.prepareStatement(checkForNeedsSnapshotQuery);
                 PreparedStatement insertTrackerRow = connection.prepareStatement(insertTrackerRowQuery);
                 PreparedStatement markRowForSnapshot = connection.prepareStatement(markCollectionForSnapshotQuery)) {
                queryRow.setString(1, tableId.identifier());
                queryRow.setString(2, postgresConnectorConfig.getLogicalName());

                String collectionName = null;
                boolean underSnapshot = false;
                try (ResultSet rs = queryRow.executeQuery()) {
                    if (rs.next()) {
                        collectionName = rs.getString("collection_name");
                        needsSnapshot = rs.getBoolean("needs_snapshot");
                        underSnapshot = rs.getBoolean("under_snapshot");
                    }
                }

                if (collectionName == null) {
                    insertTrackerRow.setString(1, tableId.identifier());
                    insertTrackerRow.setString(2, postgresConnectorConfig.getLogicalName());
                    int rows = insertTrackerRow.executeUpdate();
                    if (rows != 1) {
                        throw new SQLException("Inserted too many rows for collection {}", tableId.identifier());
                    }
                    needsSnapshot = true;
                }

                if (needsSnapshot && !underSnapshot) {
                    markRowForSnapshot.setString(1, tableId.identifier());
                    markRowForSnapshot.setString(2, postgresConnectorConfig.getLogicalName());
                    int rows = markRowForSnapshot.executeUpdate();
                    if (rows != 1) {
                        throw new SQLException("Updated too many rows for collection {}", tableId.identifier());
                    }
                }
                connection.commit();
            }
            finally {
                connection.setAutoCommit(true);
            }

            return needsSnapshot;
        }
        catch (SQLException e) {
            LOGGER.error("Failed to determine if table needs snapshot", e);
            return false;
        }
    }

    @Override
    public void snapshotCompleted() {
        if (jdbcConnection != null) {
            try {
                Connection connection = jdbcConnection.connection();
                String snapshotCompleteQuery = buildQueryString(
                        SNAPSHOT_COMPLETED,
                        partialSnapshotConfig.getTackerTableSchemaName(),
                        partialSnapshotConfig.getTrackerTableName()
                );
                try (PreparedStatement completeSnapshotUpdate = connection.prepareStatement(snapshotCompleteQuery)) {
                    completeSnapshotUpdate.setString(1, postgresConnectorConfig.getLogicalName());
                    completeSnapshotUpdate.executeUpdate();
                }
            }
            catch (SQLException e) {
                LOGGER.error("Failed to unlock snapshot tracker rows in table: {}", partialSnapshotConfig.getTrackerTableName());
            }
        }
    }

    // TODO look at using a factory to create a handler and using auto closable
    @Override
    public void cleanUp() {
        try {
            if (jdbcConnection != null) {
                jdbcConnection.close();
            }
        }
        catch (SQLException e) {
            LOGGER.error("Failed to close jdbc connection from partial snapshot thread", e);
        }
    }

    private void createTable(Connection connection) throws SQLException {
        if (!snapshotTrackerTableExists) {
            connection.setAutoCommit(false);
            String createTableQuery = buildQueryString(
                    CREATE_SNAPSHOT_TABLE,
                    partialSnapshotConfig.getTackerTableSchemaName(),
                    partialSnapshotConfig.getTrackerTableName(),
                    partialSnapshotConfig.getTrackerTablePrimaryKeyName()
            );
            try (PreparedStatement checkForTable = connection.prepareStatement(CHECK_IF_SNAPSHOT_TABLE_EXISTS);
                 PreparedStatement createTable = connection.prepareStatement(createTableQuery)) {
                checkForTable.setString(1, partialSnapshotConfig.getTrackerTableName());
                try (ResultSet rs = checkForTable.executeQuery()) {
                    rs.next();
                    if (rs.getString("oid") == null) {
                        createTable.executeUpdate();
                    }
                }
                snapshotTrackerTableExists = true;
            }
            finally {
                connection.setAutoCommit(true);
            }
        }
    }

    private String buildQueryString(String query, String... args) {
        return String.format(query, (Object[]) args);
    }
}
