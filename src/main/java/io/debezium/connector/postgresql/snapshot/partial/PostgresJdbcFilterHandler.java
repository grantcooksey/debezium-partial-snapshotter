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

    private static final String TABLE_NAME = "snapshot_tracker";
    private static final String SCHEMA = "public";
    private static final String CREATE_SNAPSHOT_TABLE = "create table " + qualifiedTableName() +" (" +
            "collection_name text constraint " + TABLE_NAME +"_pk primary key," +
            "needs_snapshot boolean not null," +
            "under_snapshot boolean not null );";
    private static final String CHECK_IF_SNAPSHOT_TABLE_EXISTS = "select to_regclass::text as oid from to_regclass(?);";
    private static final String CHECK_FOR_NEEDS_SNAPSHOT = "select collection_name, needs_snapshot, under_snapshot " +
            "from " + qualifiedTableName() +" " +
            "where collection_name like ?";
    private static final String MARK_COLLECTION_FOR_SNAPSHOT = "update " + qualifiedTableName() +" " +
            "set under_snapshot=true " +
            "where collection_name like ?;";
    private static final String INSERT_TRACKER_ROW = "insert into " + qualifiedTableName() +" " +
        "(collection_name, needs_snapshot, under_snapshot) values (?, true, true)";
    private static final String SNAPSHOT_COMPLETED = "update " + qualifiedTableName() +" " +
            "set needs_snapshot=false, under_snapshot=false " +
            "where under_snapshot=true;";


    private JdbcConnection jdbcConnection;
    private final PostgresConnectorConfig config;
    private boolean snapshotTrackerTableExists;

    public PostgresJdbcFilterHandler(PostgresConnectorConfig config) {
        this.config = config;
        this.snapshotTrackerTableExists = false;
    }

    @Override
    public boolean shouldSnapshot(TableId tableId) {
        try {
            Connection connection;
            if (jdbcConnection == null) {
                jdbcConnection = new PostgresConnection(config.jdbcConfig());
                connection = jdbcConnection.connection();
                createTable(connection);
            } else {
                connection = jdbcConnection.connection();
            }

            connection.setAutoCommit(false);

            boolean needsSnapshot = false;
            try (PreparedStatement queryRow = connection.prepareStatement(CHECK_FOR_NEEDS_SNAPSHOT);
                 PreparedStatement insertTrackerRow = connection.prepareStatement(INSERT_TRACKER_ROW);
                 PreparedStatement markRowForSnapshot = connection.prepareStatement(MARK_COLLECTION_FOR_SNAPSHOT)) {
                queryRow.setString(1, tableId.identifier());

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
                    int rows = insertTrackerRow.executeUpdate();
                    if (rows != 1) {
                        throw new SQLException("Inserted too many rows for collection {}", tableId.identifier());
                    }
                    needsSnapshot = true;
                }

                if (needsSnapshot && !underSnapshot) {
                    markRowForSnapshot.setString(1, tableId.identifier());
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
                try (PreparedStatement completeSnapshotUpdate = connection.prepareStatement(SNAPSHOT_COMPLETED)) {
                    completeSnapshotUpdate.executeUpdate();
                }
            }
            catch (SQLException e) {
                LOGGER.error("Failed to unlock snapshot tracker rows in table: {}", qualifiedTableName());
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
            try (PreparedStatement checkForTable = connection.prepareStatement(CHECK_IF_SNAPSHOT_TABLE_EXISTS);
                 PreparedStatement createTable = connection.prepareStatement(CREATE_SNAPSHOT_TABLE)) {
                checkForTable.setString(1, qualifiedTableName());
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

    private static String qualifiedTableName() {
        return SCHEMA + "." + TABLE_NAME;
    }
}
