package io.debezium.connector.postgresql.snapshot;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.TestPostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.relational.RelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceRecord;
import org.hamcrest.core.AnyOf;
import org.hamcrest.core.StringContains;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.nio.file.Paths;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

public class PartialSnapshotterTest extends BaseTest {

    private static final String CLEAN_UP_SCHEMA = "DROP SCHEMA IF EXISTS public CASCADE;" +
            "DROP SCHEMA IF EXISTS snapshot CASCADE;" +
            "CREATE SCHEMA public;";
    private static final String CREATE_TEST_DATA_TABLES =
            "create table test_data (id integer not null constraint table_name_pk primary key, name text);" +
            "create table another_test_data (id integer not null constraint another_table_name_pk primary key, name text);";
    private static final String CREATE_SNAPSHOT_TABLE = "create table public.snapshot_tracker (" +
            "collection_name text not null, " +
            "server_name text not null, " +
            "needs_snapshot boolean not null, " +
            "under_snapshot boolean not null, " +
            "constraint snapshot_tracker_pk primary key (collection_name, server_name));";

    @Before
    public void before() {
        TestUtils.execute(postgreSQLContainer, CLEAN_UP_SCHEMA);
    }

    @After
    public void after() {
        TestUtils.execute(postgreSQLContainer, CLEAN_UP_SCHEMA);
    }

    @Test
    public void testEngine() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');");
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            engine.start(consumer);
            waitForSnapshotToBeCompleted();

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));

            List<SourceRecord> records = consumer.get(1);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testFilterOneTablePartialSnapshot() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');",
                "insert into another_test_data (id, name) VALUES (1, 'dirt');",
                CREATE_SNAPSHOT_TABLE,
                "insert into snapshot_tracker (collection_name, server_name, needs_snapshot, under_snapshot) values " +
                        "('public.another_test_data', '" + TestPostgresConnectorConfig.TEST_SERVER + "', false, false);"
        );
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));

            List<SourceRecord> records = consumer.get(1);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testFilterAllTablesPartialSnapshot() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');",
                "insert into another_test_data (id, name) VALUES (1, 'dirt');",
                CREATE_SNAPSHOT_TABLE,
                "insert into snapshot_tracker (collection_name, server_name, needs_snapshot, under_snapshot) values " +
                        "('public.test_data', '" + TestPostgresConnectorConfig.TEST_SERVER + "', false, false);",
                "insert into snapshot_tracker (collection_name, server_name, needs_snapshot, under_snapshot) values " +
                        "('public.another_test_data', '" + TestPostgresConnectorConfig.TEST_SERVER + "', false, false);"
        );
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testFilterNoTablesPartialSnapshot() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');",
                "insert into another_test_data (id, name) VALUES (1, 'dirt');");
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));
            addExpectedRecord(expectedRecords, "public.another_test_data", Arrays.asList("id", 1, "name", "dirt"));

            List<SourceRecord> records = consumer.get(2);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testResnapshotPartial() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');",
                "insert into another_test_data (id, name) VALUES (1, 'dirt');");
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));
            addExpectedRecord(expectedRecords, "public.another_test_data", Arrays.asList("id", 1, "name", "dirt"));

            List<SourceRecord> records = consumer.get(2);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }

        TestUtils.execute(postgreSQLContainer,
                "update snapshot_tracker set needs_snapshot=true where collection_name like 'public.test_data';");
        // Restart the connector
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));

            List<SourceRecord> records = consumer.get(1);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testCompletedSnapshotUnlocksInTracker() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');");
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            runSnapshot(engine, new ChangeConsumer());
        }

        try (PostgresConnection postgresConnection = TestUtils.createConnection(postgreSQLContainer);
             Statement statement = postgresConnection.connection().createStatement();
             ResultSet rs = statement.executeQuery("select needs_snapshot, under_snapshot from snapshot_tracker;")) {
            while (rs.next()) {
                assertFalse(rs.getBoolean("needs_snapshot"));
                assertFalse(rs.getBoolean("under_snapshot"));
            }
        }
    }

    @Test
    public void testSnapshotEmptyDB() throws Exception {
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testCustomSnapshotTrackerTableName() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');",
                "CREATE SCHEMA snapshot;");
        Map<String, Object> snapshotTrackerConfigs = new HashMap<>();
        snapshotTrackerConfigs.put("snapshot.partial.table.name", "snapshot.custom_tracker");
        snapshotTrackerConfigs.put("snapshot.partial.pk.name", "pk_a_different_key_name");
        Configuration.Builder builder = TestPostgresConnectorConfig.customConfig(postgreSQLContainer, snapshotTrackerConfigs);

        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));

            List<SourceRecord> records = consumer.get(1);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }

        try (PostgresConnection postgresConnection = TestUtils.createConnection(postgreSQLContainer);
             Statement statement = postgresConnection.connection().createStatement();
             ResultSet rs = statement.executeQuery("select collection_name from snapshot.custom_tracker;")) {
            while (rs.next()) {
                assertThat(rs.getString("collection_name"),
                        AnyOf.anyOf(
                                StringContains.containsString("public.test_data"),
                                StringContains.containsString("public.another_test_data")
                        )
                );
            }
        }
    }

    @Test
    public void testMultipleConnectorsSnapshot() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');");

        // Use custom configs to prevent conflicts with the running engine
        Map<String, Object> secondEngineConfigs = new HashMap<>();
        secondEngineConfigs.put(PostgresConnectorConfig.SLOT_NAME.name(), "second_debezium");
        secondEngineConfigs.put(RelationalDatabaseConnectorConfig.SERVER_NAME.name(), "second_server");
        secondEngineConfigs.put(EmbeddedEngine.ENGINE_NAME.name(), "another-test-connector");
        secondEngineConfigs.put(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG,
                Paths.get(AbstractTestEmbeddedEngine.DATA_DIR, "second-file-connector-offsets.txt").toAbsolutePath());
        secondEngineConfigs.put(FileDatabaseHistory.FILE_PATH.name(),
                Paths.get(AbstractTestEmbeddedEngine.DATA_DIR, "second-file-db-history.txt").toAbsolutePath());
        Configuration.Builder secondEngineConfigBuilder = TestPostgresConnectorConfig.customConfig(postgreSQLContainer, secondEngineConfigs);

        try (TestPostgresEmbeddedEngine firstEngine = new TestPostgresEmbeddedEngine(postgreSQLContainer)) {
            ChangeConsumer firstConsumer = new ChangeConsumer();
            ChangeConsumer secondConsumer = new ChangeConsumer();

            runSnapshot(firstEngine, firstConsumer);

            try (TestPostgresEmbeddedEngine secondEngine = new TestPostgresEmbeddedEngine(secondEngineConfigBuilder)) {
                secondEngine.start(secondConsumer, false);
                TestUtils.waitForSnapshotToBeCompleted("postgres", (String) secondEngineConfigs.get("database.server.name"));
            }

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));
            List<SourceRecord> records = firstConsumer.get(1);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(firstConsumer.isEmptyForSnapshot());

            Map<String, Map<String, Object>> secondExpectedRecords = new HashMap<>();
            addExpectedRecord(secondExpectedRecords, "public.test_data",
                    Arrays.asList("id", 1, "name", "joe"), "second_server");
            List<SourceRecord> secondConnectorRecords = secondConsumer.get(1);
            verifySnapshotRecordValues(secondExpectedRecords, secondConnectorRecords);
            assertTrue(secondConsumer.isEmptyForSnapshot());
        }
    }

    private void runSnapshot(TestPostgresEmbeddedEngine engine, ChangeConsumer consumer) throws InterruptedException {
        engine.start(consumer);
        waitForSnapshotToBeCompleted();
    }

    private void waitForSnapshotToBeCompleted() throws InterruptedException {
        TestUtils.waitForSnapshotToBeCompleted("postgres", TestPostgresConnectorConfig.TEST_SERVER);
    }

    private void verifySnapshotRecordValues(Map<String, Map<String, Object>> expectedRecords, List<SourceRecord> records) {
        for (SourceRecord record : records) {
            Map<String, Object> rowData = expectedRecords.remove(record.topic());
            assertNotNull(rowData);
            for (Map.Entry<String, Object> column : rowData.entrySet()) {
                assertEquals(column.getValue(), ((Struct) record.value()).getStruct("after").get(column.getKey()));
                assertThat(((Struct) record.value()).getStruct("source").getString("snapshot"),
                        AnyOf.anyOf(
                                StringContains.containsString("true"),
                                StringContains.containsString("last")
                        )
                );
            }
        }
        assertEquals(0, expectedRecords.size());
    }

    private void addExpectedRecord(Map<String, Map<String, Object>> records, String dbObjectName, List<Object> data) {
        addExpectedRecord(records, dbObjectName, data, TestPostgresConnectorConfig.TEST_SERVER);
    }

    private void addExpectedRecord(Map<String, Map<String, Object>> records, String dbObjectName, List<Object> data, String serverName) {
        assertEquals(0, data.size() % 2);
        Collection<List<Object>> chunked = splitIntoChunks(data);

        Map<String, Object> record = new HashMap<>();

        for (List<Object> column : chunked) {
            String key = (String) column.get(0);
            Object value = column.get(1);
            record.put(key, value);
        }

        records.put(TestUtils.topicName(serverName, dbObjectName), record);
    }

    private Collection<List<Object>> splitIntoChunks(List<Object> data) {
        final AtomicInteger counter = new AtomicInteger();
        int chunkSize = 2;

        // HACK: group by works using integer division
        return data.stream()
                .collect(Collectors.groupingBy(it -> counter.getAndIncrement() / chunkSize))
                .values();
    }
}
