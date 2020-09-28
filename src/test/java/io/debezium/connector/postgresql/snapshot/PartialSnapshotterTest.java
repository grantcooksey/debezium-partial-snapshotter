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
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(PartialSnapshotterTest.class);

    private static final String CLEAN_UP_SCHEMA = "DROP SCHEMA IF EXISTS public CASCADE;" +
            "DROP SCHEMA IF EXISTS snapshot CASCADE;" +
            "CREATE SCHEMA public;";
    private static final String DROP_ALL_REPLICATION_SLOTS = "select pg_drop_replication_slot(slot_name) " +
            "from pg_replication_slots;";
    private static final String CREATE_TEST_DATA_TABLES =
            "create table test_data (id integer not null constraint table_name_pk primary key, name text);" +
            "create table another_test_data (id integer not null constraint another_table_name_pk primary key, name text);";
    private static final String CREATE_SNAPSHOT_TABLE = "create table public.snapshot_tracker (" +
            "table_name text not null, " +
            "server_name text not null, " +
            "needs_snapshot boolean not null, " +
            "under_snapshot boolean not null, " +
            "constraint snapshot_tracker_pk primary key (table_name, server_name));";

    @Before
    public void before() {
        TestUtils.execute(postgreSQLContainer, CLEAN_UP_SCHEMA);
    }

    @After
    public void after() {
        TestUtils.execute(postgreSQLContainer, CLEAN_UP_SCHEMA, DROP_ALL_REPLICATION_SLOTS);
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
                "insert into snapshot_tracker (table_name, server_name, needs_snapshot, under_snapshot) values " +
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
                "insert into snapshot_tracker (table_name, server_name, needs_snapshot, under_snapshot) values " +
                        "('public.test_data', '" + TestPostgresConnectorConfig.TEST_SERVER + "', false, false);",
                "insert into snapshot_tracker (table_name, server_name, needs_snapshot, under_snapshot) values " +
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
        Map<String, Object> configs = new HashMap<>();
        configs.put("slot.drop.on.stop", "false");
        Configuration.Builder builder = TestPostgresConnectorConfig.customConfig(postgreSQLContainer, configs);
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));
            addExpectedRecord(expectedRecords, "public.another_test_data", Arrays.asList("id", 1, "name", "dirt"));

            List<SourceRecord> records = consumer.get(2);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());

            waitForStreamingToStart();
        }

        TestUtils.execute(postgreSQLContainer,
                "update snapshot_tracker set needs_snapshot=true where table_name like 'public.test_data';");
        // Restart the connector
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            engine.start(consumer, false);
            waitForSnapshotToBeCompleted();

            Map<String, Map<String, Object>> expectedRecords = new HashMap<>();
            addExpectedRecord(expectedRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));

            List<SourceRecord> records = consumer.get(1);
            verifySnapshotRecordValues(expectedRecords, records);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testReplayRecordsDuringResnapshot() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');",
                "insert into another_test_data (id, name) VALUES (1, 'dirt');");
        Map<String, Object> configs = new HashMap<>();
        configs.put("slot.drop.on.stop", "false");
        Configuration.Builder builder = TestPostgresConnectorConfig.customConfig(postgreSQLContainer, configs);

        // Take initial snapshot
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            // insert a streaming record
            TestUtils.execute(postgreSQLContainer,"insert into another_test_data (id, name) VALUES (2, 'cat');");

            Map<String, Map<String, Object>> expectedSnapshotRecords = new HashMap<>();
            addExpectedRecord(expectedSnapshotRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));
            addExpectedRecord(expectedSnapshotRecords, "public.another_test_data", Arrays.asList("id", 1, "name", "dirt"));
            List<SourceRecord> snapshotRecords = consumer.get(2);
            verifySnapshotRecordValues(expectedSnapshotRecords, snapshotRecords);

            Map<String, Map<String, Object>> expectedStreamingRecords = new HashMap<>();
            addExpectedRecord(expectedStreamingRecords, "public.another_test_data", Arrays.asList("id", 2, "name", "cat"));
            List<SourceRecord> streamingRecords = consumer.get(1);
            verifyStreamingRecordValues(expectedStreamingRecords, streamingRecords);
        }

        TestUtils.execute(postgreSQLContainer,
                "update snapshot_tracker set needs_snapshot=true where table_name like 'public.test_data';",
                "insert into another_test_data (id, name) VALUES (3, 'dog');");
        // Restart the connector
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            engine.start(consumer, false);
            waitForSnapshotToBeCompleted();

            // Since we are using an non-temp replication slot, catch up streaming is performed
            Map<String, Map<String, Object>> expectedStreamingRecords = new HashMap<>();
            addExpectedRecord(expectedStreamingRecords, "public.another_test_data", Arrays.asList("id", 3, "name", "dog"));

            List<SourceRecord> streamingRecords = consumer.get(1);
            verifyStreamingRecordValues(expectedStreamingRecords, streamingRecords);

            Map<String, Map<String, Object>> expectedSnapshotRecords = new HashMap<>();
            addExpectedRecord(expectedSnapshotRecords, "public.test_data", Arrays.asList("id", 1, "name", "joe"));

            List<SourceRecord> snapshotRecords = consumer.get(1);
            verifySnapshotRecordValues(expectedSnapshotRecords, snapshotRecords);

            waitForStreamingToStart();

            assertTrue(consumer.isEmpty());
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
             ResultSet rs = statement.executeQuery("select table_name from snapshot.custom_tracker;")) {
            while (rs.next()) {
                assertThat(rs.getString("table_name"),
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

    @Test
    public void testSkippedSnapshotExistingConnectorRestart() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');");
        Map<String, Object> skipSnapshotConfigs = new HashMap<>();
        skipSnapshotConfigs.put("snapshot.partial.skip.existing.connector", "true");
        skipSnapshotConfigs.put("slot.drop.on.stop", "false");
        Configuration.Builder builder = TestPostgresConnectorConfig.customConfig(postgreSQLContainer, skipSnapshotConfigs);

        // Skip the snapshot for existing connector that is adding the partial snapshot plugin
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);
            assertTrue(consumer.isEmptyForSnapshot());
        }

        // Normal operation, snapshot records should exist in the table
        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);
            assertTrue(consumer.isEmptyForSnapshot());
        }
    }

    @Test
    public void testSkipSnapshotForExistingConnector() throws Exception {
        TestUtils.execute(postgreSQLContainer, CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');");
        Map<String, Object> skipSnapshotConfigs = new HashMap<>();
        skipSnapshotConfigs.put("snapshot.partial.skip.existing.connector", "true");
        Configuration.Builder builder = TestPostgresConnectorConfig.customConfig(postgreSQLContainer, skipSnapshotConfigs);

        try (TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine(builder)) {
            ChangeConsumer consumer = new ChangeConsumer();
            runSnapshot(engine, consumer);

            assertTrue(consumer.isEmptyForSnapshot());
        }

        try (PostgresConnection postgresConnection = TestUtils.createConnection(postgreSQLContainer);
             Statement statement = postgresConnection.connection().createStatement();
             ResultSet rs = statement.executeQuery("select table_name from snapshot_tracker;")) {
            while (rs.next()) {
                assertThat(rs.getString("table_name"),
                        AnyOf.anyOf(
                                StringContains.containsString("public.test_data"),
                                StringContains.containsString("public.another_test_data")
                        )
                );
            }
        }
    }

    private void runSnapshot(TestPostgresEmbeddedEngine engine, ChangeConsumer consumer) throws InterruptedException {
        engine.start(consumer);
        waitForSnapshotToBeCompleted();
    }

    private void waitForSnapshotToBeCompleted() throws InterruptedException {
        TestUtils.waitForSnapshotToBeCompleted("postgres", TestPostgresConnectorConfig.TEST_SERVER);
    }

    private void waitForStreamingToStart() throws InterruptedException {
        TestUtils.waitForStreamingToStart("postgres", TestPostgresConnectorConfig.TEST_SERVER);
    }

    private void verifySnapshotRecordValues(Map<String, Map<String, Object>> expectedRecords, List<SourceRecord> records) {
        verifyRecordValues(expectedRecords, records, true);
    }

    private void verifyStreamingRecordValues(Map<String, Map<String, Object>> expectedRecords, List<SourceRecord> records) {
        verifyRecordValues(expectedRecords, records, false);
    }

    private void verifyRecordValues(Map<String, Map<String, Object>> expectedRecords, List<SourceRecord> records, boolean isSnapshot) {
        for (SourceRecord record : records) {
            LOGGER.info("Verifying discovered record {}", record);
            Map<String, Object> rowData = expectedRecords.remove(record.topic());
            assertNotNull(rowData);
            for (Map.Entry<String, Object> column : rowData.entrySet()) {
                assertEquals(column.getValue(), ((Struct) record.value()).getStruct("after").get(column.getKey()));
                if (isSnapshot) {
                    assertThat(((Struct) record.value()).getStruct("source").getString("snapshot"),
                            AnyOf.anyOf(
                                    StringContains.containsString("true"),
                                    StringContains.containsString("last")
                            )
                    );
                }
                else {
                    assertThat(
                            (String) ((Struct) record.value()).getStruct("source").get("snapshot"),
                            AnyOf.anyOf(StringContains.containsString("false"))
                    );
                }
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
