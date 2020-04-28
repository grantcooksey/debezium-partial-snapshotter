package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.TestPostgresConnectorConfig;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class PartialSnapshotterTest {

    private static final String CLEAN_UP_SCHEMA = "DROP SCHEMA IF EXISTS public CASCADE;" +
        "CREATE SCHEMA public;";
    private static final String CREATE_TEST_DATA_TABLES =
            "create table test_data (id integer not null constraint table_name_pk primary key, name text);" +
            "create table another_test_data (id integer not null constraint another_table_name_pk primary key, name text);";

    @Before
    public void before() {
        TestUtils.execute(CLEAN_UP_SCHEMA);
    }

    @After
    public void after() {
        TestUtils.execute(CLEAN_UP_SCHEMA);
    }

    @Test
    public void testEngine() throws InterruptedException {
        TestUtils.execute(CREATE_TEST_DATA_TABLES, "insert into test_data (id, name) VALUES (1, 'joe');");
        TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine();
        ChangeConsumer consumer = new ChangeConsumer();
        engine.start(consumer);
        waitForSnapshotToBeCompleted();

        SourceRecord record = consumer.pollDataTopic();
        assertNotNull(record);
        assertEquals(record.topic(), TestPostgresConnectorConfig.TEST_SERVER + ".public.test_data");
        assertEquals(((Struct) record.value()).getStruct("after").get("id"), 1);
        assertEquals(((Struct) record.value()).getStruct("after").get("name"), "joe");

        engine.stop();
    }

    @Test
    public void testFilterOneTablePartialSnapshot() throws InterruptedException {
        TestUtils.execute(CREATE_TEST_DATA_TABLES,
                "insert into test_data (id, name) VALUES (1, 'joe');");
        TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine();
        ChangeConsumer consumer = new ChangeConsumer();
        runSnapshot(engine, consumer);

        engine.stop();
    }

    @Test
    public void testFilterAllTablesPartialSnapshot() throws InterruptedException {
        TestUtils.execute(CREATE_TEST_DATA_TABLES, "insert into test_data (id, name) VALUES (1, 'joe');");
        TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine();
        ChangeConsumer consumer = new ChangeConsumer();
        runSnapshot(engine, consumer);

        engine.stop();
    }

    @Test
    public void testFilterNoTablesPartialSnapshot() throws InterruptedException {
        TestUtils.execute(CREATE_TEST_DATA_TABLES, "insert into test_data (id, name) VALUES (1, 'joe');");
        TestPostgresEmbeddedEngine engine = new TestPostgresEmbeddedEngine();
        ChangeConsumer consumer = new ChangeConsumer();
        runSnapshot(engine, consumer);

        engine.stop();
    }

    private void runSnapshot(TestPostgresEmbeddedEngine engine, ChangeConsumer consumer) throws InterruptedException {
        engine.start(consumer);
        waitForSnapshotToBeCompleted();
    }

    private void waitForStreamingRunning() throws InterruptedException {
        TestUtils.waitForStreamingRunning("postgres", TestPostgresConnectorConfig.TEST_SERVER);
    }

    private void waitForSnapshotToBeCompleted() throws InterruptedException {
        TestUtils.waitForSnapshotToBeCompleted("postgres", TestPostgresConnectorConfig.TEST_SERVER);
    }
}
