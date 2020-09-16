package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.TestPostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.time.Duration;

public class TestUtils {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestUtils.class);

    public static final int MAX_TEST_DURATION_SEC = 60;

    public static PostgresConnection createConnection(PostgreSQLContainer postgreSQLContainer) {
        return new PostgresConnection(TestPostgresConnectorConfig.defaultJdbcConfig(postgreSQLContainer));
    }

    public static void execute(PostgreSQLContainer postgreSQLContainer, String statement, String... furtherStatements) {
        if (furtherStatements != null) {
            StringBuilder statementBuilder = new StringBuilder(statement);
            for (String further : furtherStatements) {
                statementBuilder.append(further);
            }
            statement = statementBuilder.toString();
        }

        try (PostgresConnection connection = createConnection(postgreSQLContainer)) {
            connection.setAutoCommit(false);
            connection.executeWithoutCommitting(statement);
            Connection jdbcConn = connection.connection();
            if (!statement.endsWith("ROLLBACK;")) {
                jdbcConn.commit();
            }
            else {
                jdbcConn.rollback();
            }
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void waitForJMXToDeregister(String connector, String server) throws InterruptedException {
        ObjectName mbean = getJMXSnapshotObjectName(connector, server, "snapshot");
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        final Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.system());

        while (mbeanServer.isRegistered(mbean)) {
            LOGGER.warn("Metrics not yet de-registered, waiting...");
            metronome.pause();
        }
    }

    public static void waitForSnapshotToBeCompleted(String connector, String server) throws InterruptedException {
        ObjectName mbean = getJMXSnapshotObjectName(connector, server, "snapshot");
        pollForJmxMetric(mbean, "SnapshotCompleted");
    }

    public static void waitForStreamingToStart(String connector, String server) throws InterruptedException {
        ObjectName mbean = getJMXSnapshotObjectName(connector, server, "streaming");
        pollForJmxMetric(mbean, "Connected");
    }

    public static void pollForJmxMetric(ObjectName mbeanName, String attribute) throws InterruptedException {
        int waitForSeconds = MAX_TEST_DURATION_SEC;
        final MBeanServer mbeanServer = ManagementFactory.getPlatformMBeanServer();
        final Metronome metronome = Metronome.sleeper(Duration.ofSeconds(1), Clock.system());

        while (true) {
            if (waitForSeconds-- <= 0) {
                Assert.fail("JMX metrics poll not completed on time");
            }
            try {
                final boolean completed = (boolean) mbeanServer
                        .getAttribute(mbeanName, attribute);
                if (completed) {
                    break;
                }
            }
            catch (InstanceNotFoundException e) {
                LOGGER.warn("Metrics not yet started");
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            metronome.pause();
        }
    }

    public static String topicName(String serverName, String dbObjectName) {
        return serverName + "." + dbObjectName;
    }

    private static ObjectName getJMXSnapshotObjectName(String connector, String server, String context) {
        ObjectName mbean;
        try {
            mbean = new ObjectName("debezium." + connector + ":type=connector-metrics,context=" + context + ",server=" + server);
        }
        catch (MalformedObjectNameException e) {
            throw new IllegalStateException(e);
        }
        return mbean;
    }

}
