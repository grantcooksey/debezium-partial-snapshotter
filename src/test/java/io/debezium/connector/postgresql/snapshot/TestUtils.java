package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.TestPostgresConnectorConfig;
import io.debezium.connector.postgresql.connection.PostgresConnection;
import io.debezium.util.Clock;
import io.debezium.util.Metronome;
import org.junit.Assert;
import org.testcontainers.containers.PostgreSQLContainer;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;
import java.sql.Connection;
import java.time.Duration;

public class TestUtils {

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

    public static void waitForStreamingRunning(String connector, String server) throws InterruptedException {
        ObjectName mBean;
        try {
            mBean = new ObjectName("debezium." + connector + ":type=connector-metrics,context=streaming,server=" + server);
        }
        catch (MalformedObjectNameException e) {
            throw new IllegalStateException(e);
        }
        pollForJmxMetric(mBean, "Connected");
    }

    public static void waitForSnapshotToBeCompleted(String connector, String server) throws InterruptedException {
        ObjectName mbean;
        try {
            mbean = new ObjectName("debezium." + connector + ":type=connector-metrics,context=snapshot,server=" + server);
        }
        catch (MalformedObjectNameException e) {
            throw new IllegalStateException(e);
        }
        pollForJmxMetric(mbean, "SnapshotCompleted");
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
                System.out.println("Metrics not yet started");
            }
            catch (Exception e) {
                throw new IllegalStateException(e);
            }
            metronome.pause();
        }
    }

    public static String topicName(String dbObjectName) {
        return TestPostgresConnectorConfig.TEST_SERVER + "." + dbObjectName;
    }

}
