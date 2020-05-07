package io.debezium.connector.postgresql.snapshot;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.TestPostgresConnectorConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.PostgreSQLContainer;

public class TestPostgresEmbeddedEngine extends AbstractTestEmbeddedEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestPostgresEmbeddedEngine.class);

    public Configuration config;

    public TestPostgresEmbeddedEngine(PostgreSQLContainer postgreSQLContainer) {
        this.config = new TestPostgresConnectorConfig(TestPostgresConnectorConfig.defaultConfig(postgreSQLContainer).build()).getConfig();
    }

    public TestPostgresEmbeddedEngine(Configuration.Builder configBuilder) {
        this.config = new TestPostgresConnectorConfig(configBuilder.build()).getConfig();
    }

    @Override
    public Class<? extends SourceConnector> getConnectorClass() {
        return PostgresConnector.class;
    }

    @Override
    public Configuration getConfiguration() {
        return config;
    }
}
