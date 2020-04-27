package io.debezium.connector.postgresql.snapshot;

import io.debezium.config.Configuration;
import io.debezium.connector.postgresql.PostgresConnector;
import io.debezium.connector.postgresql.TestPostgresConnectorConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.function.Function;

public class TestPostgresEmbeddedEngine extends AbstractTestEmbeddedEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(TestPostgresEmbeddedEngine.class);

    public Configuration config;

    public TestPostgresEmbeddedEngine() {
        this.config = new TestPostgresConnectorConfig(TestPostgresConnectorConfig.defaultConfig().build()).getConfig();
    }

    public TestPostgresEmbeddedEngine(Function<Configuration.Builder, Configuration.Builder> customConfig) {
        Configuration.Builder builder = customConfig.apply(TestPostgresConnectorConfig.defaultConfig());
        this.config = new TestPostgresConnectorConfig(builder.build()).getConfig();
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
