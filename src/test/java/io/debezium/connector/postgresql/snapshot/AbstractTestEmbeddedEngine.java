package io.debezium.connector.postgresql.snapshot;

import io.debezium.config.Configuration;
import io.debezium.embedded.Connect;
import io.debezium.embedded.EmbeddedEngine;
import io.debezium.embedded.StopConnectorException;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.StopEngineException;
import io.debezium.relational.HistorizedRelationalDatabaseConnectorConfig;
import io.debezium.relational.history.FileDatabaseHistory;
import org.apache.kafka.connect.runtime.standalone.StandaloneConfig;
import org.apache.kafka.connect.source.SourceConnector;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

abstract class AbstractTestEmbeddedEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractTestEmbeddedEngine.class);

    private static final String DATA_DIR = "build/data";
    private static final String DATA_OFFSET_FILE = "file-connector-offsets.txt";
    private static final String DATA_DB_HISTORY_FILE = "file-db-history.txt";

    private DebeziumEngine<SourceRecord> engine;
    private ExecutorService executor;

    public abstract Class<? extends SourceConnector> getConnectorClass();

    public abstract Configuration getConfiguration();

    public void start(DebeziumEngine.ChangeConsumer<SourceRecord> changeConsumer) {
        LOGGER.info("Starting debezium engine");
        try {
            init(getConnectorClass(), getConfiguration(), changeConsumer);
        }
        catch (IOException e) {
            LOGGER.error("Failed to start the engine", e);
        }
    }

    public void stop() {
        LOGGER.info("Stopping the connector");
        // Try to stop the connector ...
        if (engine != null) {
            try {
                engine.close();
            }
            catch (IOException e) {
                LOGGER.warn("Engine failed to close");
                Thread.currentThread().interrupt();
            }
        }
        executor.shutdownNow();

        engine = null;
        executor = null;

        LOGGER.info("Engine has been stopped and removed");
    }

    public void init(Class<? extends SourceConnector> connectorClass, Configuration connectorConfig,
                     DebeziumEngine.ChangeConsumer<SourceRecord> changeConsumer) throws IOException {
        resetLocalStorage();

        Configuration config = Configuration.copy(connectorConfig)
                .with(EmbeddedEngine.ENGINE_NAME, "test-connector")
                .with(EmbeddedEngine.CONNECTOR_CLASS, connectorClass.getName())
                .with(StandaloneConfig.OFFSET_STORAGE_FILE_FILENAME_CONFIG, getStorageFile(DATA_OFFSET_FILE))
                .with(EmbeddedEngine.OFFSET_FLUSH_INTERVAL_MS, 0)
                .with(HistorizedRelationalDatabaseConnectorConfig.DATABASE_HISTORY, FileDatabaseHistory.class.getName())
                .with(FileDatabaseHistory.FILE_PATH, getStorageFile(DATA_DB_HISTORY_FILE))
                .build();

        final Properties connectorProps = config.asProperties();

        // Create the engine with this configuration ...
        this.engine = DebeziumEngine.create(Connect.class)
                .using(connectorProps)
                .notifying(changeConsumer)
                .build();

        // Run the engine asynchronously ...
        this.executor = Executors.newSingleThreadExecutor();
        executor.execute(engine);
    }

    private Path getStorageFile(String file) {
        return Paths.get(DATA_DIR, file).toAbsolutePath();
    }

    private void resetLocalStorage() throws IOException {
        Path storageSource = Paths.get(DATA_DIR);
        if (Files.isDirectory(storageSource)) {
            LOGGER.info("Found existing storage for the engine. Deleting all files");
            try (Stream<Path> files = Files.walk(storageSource)) {
                files.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
            }
        }
        LOGGER.info("Creating new storage directory for the engine");
        Files.createDirectory(storageSource);
    }




}
