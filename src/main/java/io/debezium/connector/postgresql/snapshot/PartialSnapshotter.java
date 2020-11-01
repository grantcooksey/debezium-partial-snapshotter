package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.snapshot.partial.filters.FilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.PartialSnapshotConfig;
import io.debezium.connector.postgresql.snapshot.partial.filters.handlers.PostgresJdbcFilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.filters.handlers.ThreadedSnapshotFilter;
import io.debezium.connector.postgresql.snapshot.partial.VersionHelper;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class PartialSnapshotter extends ExportedSnapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartialSnapshotter.class);

    private FilterHandler filter;
    private int shouldStreamCallCount;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        if (VersionHelper.isCurrentVersionCompatibleWithPlugin()) {
            PartialSnapshotConfig partialSnapshotConfig = new PartialSnapshotConfig(config.getConfig());
            FilterHandler handler = new PostgresJdbcFilterHandler(config, partialSnapshotConfig);
            this.filter = new ThreadedSnapshotFilter(handler);
        } else {
            LOGGER.warn("Current debezium version is not compatible with the partial snapshotter plugin. The " +
                "version must be " + VersionHelper.MIN_VERSION + " or greater. Reverting to use the default 'initial' snapshot");
        }

        shouldStreamCallCount = 0;

        super.init(config, sourceInfo, slotState);
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId) {
        if (VersionHelper.isCurrentVersionCompatibleWithPlugin()) {
            if (filter.shouldSnapshot(tableId)) {
                LOGGER.info("Data collection {} will have a snapshot performed", tableId);
                return super.buildSnapshotQuery(tableId);
            }

            LOGGER.info("Skipping snapshot for data collection {}", tableId);
            return Optional.empty();
        }
        return super.buildSnapshotQuery(tableId);
    }

    @Override
    public boolean shouldSnapshot() {
        if (VersionHelper.isCurrentVersionCompatibleWithPlugin()) {
            LOGGER.info("Performing snapshot. Partial snapshotter will always attempt a snapshot.");
            return true;
        }
        return super.shouldSnapshot();
    }

    @Override
    public boolean shouldStreamEventsStartingFromSnapshot() {
        if (VersionHelper.isCurrentVersionCompatibleWithPlugin()) {
            return false;
        }
        return super.shouldStreamEventsStartingFromSnapshot();
    }

    @Override
    public boolean shouldStream() {
        // HACK: this function is called during streaming setup and initialization. Tracking call counts here is a
        // workaround till we can add a proper close handler to the snapshotter.
        // In the case where this is first time a connector is running and needs to
        // take an initial snapshot, catch up streaming is skipped and the call count is reduced.
        if ((super.shouldSnapshot() && shouldStreamCallCount >= 1) || shouldStreamCallCount >= 2) {
            close();
        }
        shouldStreamCallCount += 1;
        return super.shouldStream();
    }

    private void close() {
        filter.close();
    }
}
