package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.snapshot.partial.FilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.PartialSnapshotConfig;
import io.debezium.connector.postgresql.snapshot.partial.PostgresJdbcFilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.SnapshotFilter;
import io.debezium.connector.postgresql.snapshot.partial.VersionHelper;
import io.debezium.connector.postgresql.spi.OffsetState;
import io.debezium.connector.postgresql.spi.SlotState;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Optional;

public class PartialSnapshotter extends ExportedSnapshotter {

    private static final Logger LOGGER = LoggerFactory.getLogger(PartialSnapshotter.class);

    private SnapshotFilter filter;

    @Override
    public void init(PostgresConnectorConfig config, OffsetState sourceInfo, SlotState slotState) {
        if (VersionHelper.isCurrentVersionCompatibleWithPlugin()) {
            PartialSnapshotConfig partialSnapshotConfig = new PartialSnapshotConfig(config.getConfig());
            FilterHandler handler = new PostgresJdbcFilterHandler(config, partialSnapshotConfig);
            this.filter = new SnapshotFilter(handler, config);
        } else {
            LOGGER.warn("Current debezium version is not compatible with the partial snapshotter plugin. The " +
                "version must be 1.3.0-Beta2 or greater. Reverting to use the default 'initial' snapshot");
        }
        super.init(config, sourceInfo, slotState);
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId) {
        if (VersionHelper.isCurrentVersionCompatibleWithPlugin()) {
            if (filter.shouldSnapshotTable(tableId)) {
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
}
