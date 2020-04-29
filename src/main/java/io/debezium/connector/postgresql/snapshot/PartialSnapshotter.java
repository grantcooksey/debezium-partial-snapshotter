package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.snapshot.partial.FilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.PostgresJdbcFilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.SnapshotFilter;
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
        FilterHandler handler = new PostgresJdbcFilterHandler(config);
        this.filter = new SnapshotFilter(handler, config);
    }

    @Override
    public Optional<String> buildSnapshotQuery(TableId tableId) {
        if (filter.shouldSnapshotTable(tableId)) {
            LOGGER.info("Data collection {} will have a snapshot performed", tableId);
            return super.buildSnapshotQuery(tableId);
        }

        LOGGER.info("Skipping snapshot for data collection {}", tableId);
        return Optional.empty();
    }
}
