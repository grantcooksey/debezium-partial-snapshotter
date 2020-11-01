package io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message;

import io.debezium.connector.postgresql.snapshot.partial.filters.FilterHandler;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;

public class ShouldSnapshotFilterMessage implements SnapshotFilterMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(ShouldSnapshotFilterMessage.class);

    private final TableId tableId;
    private final ArrayBlockingQueue<Boolean> responseQueue;
    private final FilterHandler filterHandler;

    public ShouldSnapshotFilterMessage(TableId tableId, ArrayBlockingQueue<Boolean> responseQueue, FilterHandler filterHandler) {
        this.tableId = tableId;
        this.responseQueue = responseQueue;
        this.filterHandler = filterHandler;
    }

    @Override
    public MessageResponse handle() {
        boolean response = filterHandler.shouldSnapshot(tableId);
        try {
            responseQueue.put(response);
        } catch (InterruptedException e) {
            LOGGER.error("Partial snapshotter response timed out.", e);
        }

        return new MessageResponse(true);
    }
}
