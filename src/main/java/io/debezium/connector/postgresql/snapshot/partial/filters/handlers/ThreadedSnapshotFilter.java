package io.debezium.connector.postgresql.snapshot.partial.filters.handlers;

import io.debezium.connector.postgresql.snapshot.partial.filters.FilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.filters.threaded.SnapshotFilterManager;
import io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message.PoisonPillMessage;
import io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message.ShouldSnapshotFilterMessage;
import io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message.SnapshotFilterMessage;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class ThreadedSnapshotFilter implements FilterHandler {

    private static final Logger LOGGER = LoggerFactory.getLogger(ThreadedSnapshotFilter.class);

    private static final int MAX_RESPONSE_POLLING_ATTEMPTS = 30;
    private static final int ONE_SECOND = 1;

    private final LinkedBlockingQueue<SnapshotFilterMessage> requestQueue;
    private final FilterHandler filterHandler;

    public ThreadedSnapshotFilter(FilterHandler filterHandler) {
        this.requestQueue = new LinkedBlockingQueue<>();
        this.filterHandler = filterHandler;

        Thread queryWorker = new Thread(new SnapshotFilterManager(requestQueue));

        LOGGER.debug("Starting snapshot filter manager thread");
        queryWorker.start();
    }

    @Override
    public boolean shouldSnapshot(TableId tableId) {
        ArrayBlockingQueue<Boolean> responseQueue = new ArrayBlockingQueue<>(1);
        SnapshotFilterMessage message = new ShouldSnapshotFilterMessage(tableId, responseQueue, filterHandler);

        try {
            requestQueue.put(message);
            for (int i = 0; i < MAX_RESPONSE_POLLING_ATTEMPTS; i++) {
                Boolean response = responseQueue.poll(ONE_SECOND, TimeUnit.SECONDS);
                if (response != null) {
                    return response;
                }
                LOGGER.info("Response from snapshot filter manager thread timed out for {}. Retrying", tableId);
            }

            LOGGER.warn("Failed to get response whether to snapshot or not for {}", tableId);
        }
        catch (InterruptedException e) {
            LOGGER.error("Interrupted while communicating with the snapshot filter manager for {}", tableId, e);
        }

        LOGGER.warn("Failed to determine whether to not snapshot {}. Performing snapshot by default.", tableId);
        return true;
    }

    @Override
    public void close() {
        LOGGER.info("Sending a request to close the snapshot filter.");
        try {
            SnapshotFilterMessage poisonPill = new PoisonPillMessage(filterHandler);
            requestQueue.put(poisonPill);
        }
        catch (InterruptedException e) {
            LOGGER.error("Interrupted while closing snapshot filter. External resources may not have closed.", e);
        }

    }
}
