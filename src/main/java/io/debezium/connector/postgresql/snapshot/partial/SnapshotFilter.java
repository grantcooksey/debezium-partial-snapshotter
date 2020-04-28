package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.config.CommonConnectorConfig;
import io.debezium.relational.TableId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SnapshotFilter {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotFilter.class);

    private static final int MAX_RESPONSE_POLLING_ATTEMPTS = 30;
    private static final int ONE_SECOND = 1;

    private final LinkedBlockingQueue<SnapshotFilterMessage> requestQueue;

    public SnapshotFilter(FilterHandler handler, CommonConnectorConfig config) {
        requestQueue = new LinkedBlockingQueue<>();

        Thread queryWorker = new Thread(new SnapshotFilterManager(requestQueue, handler, config));

        LOGGER.debug("Starting snapshot filter manager thread");
        queryWorker.start();
    }

    public boolean shouldSnapshotTable(TableId tableId) {
        ArrayBlockingQueue<Boolean> responseQueue = new ArrayBlockingQueue<>(1);
        SnapshotFilterMessage message = new SnapshotFilterMessage(tableId, responseQueue);

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
}
