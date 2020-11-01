package io.debezium.connector.postgresql.snapshot.partial.filters.threaded;

import io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message.MessageResponse;
import io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message.SnapshotFilterMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SnapshotFilterManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotFilterManager.class);

    private static final int ONE_SECOND = 1;

    private final LinkedBlockingQueue<SnapshotFilterMessage> requestQueue;

    public SnapshotFilterManager(LinkedBlockingQueue<SnapshotFilterMessage> requestQueue) {
        this.requestQueue = requestQueue;
    }

    @Override
    public void run() {
        MessageResponse response = null;
        while (response == null || response.isFilterActive()) {
            SnapshotFilterMessage message = pollForRequest();
            if (message != null) {
                response = message.handle();
            }
        }

        LOGGER.info("Shutting down snapshot filter thread");
    }

    private SnapshotFilterMessage pollForRequest() {
        try {
            return requestQueue.poll(ONE_SECOND, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.error("Request polling shouldn't have been interrupted", e);
        }
        return null;
    }
}
