package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.config.CommonConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SnapshotFilterManager implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotFilterManager.class);

    private static final int ONE_SECOND = 1;

    private final LinkedBlockingQueue<SnapshotFilterMessage> requestQueue;
    private final FilterHandler filterHandler;
    private final SnapshotLifetimeMonitor snapshotLifetimeMonitor;

    public SnapshotFilterManager(LinkedBlockingQueue<SnapshotFilterMessage> requestQueue, FilterHandler filterHandler,
                                 CommonConnectorConfig config) {
        this.requestQueue = requestQueue;
        this.filterHandler = filterHandler;

        snapshotLifetimeMonitor = new SnapshotLifetimeMonitor(config);
    }

    @Override
    public void run() {
        snapshotLifetimeMonitor.waitForSnapshotToStart();
        try {
            while (isSnapshotRunning()) {
                SnapshotFilterMessage message = pollForRequest();

                if (message != null) {
                    boolean response = filterHandler.shouldSnapshot(message.tableId);
                    try {
                        message.responseQueue.put(response);
                    } catch (InterruptedException e) {
                        LOGGER.error("Partial snapshotter response timed out.", e);
                    }
                }
            }
            filterHandler.snapshotCompleted();
        }
        finally {
            filterHandler.cleanUp();
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

    private boolean isSnapshotRunning() {
        return !snapshotLifetimeMonitor.snapshotIsDone();
    }
}
