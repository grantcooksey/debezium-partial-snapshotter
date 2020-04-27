package io.debezium.connector.postgresql.snapshot.partial;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class SnapshotFilterManager implements Runnable {

    private static final int ONE_SECOND = 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotFilterManager.class);

    private final LinkedBlockingQueue<SnapshotFilterMessage> requestQueue;
    private final FilterHandler filterHandler;
    private final ExecutorService executorService;
    private final Future<?> jmxSnapshotMonitor;

    public SnapshotFilterManager(LinkedBlockingQueue<SnapshotFilterMessage> requestQueue, FilterHandler filterHandler) {
        this.requestQueue = requestQueue;
        this.filterHandler = filterHandler;

        executorService = Executors.newSingleThreadExecutor();
        jmxSnapshotMonitor = executorService.submit(() -> {
            // TODO
            System.out.println("Starting JMX metric monitor");
            try {
                Thread.sleep(10000);
            }
            catch (InterruptedException e) {
                System.out.println("This was bad" + e.getLocalizedMessage());
            }
            System.out.println("JMX metric monitor detected snapshot finished! Closing..");
        });
    }

    @Override
    public void run() {
        while (isSnapshotRunning()) {
            SnapshotFilterMessage message = pollForRequest();

            if (message != null) {
                boolean response = filterHandler.shouldSnapshot(message.tableId);
                try {
                    message.responseQueue.put(response);
                }
                catch (InterruptedException e) {
                    LOGGER.error("Partial snapshotter response timed out.", e);
                }
            }
        }

        executorService.shutdownNow();
        System.out.println("Snapshot is complete, shutting down snapshot filter thread.");
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
        return !jmxSnapshotMonitor.isDone();
    }
}
