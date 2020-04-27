package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.relational.TableId;

import java.util.concurrent.ArrayBlockingQueue;

public class SnapshotFilterMessage {
    public TableId tableId;
    public ArrayBlockingQueue<Boolean> responseQueue;

    public SnapshotFilterMessage(TableId tableId, ArrayBlockingQueue<Boolean> responseQueue) {
        this.tableId = tableId;
        this.responseQueue = responseQueue;
    }
}
