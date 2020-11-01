package io.debezium.connector.postgresql.snapshot.partial.filters;

import io.debezium.relational.TableId;

public interface FilterHandler {
    boolean shouldSnapshot(TableId tableId);
    void close();
}
