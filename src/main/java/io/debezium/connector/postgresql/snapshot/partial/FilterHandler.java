package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.relational.TableId;

import java.util.Optional;

public interface FilterHandler {
    public boolean shouldSnapshot(TableId tableId);
}
