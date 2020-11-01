package io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message;

@FunctionalInterface
public interface SnapshotFilterMessage {
    MessageResponse handle();
}
