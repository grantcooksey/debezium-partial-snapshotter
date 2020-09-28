package io.debezium.connector.postgresql.snapshot.partial.message;

@FunctionalInterface
public interface SnapshotFilterMessage {
    MessageResponse handle();
}
