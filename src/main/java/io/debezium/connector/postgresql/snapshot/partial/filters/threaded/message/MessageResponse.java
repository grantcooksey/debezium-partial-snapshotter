package io.debezium.connector.postgresql.snapshot.partial.filters.threaded.message;

public class MessageResponse {

    private final boolean isFilterActive;

    public MessageResponse(boolean isFilterActive) {
        this.isFilterActive = isFilterActive;
    }

    public boolean isFilterActive() {
        return isFilterActive;
    }
}
