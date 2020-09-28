package io.debezium.connector.postgresql.snapshot.partial.message;

import io.debezium.connector.postgresql.snapshot.partial.FilterHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PoisonPillMessage implements SnapshotFilterMessage {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoisonPillMessage.class);

    private final FilterHandler filterHandler;

    public PoisonPillMessage(FilterHandler filterHandler) {
        this.filterHandler = filterHandler;
    }

    @Override
    public MessageResponse handle() {
        filterHandler.close();
        LOGGER.info("Snapshot filter successfully closed.");
        return new MessageResponse(false);
    }
}
