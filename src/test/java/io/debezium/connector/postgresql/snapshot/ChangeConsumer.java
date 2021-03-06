package io.debezium.connector.postgresql.snapshot;

import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.DebeziumEngine.RecordCommitter;
import io.debezium.engine.RecordChangeEvent;
import io.debezium.engine.StopEngineException;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;


public class ChangeConsumer implements DebeziumEngine.ChangeConsumer<RecordChangeEvent<SourceRecord>> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ChangeConsumer.class);

    private final LinkedBlockingQueue<SourceRecord> dataTopic;

    public ChangeConsumer() {
        this.dataTopic = new LinkedBlockingQueue<>();
    }

    @Override
    public void handleBatch(List<RecordChangeEvent<SourceRecord>> records, RecordCommitter<RecordChangeEvent<SourceRecord>> committer) throws InterruptedException {
        try {
            for (RecordChangeEvent<SourceRecord> record : records) {
                try {
                    dataTopic.put(record.record());
                    committer.markProcessed(record);
                }
                catch (StopEngineException ex) {
                    // ensure that we mark the record as finished
                    // in this case
                    committer.markProcessed(record);
                    throw ex;
                }
            }
        }
        finally {
            committer.markBatchFinished();
        }
    }

    public List<SourceRecord> get(int n) {
        List<SourceRecord> records = new LinkedList<>();
        for (int i = 0; i < n; i++) {
            SourceRecord record = pollDataTopic();
            if (record != null) {
                records.add(record);
            }
            else {
                LOGGER.warn("Consumer timed out!");
            }
        }
        return records;
    }

    public SourceRecord pollDataTopic() {
        return pollDataTopic(TestUtils.MAX_TEST_DURATION_SEC);
    }

    public SourceRecord pollDataTopic(int timeoutSec) {
        try {
            return dataTopic.poll(timeoutSec, TimeUnit.SECONDS);
        }
        catch (InterruptedException e) {
            LOGGER.error("Data topic polling shouldn't have been interrupted...", e);
        }

        return null;
    }

    public boolean isEmptyForSnapshot() {
        if (isEmpty()) {
            return true;
        }

        SourceRecord record =  pollDataTopic(1);
        if (record == null) {
            return true;
        }

        String snapshotStatus = ((Struct) record.value()).getStruct("source").getString("snapshot");

        return snapshotStatus == null || snapshotStatus.equals("false");
    }

    public boolean isEmpty() {
        return dataTopic.isEmpty();
    }
}
