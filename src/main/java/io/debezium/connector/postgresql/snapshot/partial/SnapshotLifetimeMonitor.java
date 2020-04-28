package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.config.CommonConnectorConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;
import java.lang.management.ManagementFactory;

public class SnapshotLifetimeMonitor {

    private static final Logger LOGGER = LoggerFactory.getLogger(SnapshotLifetimeMonitor.class);

    private static final String ATTRIBUTE = "SnapshotCompleted";
    private static final int MAX_SEC_TO_WAIT_FOR_STARTUP = 30;
    private static final int ONE_SECOND_IN_MS = 1000;

    private final MBeanServer mbeanServer;
    private ObjectName objectName;

    public SnapshotLifetimeMonitor(CommonConnectorConfig config) {
        this.mbeanServer = ManagementFactory.getPlatformMBeanServer();
        this.objectName = null;
        try {
            this.objectName = new ObjectName("debezium." + config.getContextName().toLowerCase() +
                    ":type=connector-metrics,context=snapshot,server=" + config.getLogicalName());
        }
        catch (MalformedObjectNameException e) {
            LOGGER.error("Bad name for JMX snapshot monitor");
        }
    }

    public void waitForSnapshotToStart() {
        LOGGER.debug("Waiting for mbean {} to start", objectName);
        int startupCounter = 0;

        while (!mbeanServer.isRegistered(objectName) && startupCounter < MAX_SEC_TO_WAIT_FOR_STARTUP) {
            LOGGER.debug("mbean {} was not found too be registered. Retrying...", objectName);
            try {
                Thread.sleep(ONE_SECOND_IN_MS);
            }
            catch (InterruptedException e) {
                LOGGER.warn("Interrupted while waiting for snapshot mbean to register", e);
            }
            startupCounter++;
        }

        if (!mbeanServer.isRegistered(objectName)) {
            throw new RuntimeException("Failed to discover mbean {}. Check that the snapshot has not crashed");
        }
    }

    public boolean snapshotIsDone() {
        LOGGER.debug("Checking JMX metrics to see if snapshot is finished");

        try {

            if (objectName != null) {
                return (boolean) mbeanServer.getAttribute(objectName, ATTRIBUTE);
            }
        }
        catch (InstanceNotFoundException e) {
            LOGGER.error("Metrics should have already started.", e);
        }
        catch (Exception e) {
            LOGGER.error("An error occurred with the snapshot monitor", e);
        }

        // If error occurs, close early and let default snapshot choice take over
        return true;
    }
}
