package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.PostgresConnectorConfig;
import io.debezium.connector.postgresql.snapshot.PartialSnapshotter;
import io.debezium.connector.postgresql.snapshot.partial.filters.FilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.filters.handlers.PostgresJdbcFilterHandler;
import io.debezium.connector.postgresql.snapshot.partial.filters.handlers.ThreadedSnapshotFilter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionHelper {

    public static String MIN_VERSION = "1.3.0.Final";

    private static final Logger LOGGER = LoggerFactory.getLogger(VersionHelper.class);

    public static boolean isCurrentVersionCompatibleWithPlugin() {
        String version = Module.version();
        // Always include SNAPSHOT versions
        return version.endsWith("SNAPSHOT") || isSupportedVersion(version);
    }

    public static boolean usesDeprecatedThreadedHandler() {
        String version = Module.version();
        String[] explodedVersion = version.split("\\.");
        int minor = Integer.parseInt(explodedVersion[1]);
        if (version.equals("1.4.0.Aplha1") || minor <= 3) {
            return true;
        }

        return false;
    }

    public static FilterHandler getFilterHandlerForVersion(PostgresConnectorConfig config) {
        PartialSnapshotConfig partialSnapshotConfig = new PartialSnapshotConfig(config.getConfig());
        FilterHandler handler = new PostgresJdbcFilterHandler(config, partialSnapshotConfig);
        if (usesDeprecatedThreadedHandler()) {
            LOGGER.warn("The Partial Snapshotter is using a deprecated Debezium version. Consider upgrading to a 1.4 " +
                "release or greater.");
            return new ThreadedSnapshotFilter(handler);
        }

        return handler;
    }

    private static boolean isSupportedVersion(String version) {
        String[] explodedVersion = version.split("\\.");
        int major = Integer.parseInt(explodedVersion[0]);
        int minor = Integer.parseInt(explodedVersion[1]);

        if (major >= 1 && minor >= 3) {
            // 1.3.0.CR1 was the first release that supported Snapshotter#shouldStreamEventsStartingFromSnapshot
            // Note: We should phase out CR1 references in favor of only supporting Final
            // 1.4.0.Final will support the new close handler.
            if (major == 1 && minor == 3 && explodedVersion.length >= 4 &&
                ! explodedVersion[3].equals("Final") && ! explodedVersion[3].equals("CR1")) {
                return false;
            }
            return true;
        }
        return false;
    }
}
