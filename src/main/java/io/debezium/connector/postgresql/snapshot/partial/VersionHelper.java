package io.debezium.connector.postgresql.snapshot.partial;

import io.debezium.connector.postgresql.Module;

public class VersionHelper {

    public static boolean isCurrentVersionCompatibleWithPlugin() {
        String version = Module.version();
        // Always include SNAPSHOT versions
        return version.endsWith("SNAPSHOT") || isSupportedVersion(version);
    }

    private static boolean isSupportedVersion(String version) {
        String[] explodedVersion = version.split("\\.");
        int major = Integer.parseInt(explodedVersion[0]);
        int minor = Integer.parseInt(explodedVersion[1]);

        if (major >= 1 && minor >= 3) {
            // 1.3.0.Beta2 was the first release that supported Snapshotter#shouldStreamEventsStartingFromSnapshot
            if (major == 1 && minor == 3 && explodedVersion.length >= 4 &&
                ! explodedVersion[3].equals("Final") && ! explodedVersion[3].equals("Beta2")) {
                return false;
            }
            return true;
        }
        return false;
    }
}
