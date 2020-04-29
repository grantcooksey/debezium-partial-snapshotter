package io.debezium.connector.postgresql.snapshot;

import org.junit.ClassRule;

import org.testcontainers.containers.PostgreSQLContainer;

public class BaseTest {

    @ClassRule
    public static PostgreSQLContainer postgreSQLContainer = spinUpPGInstance();

    private static PostgreSQLContainer spinUpPGInstance() {
        PostgreSQLContainer newContainer = new PostgreSQLContainer("postgres:11")
                .withDatabaseName("postgres")
                .withUsername("postgres")
                .withPassword("postgres");
        newContainer.withCommand("postgres", "-c", "wal_level=logical");
        return newContainer;
    }

}
