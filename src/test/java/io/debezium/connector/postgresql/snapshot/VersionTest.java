package io.debezium.connector.postgresql.snapshot;

import io.debezium.connector.postgresql.Module;
import io.debezium.connector.postgresql.snapshot.partial.VersionHelper;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class VersionTest {

    @Test
    public void testCompatibleVersion() {
        try (MockedStatic<Module> mockedModule = Mockito.mockStatic(Module.class)) {
            mockedModule.when(Module::version).thenReturn("1.4.0.Final");
            Assert.assertTrue(VersionHelper.isCurrentVersionCompatibleWithPlugin());
        }
    }

    @Test
    public void testIncompatibleTagVersion() {
        try (MockedStatic<Module> mockedModule = Mockito.mockStatic(Module.class)) {
            mockedModule.when(Module::version).thenReturn("1.3.0.Beta1");
            Assert.assertFalse(VersionHelper.isCurrentVersionCompatibleWithPlugin());
        }
    }

    @Test
    public void testIncompatibleMinorVersion() {
        try (MockedStatic<Module> mockedModule = Mockito.mockStatic(Module.class)) {
            mockedModule.when(Module::version).thenReturn("1.2.0.Final");
            Assert.assertFalse(VersionHelper.isCurrentVersionCompatibleWithPlugin());
        }
    }

    @Test
    public void testFirstAllowedVersion() {
        try (MockedStatic<Module> mockedModule = Mockito.mockStatic(Module.class)) {
            mockedModule.when(Module::version).thenReturn("1.3.0.Beta2");
            Assert.assertTrue(VersionHelper.isCurrentVersionCompatibleWithPlugin());
        }
    }

    @Test
    public void testFirstFinalVersion() {
        try (MockedStatic<Module> mockedModule = Mockito.mockStatic(Module.class)) {
            mockedModule.when(Module::version).thenReturn("1.3.0.Final");
            Assert.assertTrue(VersionHelper.isCurrentVersionCompatibleWithPlugin());
        }
    }

    @Test
    public void testFirstFinalWithBugFixVersion() {
        try (MockedStatic<Module> mockedModule = Mockito.mockStatic(Module.class)) {
            mockedModule.when(Module::version).thenReturn("1.3.1.Final");
            Assert.assertTrue(VersionHelper.isCurrentVersionCompatibleWithPlugin());
        }
    }

}
