package org.apache.flink.sql;

import org.apache.flink.configuration.EnvironmentConfiguration;
import org.apache.flink.configuration.SqlConfiguration;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlConfigurationTest {
    @Test
    public void testSingleCatalogDefinitionFromEnv(){
        Map<String,String> env = new HashMap<>();
        env.put("CATALOG_TEST_TYPE","kafka");
        env.put("CATALOG_TEST_PROPERTIES_BOOTSTRAP_SERVERS","kafka:6000");

        Map<String,Map<String,String>> expectedCatalogs = new HashMap<>();
        Map<String,String> testCatalog = new HashMap<>();
        testCatalog.put("type","kafka");
        testCatalog.put("properties.bootstrap.servers","kafka:6000");
        expectedCatalogs.put("test", testCatalog);

        EnvironmentConfiguration environmentConfiguration = mock(EnvironmentConfiguration.class);
        when(environmentConfiguration.getenv()).thenReturn(env);
        SqlConfiguration sqlConfiguration = new SqlConfiguration(environmentConfiguration);

        sqlConfiguration.load();
        Map<String,Map<String,String>> catalogs = sqlConfiguration.getCatalogs();
        assertEquals(1,catalogs.size());
        assertEquals(expectedCatalogs,catalogs);
    }

    @Test
    public void testMultipleCatalogDefinitionFromEnv(){
        Map<String,String> env = new HashMap<>();
        env.put("CATALOG_TEST_TYPE","kafka");
        env.put("CATALOG_TEST_PROPERTIES_BOOTSTRAP_SERVERS","kafka:6000");

        env.put("CATALOG_TEST2_TYPE","hive");
        env.put("CATALOG_TEST2_HIVE__CONF__DIR","/opt/hive-conf");

        env.put("CATALOG_TEST3_TYPE","kafka");
        env.put("CATALOG_TEST3_PROPERTIES_BOOTSTRAP_SERVERS","kafka:6001");

        Map<String,Map<String,String>> expectedCatalogs = new HashMap<>();
        Map<String,String> testCatalog = new HashMap<>();
        testCatalog.put("type","kafka");
        testCatalog.put("properties.bootstrap.servers","kafka:6000");
        expectedCatalogs.put("test", testCatalog);
        Map<String,String> test2Catalog = new HashMap<>();
        test2Catalog.put("type","hive");
        test2Catalog.put("hive-conf-dir","/opt/hive-conf");
        expectedCatalogs.put("test2", test2Catalog);
        Map<String,String> test3Catalog = new HashMap<>();
        test3Catalog.put("type","kafka");
        test3Catalog.put("properties.bootstrap.servers","kafka:6001");
        expectedCatalogs.put("test3", test3Catalog);

        EnvironmentConfiguration environmentConfiguration = mock(EnvironmentConfiguration.class);
        when(environmentConfiguration.getenv()).thenReturn(env);
        SqlConfiguration sqlConfiguration = new SqlConfiguration(environmentConfiguration);

        sqlConfiguration.load();
        Map<String,Map<String,String>> catalogs = sqlConfiguration.getCatalogs();
        assertEquals(3,catalogs.size());
        assertEquals(expectedCatalogs,catalogs);
    }
}
