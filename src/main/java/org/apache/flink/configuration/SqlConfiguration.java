package org.apache.flink.configuration;

import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class SqlConfiguration {
    private static final String CATALOG_PREFIX = "catalog.";

    private final EnvironmentConfiguration environmentConfiguration;
    private final Map<String,String> originalConfig;
    private final Map<String,Map<String,String>> catalogs;

    public SqlConfiguration() {
        this(new EnvironmentConfiguration());
    }

    public SqlConfiguration(EnvironmentConfiguration environmentConfiguration) {
        this.environmentConfiguration = environmentConfiguration;
        originalConfig = new HashMap<>();
        catalogs = new HashMap<>();
    }

    public void load(){
        Map<String,String> envConfig = loadEnv();
        originalConfig.putAll(envConfig);

        originalConfig.forEach((k,v) -> {
            if(k.startsWith(CATALOG_PREFIX)){
                String[] keyParts = k.substring(CATALOG_PREFIX.length()).split("\\.",2);
                if(keyParts.length < 2)
                    throw new IllegalArgumentException("Catalog definition is malformed for key "+ k);
                String catalogName = keyParts[0];
                catalogs.computeIfAbsent(catalogName, n -> new HashMap<>())
                        .put(keyParts[1],v);
            }
        });
    }

    public Map<String,String> getOriginalConfig(){
        return originalConfig;
    }

    public Map<String,Map<String,String>> getCatalogs(){
        return catalogs;
    }

    private Map<String,String> loadEnv(){
        Map<String,String> envVariables = new HashMap<>();
        environmentConfiguration.getenv().forEach((k,v) -> envVariables.put(
                k.toLowerCase(Locale.ROOT)
                .replace("__","-")
                .replace("_","."),v));
        return envVariables;
    }
}
