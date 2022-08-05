package org.apache.flink.configuration;

import java.util.Map;

public class EnvironmentConfiguration {
    public Map<String,String> getenv(){
        return System.getenv();
    }
}
