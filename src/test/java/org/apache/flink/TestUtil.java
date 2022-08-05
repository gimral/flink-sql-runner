package org.apache.flink;

import java.io.File;
import java.util.Objects;

public class TestUtil {

    public File loadResourceFile(String resourceName){
        ClassLoader classLoader = getClass().getClassLoader();
        return new File(Objects.requireNonNull(classLoader.getResource(resourceName)).getFile());
    }
}
