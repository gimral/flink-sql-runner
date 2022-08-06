package org.apache.flink.sql;

import org.apache.flink.TestUtil;
import org.apache.flink.configuration.EnvironmentConfiguration;
import org.apache.flink.configuration.SqlConfiguration;
import org.apache.flink.util.FileUtils;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlParserTest {

    private static Stream<Arguments> provideSqlStatements() {
        return Stream.of(
                Arguments.of("sql/simple-kafka.sql",  Arrays.asList("CREATE TABLE gen_order\n" +
                                "WITH (\n" +
                                "'connector' = 'datagen',\n" +
                                "'number-of-rows' = '10'\n" +
                                ")\n" +
                                "LIKE `kafka`.`default`.`order`(EXCLUDING ALL);\n",
                        "INSERT INTO `kafka`.`default`.`order` SELECT * FROM gen_order;\n")),
                Arguments.of("sql/env-variable-blackhole.sql", Arrays.asList("CREATE TABLE orders (\n" +
                        "order_number BIGINT,\n" +
                        "price        DECIMAL(32,2),\n" +
                        "buyer        ROW<first_name STRING, last_name STRING>,\n" +
                        "order_time   TIMESTAMP(3)\n" +
                        ") WITH (\n" +
                        "'connector' = 'datagen'\n" +
                        ");\n",
                        "CREATE TABLE print_table WITH ('connector' = 'print')\n" +
                                "LIKE orders;\n",
                        "CREATE TABLE blackhole_table WITH ('connector' = 'blackhole')\n" +
                                "LIKE orders;\n",
                        "EXECUTE STATEMENT SET\n" +
                                "BEGIN\n" +
                                "INSERT INTO blackhole_table SELECT * FROM orders;\n" +
                                "INSERT INTO blackhole_table SELECT * FROM orders;\n" +
                                "END;\n"
                        ))
        );
    }


    @ParameterizedTest
    @MethodSource("provideSqlStatements")
    public void testSimpleSql(String resourceName,List<String> expected) throws IOException {
        TestUtil testUtil = new TestUtil();
        File file = testUtil.loadResourceFile(resourceName);
        String sql = FileUtils.readFileUtf8(file);

        Map<String,String> env = new java.util.HashMap<>();
        env.put("TARGET_TABLE","blackhole_table");
        env.put(SqlParser.ENVIRONMENT_PREFIX_KEY,"$");

        SqlParser sqlParser = new SqlParser();
        List<String> parsedSql = sqlParser.parseStatements(sql,env);

        assertEquals(expected,parsedSql);
    }
}
