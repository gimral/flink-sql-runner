package org.apache.flink.sql;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.SqlConfiguration;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public class SqlExecutor {
    private static final Logger LOG = LoggerFactory.getLogger(SqlExecutor.class);
    private final SqlParser sqlParser;
    private final SqlConfiguration sqlConfiguration;

    public SqlExecutor() {
     this(new SqlConfiguration());
    }

    public SqlExecutor(SqlConfiguration sqlConfiguration) {
        this.sqlConfiguration = sqlConfiguration;
        sqlParser = new SqlParser();
    }

    public void executeSqlFile(String file) throws IOException {

        sqlConfiguration.load();

        TableEnvironment tableEnv = TableEnvironment.create(new Configuration());
        registerCatalogs(tableEnv,sqlConfiguration.getCatalogs());

        String script = FileUtils.readFileUtf8(new File(file));
        List<String> statements = sqlParser.parseStatements(script);



        for (String statement : statements) {
            LOG.info("Executing:\n{}", statement);
            tableEnv.executeSql(statement);
        }
    }

    private void registerCatalogs(TableEnvironment tableEnv,Map<String, Map<String,String>> catalogs){
        catalogs.forEach((name,conf) -> {
            LOG.info("Creating catalog:\n{}", name);
            StringBuilder statementBuilder = new StringBuilder();
            statementBuilder.append("CREATE CATALOG ")
                            .append(name)
                            .append(" WITH(");
            conf.forEach((k,v) -> statementBuilder.append("'").append(k).append("' = '").append(v).append("'").append(","));
            statementBuilder.deleteCharAt(statementBuilder.length() -1);
            statementBuilder.append(");");

            tableEnv.executeSql(statementBuilder.toString());
        });


//                "CATALOG_KAFKA_TYPE"
//                "CATALOG_KAFKA_PROPERTIES_BOOTSTRAP_SERVERS"
    }
}
