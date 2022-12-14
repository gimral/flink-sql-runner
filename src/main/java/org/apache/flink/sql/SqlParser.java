package org.apache.flink.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;

public class SqlParser {
    private static final Logger LOG = LoggerFactory.getLogger(SqlParser.class);

    private static final String STATEMENT_DELIMITER = ";"; // a statement should end with `;`
    private static final String LINE_DELIMITER = "\n";

    @SuppressWarnings("RegExpRedundantEscape")
    private static final String COMMENT_PATTERN = "(--.*)|(((\\/\\*)+?[\\w\\W]+?(\\*\\/)+))";

    public static final String ENVIRONMENT_PREFIX_KEY = "ENVIRONMENT_PREFIX_KEY";
    private static final String ENVIRONMENT_PREFIX_DEFAULT = "ENV:";

    public List<String> parseStatements(String script, Map<String,String> environmentVariables) {
        String formatted = formatSql(script).replaceAll(COMMENT_PATTERN, "");

        String envPrefix = environmentVariables.getOrDefault(ENVIRONMENT_PREFIX_KEY,ENVIRONMENT_PREFIX_DEFAULT);

        for (Map.Entry<String,String> environmentVariable:
             environmentVariables.entrySet()) {
            formatted = formatted.replaceAll( Matcher.quoteReplacement(envPrefix + environmentVariable.getKey()),environmentVariable.getValue());
        }

        List<String> statements = new ArrayList<>();

        StringBuilder current = null;
        boolean statementSet = false;
        for (String line : formatted.split("\n")) {
            String trimmed = line.trim();
            if ("".equals(trimmed)) {
                continue;
            }
            if (current == null) {
                current = new StringBuilder();
            }
            if (trimmed.startsWith("EXECUTE STATEMENT SET")) {
                statementSet = true;
            }
            current.append(trimmed);
            current.append("\n");
            if (trimmed.endsWith(STATEMENT_DELIMITER)) {
                if (!statementSet || trimmed.equals("END;")) {
                    statements.add(current.toString());
                    current = null;
                    statementSet = false;
                }
            }
        }
        return statements;
    }

    private String formatSql(String content) {
        String trimmed = content.trim();
        StringBuilder formatted = new StringBuilder();
        formatted.append(trimmed);
        if (!trimmed.endsWith(STATEMENT_DELIMITER)) {
            formatted.append(STATEMENT_DELIMITER);
        }
        formatted.append(LINE_DELIMITER);
        return formatted.toString();
    }
}
