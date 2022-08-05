package org.apache.flink.sql;

import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.rest.exceptions.RestClientException;
import io.confluent.kafka.schemaregistry.json.JsonSchema;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafka;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafka$;
import io.github.embeddedkafka.schemaregistry.EmbeddedKafkaConfigImpl;
import org.apache.flink.TestUtil;
import org.apache.flink.configuration.EnvironmentConfiguration;
import org.apache.flink.configuration.SqlConfiguration;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.table.catalog.kafka.factories.SchemaRegistryClientFactory;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.FileUtils;
import org.junit.ClassRule;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import scala.collection.immutable.HashMap;
import scala.concurrent.duration.Duration;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SqlExecutorIntegrationTest {
    private static EmbeddedKafka$ kafkaOps;
    private static EmbeddedKafkaConfigImpl kafkaConfig;
    private final static int KAFKA_PORT = 6001;
    private final static int ZK_PORT = 6002;
    private final static int SR_PORT = 6003;
    public final static List<String> EMBEDDED_SCHEMA_REGISTRY_URIS = Collections.singletonList("http://localhost:"+SR_PORT);

    @ClassRule
    public static MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    @BeforeAll
    public static void setup() throws RestClientException, IOException {
        kafkaConfig = new EmbeddedKafkaConfigImpl(KAFKA_PORT,ZK_PORT,SR_PORT,new HashMap<>(),new HashMap<>(),new HashMap<>(), new HashMap<>());
        EmbeddedKafka.start(kafkaConfig);
        kafkaOps = EmbeddedKafka$.MODULE$;
        SchemaRegistryClientFactory schemaRegistryClientFactory = new SchemaRegistryClientFactory();
        SchemaRegistryClient schemaRegistryClient = schemaRegistryClientFactory.get(EMBEDDED_SCHEMA_REGISTRY_URIS,1000,new java.util.HashMap<>());

        TestUtil testUtil = new TestUtil();
        File schemaFolder = testUtil.loadResourceFile("schema");
        for (File schemaFile :
                Objects.requireNonNull(schemaFolder.listFiles())) {
            String schema = FileUtils.readFileUtf8(schemaFile);
            String topicName = schemaFile.getName().replace(".schema.json","");
            kafkaOps.createCustomTopic(topicName,new HashMap<>(),1,1, kafkaConfig);
            schemaRegistryClient.register(topicName, new JsonSchema(schema));
        }
    }

    @AfterAll
    public static void tearDown() {
        EmbeddedKafka.stop();
    }

    private static Stream<Arguments> provideSqlParameters() {
        return Stream.of(
                Arguments.of("sql/simple-kafka.sql", "order"),
                Arguments.of("sql/statement-set-datagen.sql", "")
        );
    }

    @ParameterizedTest
    @MethodSource("provideSqlParameters")
    public void testSql(String resourceName,String topicName) throws IOException, InterruptedException {
        TestUtil testUtil = new TestUtil();
        File file = testUtil.loadResourceFile(resourceName);

        String absolutePath = file.getAbsolutePath();

        Map<String,String> env = new java.util.HashMap<>();
        env.put("CATALOG_KAFKA_TYPE","kafka");
        env.put("CATALOG_KAFKA_PROPERTIES_BOOTSTRAP_SERVERS","localhost:" + KAFKA_PORT);
        env.put("CATALOG_KAFKA_SCHEMA_REGISTRY_URI","http://localhost:" + SR_PORT);
        EnvironmentConfiguration environmentConfiguration = mock(EnvironmentConfiguration.class);
        when(environmentConfiguration.getenv()).thenReturn(env);
        SqlConfiguration sqlConfiguration = new SqlConfiguration(environmentConfiguration);

        SqlExecutor sqlExecutor = new SqlExecutor(sqlConfiguration);
        sqlExecutor.executeSqlFile(absolutePath);

        if(!Objects.equals(topicName, "")) {
            String record = kafkaOps.consumeFirstStringMessageFrom("order", false, Duration.create(120, TimeUnit.SECONDS), kafkaConfig);
            assertNotNull(record);
        }
    }
}
