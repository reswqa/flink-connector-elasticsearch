package org.apache.flink.streaming.tests;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.elasticsearch.test.DockerImageVersions;
import org.apache.flink.connector.testframe.container.FlinkContainers;
import org.apache.flink.connector.testframe.container.TestcontainersSettings;
import org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions;
import org.apache.flink.test.resources.ResourceTestUtils;
import org.apache.flink.test.util.SQLJobSubmission;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class Elasticsearch7SmokeTest {
    private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7SmokeTest.class);

    private static final String ELASTICSEARCH_HOSTNAME = "elasticsearch";

    @Container
    public static final ElasticsearchContainer ELASTICSEARCH_CONTAINER =
            new ElasticsearchContainer(DockerImageName.parse(DockerImageVersions.ELASTICSEARCH_6))
                    .withEnv("cluster.routing.allocation.disk.threshold_enabled", "false")
                    .withNetworkAliases(ELASTICSEARCH_HOSTNAME);

    public static final TestcontainersSettings TESTCONTAINERS_SETTINGS =
            TestcontainersSettings.builder().logger(LOG).dependsOn(ELASTICSEARCH_CONTAINER).build();

    public static final FlinkContainers FLINK =
            FlinkContainers.builder().withTestcontainersSettings(TESTCONTAINERS_SETTINGS).build();

    private final Path sqlConnectorEsJar =
            ResourceTestUtils.getResource("flink-sql-connector-elasticsearch7\\.jar");

    private static Configuration getConfiguration() {
        // modify configuration to have enough slots
        final Configuration flinkConfig = new Configuration();
        flinkConfig.setInteger(TaskManagerOptions.NUM_TASK_SLOTS, 3);
        flinkConfig.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, true);
        return flinkConfig;
    }

    @BeforeAll
    static void before() throws Exception {
        ELASTICSEARCH_CONTAINER.start();
        // FLINK.start();
    }

    @AfterAll
    static void after() throws Exception {
        ELASTICSEARCH_CONTAINER.stop();
        // FLINK.stop();
    }

    @Test
    void testSqlJob() throws Exception {
        FLINK.start();
        String index = "es-smoke-test";
        Map<String, String> map = new HashMap<>();
        map.put("$CONNECTOR_NAME", "elasticsearch-7");
        map.put("$INDEX", index);
        map.put("$HOSTS", ELASTICSEARCH_CONTAINER.getHttpHostAddress());
        List<String> sqlLines = initializeSqlLines(map);
        executeSqlStatements(sqlLines);

        Elasticsearch7Client client =
                new Elasticsearch7Client(ELASTICSEARCH_CONTAINER.getHttpHostAddress());
        Map<String, Object> response = client.get(index, "1_2012-12-12T12:12:12");
        Map<Object, Object> expectedMap = new HashMap<>();
        expectedMap.put("a", 1);
        expectedMap.put("b", "00:00:12");
        expectedMap.put("c", "ABCDE");
        expectedMap.put("d", 12.12d);
        expectedMap.put("e", 2);
        expectedMap.put("f", "2003-10-20");
        expectedMap.put("g", "2012-12-12 12:12:12");
        assertThat(response).isEqualTo(expectedMap);
    }

    private void executeSqlStatements(List<String> sqlLines) throws Exception {
        FLINK.submitSQLJob(
                new SQLJobSubmission.SQLJobSubmissionBuilder(sqlLines)
                        .addJars(sqlConnectorEsJar)
                        .build());
    }

    private List<String> initializeSqlLines(Map<String, String> vars) throws IOException {
        URL url = Elasticsearch7SmokeTest.class.getClassLoader().getResource("smoke-test.sql");
        if (url == null) {
            throw new FileNotFoundException("smoke-test.sql");
        }

        List<String> lines = Files.readAllLines(new File(url.getFile()).toPath());
        List<String> result = new ArrayList<>();
        for (String line : lines) {
            for (Map.Entry<String, String> var : vars.entrySet()) {
                line = line.replace(var.getKey(), var.getValue());
            }
            result.add(line);
        }

        return result;
    }

    @Test
    void testDataStreamJob() {}
}
