/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.pipeline.tests;

import org.apache.flink.cdc.common.test.utils.TestUtils;
import org.apache.flink.cdc.connectors.elasticsearch.sink.utils.ElasticsearchContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlVersion;
import org.apache.flink.cdc.connectors.mysql.testutils.UniqueDatabase;
import org.apache.flink.cdc.pipeline.tests.utils.PipelineTestEnvironment;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.junit.*;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.io.IOException;
import java.nio.file.Path;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.Assert.*;

/** End-to-end tests for mysql cdc to Elasticsearch pipeline job. */
@RunWith(Parameterized.class)
public class MySqlToElasticsearchE2eITCase extends PipelineTestEnvironment {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlToElasticsearchE2eITCase.class);

    // ------------------------------------------------------------------------------------------
    // MySQL Variables (we always use MySQL as the data source for easier verifying)
    // ------------------------------------------------------------------------------------------
    protected static final String MYSQL_TEST_USER = "mysqluser";
    protected static final String MYSQL_TEST_PASSWORD = "mysqlpw";
    protected static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";

    private static final String ELASTICSEARCH_VERSION = "8.12.1";
    private static final String DEFAULT_USERNAME = "elastic";
    private static final String DEFAULT_PASSWORD = "123456";

    public static final Duration DEFAULT_RESULT_VERIFY_TIMEOUT = Duration.ofSeconds(30);

    private ElasticsearchClient client = createElasticsearchClient();

    @ClassRule
    public static final MySqlContainer MYSQL =
            (MySqlContainer)
                    new MySqlContainer(
                                    MySqlVersion.V8_0) // v8 support both ARM and AMD architectures
                            .withConfigurationOverride("docker/mysql/my.cnf")
                            .withSetupSQL("docker/mysql/setup.sql")
                            .withDatabaseName("flink-test")
                            .withUsername("flinkuser")
                            .withPassword("flinkpw")
                            .withNetwork(NETWORK)
                            .withNetworkAliases("mysql");

    @ClassRule
    public static final ElasticsearchContainer ELASTICSEARCH =  createElasticsearchContainer();


    private static ElasticsearchContainer createElasticsearchContainer() {
        ElasticsearchContainer esContainer = new ElasticsearchContainer(ELASTICSEARCH_VERSION);
        esContainer.withLogConsumer(new Slf4jLogConsumer(LOG));
        esContainer.withPassword(DEFAULT_PASSWORD);
        esContainer.withEnv("xpack.security.enabled", "true");
        return esContainer;
    }

    protected final UniqueDatabase mysqlInventoryDatabase =
            new UniqueDatabase(MYSQL, "mysql_inventory", MYSQL_TEST_USER, MYSQL_TEST_PASSWORD);

    @BeforeClass
    public static void initializeContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL)).join();
        Startables.deepStart(Stream.of(ELASTICSEARCH)).join();
        LOG.info("Containers are started.");
    }

    @BeforeEach
    public void setUp() {
        client = createElasticsearchClient();
    }

    @AfterEach
    public void tearDown() throws Exception {
        if (client != null) {
            client.shutdown();
        }
    }

    @Before
    public void before() throws Exception {
        super.before();
        mysqlInventoryDatabase.createAndInitialize();
        createElasticsearchDatabase(mysqlInventoryDatabase.getDatabaseName());
    }

    @After
    public void after() {
        super.after();
        mysqlInventoryDatabase.dropDatabase();
        try {
            dropElasticsearchDatabase(mysqlInventoryDatabase.getDatabaseName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testSyncWholeDatabase() throws Exception {
        String databaseName = mysqlInventoryDatabase.getDatabaseName();
        String pipelineJob =
                String.format(
                        "source:\n"
                                + "  type: mysql\n"
                                + "  hostname: mysql\n"
                                + "  port: 3306\n"
                                + "  username: %s\n"
                                + "  password: %s\n"
                                + "  tables: %s.\\.*\n"
                                + "  server-id: 5400-5404\n"
                                + "  server-time-zone: UTC\n"
                                + "\n"
                                + "sink:\n"
                                + "  type: elasticsearch\n"
                                + "  hosts: elasticsearch:9092\n"
                                + "  username: %s\n"
                                + "  password: \"%s\"\n"
                                + "  version: 8\n"
                                + "  batch.size.max.bytes: 52428800\n"
                                + "\n"
                                + "pipeline:\n"
                                + "  parallelism: %d",
                        MYSQL_TEST_USER,
                        MYSQL_TEST_PASSWORD,
                        databaseName,
                        DEFAULT_USERNAME,
                        DEFAULT_PASSWORD,
                        parallelism);
        Path mysqlCdcJar = TestUtils.getResource("mysql-cdc-pipeline-connector.jar");
        Path ElasticsearchCdcConnector = TestUtils.getResource("elasticsearch-cdc-pipeline-connector.jar");
        Path mysqlDriverJar = TestUtils.getResource("mysql-driver.jar");
        submitPipelineJob(pipelineJob, mysqlCdcJar, ElasticsearchCdcConnector, mysqlDriverJar);
        waitUntilJobRunning(Duration.ofSeconds(30));
        LOG.info("Pipeline job is running");

        validateSinkResult(
                databaseName,
                "products",
                7,
                Arrays.asList(
                        "101 | scooter | Small 2-wheel scooter | 3.14 | red | {\"key1\": \"value1\"} | {\"coordinates\":[1,1],\"type\":\"Point\",\"srid\":0}",
                        "102 | car battery | 12V car battery | 8.1 | white | {\"key2\": \"value2\"} | {\"coordinates\":[2,2],\"type\":\"Point\",\"srid\":0}",
                        "103 | 12-pack drill bits | 12-pack of drill bits with sizes ranging from #40 to #3 | 0.8 | red | {\"key3\": \"value3\"} | {\"coordinates\":[3,3],\"type\":\"Point\",\"srid\":0}",
                        "104 | hammer | 12oz carpenter's hammer | 0.75 | white | {\"key4\": \"value4\"} | {\"coordinates\":[4,4],\"type\":\"Point\",\"srid\":0}",
                        "105 | hammer | 14oz carpenter's hammer | 0.875 | red | {\"k1\": \"v1\", \"k2\": \"v2\"} | {\"coordinates\":[5,5],\"type\":\"Point\",\"srid\":0}",
                        "106 | hammer | 16oz carpenter's hammer | 1.0 | null | null | null",
                        "107 | rocks | box of assorted rocks | 5.3 | null | null | null",
                        "108 | jacket | water resistent black wind breaker | 0.1 | null | null | null",
                        "109 | spare tire | 24 inch spare tire | 22.2 | null | null | null"));

        validateSinkResult(
                databaseName,
                "customers",
                4,
                Arrays.asList(
                        "101 | user_1 | Shanghai | 123567891234",
                        "102 | user_2 | Shanghai | 123567891234",
                        "103 | user_3 | Shanghai | 123567891234",
                        "104 | user_4 | Shanghai | 123567891234"));
    }

    public void createElasticsearchDatabase(String databaseName) throws IOException {
        client.indices().create(c -> c.index(databaseName));
    }

    public void dropElasticsearchDatabase(String databaseName) throws IOException {
        client.indices().delete(d -> d.index(databaseName));
    }

    private void validateSinkResult(
            String databaseName, String tableName, int columnCount, List<String> expected)
            throws Exception {
        waitAndVerify(
                databaseName,
                expected,
                DEFAULT_RESULT_VERIFY_TIMEOUT.toMillis(),
                true);
    }

    private void waitAndVerify(
            String databaseName,
            List<String> expected,
            long timeoutMilliseconds,
            boolean inAnyOrder)
            throws Exception {
        long deadline = System.currentTimeMillis() + timeoutMilliseconds;
        while (System.currentTimeMillis() < deadline) {
            try {
                List<String> actual = fetchTableContent(databaseName);
                if (inAnyOrder) {
                    if (expected.stream()
                            .sorted()
                            .collect(Collectors.toList())
                            .equals(actual.stream().sorted().collect(Collectors.toList()))) {
                        return;
                    }
                } else {
                    if (expected.equals(actual)) {
                        return;
                    }
                }
                LOG.info(
                        "Executing {}:: didn't get expected results.\nExpected: {}\n  Actual: {}",
                        databaseName,
                        expected,
                        actual);
            } catch (SQLSyntaxErrorException t) {
                LOG.info("Database {} isn't ready yet. Waiting for the next loop...", databaseName);
            }
            Thread.sleep(1000L);
        }
        fail(String.format("Failed to verify content of %s.", databaseName));
    }

    private List<String> fetchTableContent(String databaseName) throws Exception {

        List<String> results = new ArrayList<>();

        SearchRequest searchRequest =
                SearchRequest.of(s -> s.index(databaseName).query(q -> q.matchAll(m -> m)));
        SearchResponse<Map> response = client.search(searchRequest, Map.class);
        // 执行搜索请求
        SearchResponse<Map> searchResponse = client.search(searchRequest, Map.class);

        // 获取搜索结果
        List<Hit<Map>> hits = searchResponse.hits().hits();

        // 打印每个文档
        for (Hit<Map> hit : hits) {
            Map map = hit.source();
            List<String> columns = new ArrayList<>();
            Iterator<Map.Entry<String, String>> itr = map.entrySet().iterator();
            while (itr.hasNext()) {
                Map.Entry<String, String> entry = itr.next();
                System.out.println("Key = " + entry.getKey() + ", Value = " + entry.getValue());
                columns.add(entry.getValue());
            }

            results.add(String.join(" | ", columns));
        }

        return results;
    }

    private ElasticsearchClient createElasticsearchClient() {
        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(
                AuthScope.ANY, new UsernamePasswordCredentials(DEFAULT_USERNAME, DEFAULT_PASSWORD));
        RestClientTransport transport =
                new RestClientTransport(
                        RestClient.builder(
                                        new HttpHost(
                                                ELASTICSEARCH.getHost(),
                                                ELASTICSEARCH.getFirstMappedPort(),
                                                "http"))
                                .setHttpClientConfigCallback(
                                        new RestClientBuilder.HttpClientConfigCallback() {
                                            @Override
                                            public HttpAsyncClientBuilder customizeHttpClient(
                                                    HttpAsyncClientBuilder httpAsyncClientBuilder) {
                                                return httpAsyncClientBuilder
                                                        .setDefaultCredentialsProvider(
                                                                credentialsProvider);
                                            }
                                        })
                                .build(),
                        new JacksonJsonpMapper());
        return new ElasticsearchClient(transport);
    }
}
