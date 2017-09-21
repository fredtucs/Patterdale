package io.github.tjheslin1.patterdale.config;

import com.fasterxml.jackson.databind.JsonMappingException;
import io.github.tjheslin1.patterdale.metrics.probe.DatabaseDefinition;
import io.github.tjheslin1.patterdale.metrics.probe.Probe;
import org.assertj.core.api.WithAssertions;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import testutil.WithMockito;

import java.io.File;
import java.io.FileWriter;
import java.net.URL;
import java.util.HashMap;
import java.util.List;

import static io.github.tjheslin1.patterdale.metrics.probe.DatabaseDefinition.databaseDefinition;
import static io.github.tjheslin1.patterdale.metrics.probe.Probe.probe;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;

public class ConfigUnmarshallerTest implements WithAssertions, WithMockito {

    private final Logger logger = mock(Logger.class);

    private final ConfigUnmarshaller configUnmarshaller = new ConfigUnmarshaller(logger);

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Test
    public void unmarshallConfigFileToPatterdaleConfig() throws Exception {
        File tempFile = temporaryFolder.newFile("patterdale.yml");
        FileWriter fileWriter = new FileWriter(tempFile);
        fileWriter.write(PATTERDALE_YML);
        fileWriter.flush();

        PatterdaleConfig patterdaleConfig = configUnmarshaller.parseConfig(tempFile);

        assertThat(patterdaleConfig.httpPort).isEqualTo(expectedConfig().httpPort);
        assertThat(patterdaleConfig.databases).isEqualTo(expectedConfig().databases);
        assertThat(patterdaleConfig.connectionPool).isEqualTo(expectedConfig().connectionPool);
    }

    @Test
    public void blowsUpIfPassWordsFileIsMalformed() throws Exception {
        File tempFile = temporaryFolder.newFile("patterdale.yml");
        FileWriter fileWriter = new FileWriter(tempFile);
        fileWriter.write("invalid content");
        fileWriter.flush();

        assertThatThrownBy(() -> configUnmarshaller.parseConfig(tempFile))
                .isExactlyInstanceOf(IllegalArgumentException.class)
                .hasMessage(format("Error occurred reading config file '%s'.", tempFile.getName()))
                .hasCauseExactlyInstanceOf(JsonMappingException.class);
    }

    @SuppressWarnings("ConstantConditions")
    private File loadTestConfigFile() {
        URL url = this.getClass().getClassLoader().getResource("patterdale.yml");
        return new File(url.getPath());
    }

    private PatterdaleConfig expectedConfig() {
        PatterdaleConfig expectedConfig = new PatterdaleConfig();

        expectedConfig.httpPort = 7001;
        expectedConfig.databases = new DatabaseDefinition[]{
                databaseDefinition(NAME, USER, JDBC_URL, PROBES),
                databaseDefinition(NAME_2, USER, JDBC_URL_2, PROBES_2)
        };

        HashMap<String, String> connectionPoolProperties = new HashMap<>();
        connectionPoolProperties.put("maxSize", "5");
        connectionPoolProperties.put("minIdle", "1");
        expectedConfig.connectionPool = connectionPoolProperties;

        return expectedConfig;
    }

    private static final String NAME = "test";
    private static final String NAME_2 = "test2";
    private static final String USER = "system";
    private static final String JDBC_URL = "jdbc:oracle:thin:@localhost:1522:xe";
    private static final String JDBC_URL_2 = "jdbc:oracle:thin:@localhost:1523:xe";
    private static final String EXISTS = "exists";
    private static final String LIST = "list";
    private static final String METRIC_NAME = "database_up";
    private static final String METRIC_LABELS = "database=\"myDB\",query=\"SELECT 1 FROM DUAL\"";
    private static final String METRIC_LABELS_2 = "database=\"myDB2\",query=\"SELECT 1 FROM DUAL\"";
    private static final String METRIC_LABELS_3 = "database=\"myDB2\",query=\"SELECT 2 FROM DUAL\"";
    private static final String METRIC_LABELS_4 = "database=\"myDB2\",slowQuery=\"%s\"";
    private static final String QUERY_SQL = "SELECT 1 FROM DUAL";
    private static final String QUERY_SQL_2 = "SELECT 2 FROM DUAL";
    private static final String QUERY_SQL_3 = "SELECT * FROM slowest_queries TOP 5";
    private static final List<Probe> PROBES = singletonList(probe(QUERY_SQL, EXISTS, METRIC_NAME, METRIC_LABELS));
    private static final List<Probe> PROBES_2 = asList(
            probe(QUERY_SQL, EXISTS, METRIC_NAME, METRIC_LABELS_2),
            probe(QUERY_SQL_2, EXISTS, METRIC_NAME, METRIC_LABELS_3),
            probe(QUERY_SQL_3, LIST, METRIC_NAME, METRIC_LABELS_4)
    );

    private static final String PATTERDALE_YML = "httpPort: 7001\n"+
            "databases:\n"+
            "  - name: test\n"+
            "    user: system\n"+
            "    jdbcUrl: jdbc:oracle:thin:@localhost:1522:xe\n"+
            "    probes:\n"+
            "      - query: SELECT 1 FROM DUAL\n"+
            "        type: exists\n"+
            "        metricName: database_up\n"+
            "        metricLabels: database=\"myDB\",query=\"SELECT 1 FROM DUAL\"\n"+
            "  - name: test2\n"+
            "    user: system\n"+
            "    jdbcUrl: jdbc:oracle:thin:@localhost:1523:xe\n"+
            "    probes:\n"+
            "      - query: SELECT 1 FROM DUAL\n"+
            "        type: exists\n"+
            "        metricName: database_up\n"+
            "        metricLabels: database=\"myDB2\",query=\"SELECT 1 FROM DUAL\"\n"+
            "      - query: SELECT 2 FROM DUAL\n"+
            "        type: exists\n"+
            "        metricName: database_up\n"+
            "        metricLabels: database=\"myDB2\",query=\"SELECT 2 FROM DUAL\"\n"+
            "      - query: SELECT * FROM slowest_queries TOP 5\n"+
            "        type: list\n"+
            "        metricName: database_up\n"+
            "        metricLabels: database=\"myDB2\",slowQuery=\"%s\"\n"+
            "connectionPool:\n"+
            "  maxSize: 5\n"+
            "  minIdle: 1";
}