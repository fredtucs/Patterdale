/*
 * Copyright 2017 - 2021 Thomas Heslin <tjheslin1@kolabnow.com>.
 *
 * This file is part of Patterdale.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.github.tjheslin1.patterdale;

import io.github.tjheslin1.patterdale.config.*;
import io.github.tjheslin1.patterdale.database.DBConnectionPool;
import io.github.tjheslin1.patterdale.database.HikariDataSourceProvider;
import io.github.tjheslin1.patterdale.database.hikari.H2DataSourceProvider;
import io.github.tjheslin1.patterdale.database.hikari.HikariDBConnection;
import io.github.tjheslin1.patterdale.database.hikari.HikariDBConnectionPool;
import io.github.tjheslin1.patterdale.database.hikari.OracleDataSourceProvider;
import io.github.tjheslin1.patterdale.http.WebServer;
import io.github.tjheslin1.patterdale.http.jetty.JettyWebServerBuilder;
import io.github.tjheslin1.patterdale.metrics.MetricsUseCase;
import io.github.tjheslin1.patterdale.metrics.probe.DatabaseDefinition;
import io.github.tjheslin1.patterdale.metrics.probe.OracleSQLProbe;
import io.github.tjheslin1.patterdale.metrics.probe.Probe;
import io.github.tjheslin1.patterdale.metrics.probe.TypeToProbeMapper;
import io.prometheus.client.CollectorRegistry;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static io.github.tjheslin1.patterdale.config.PatterdaleRuntimeParameters.patterdaleRuntimeParameters;
import static io.github.tjheslin1.patterdale.infrastructure.RegisterExporters.serverWithStatisticsCollection;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;

public class Patterdale {

    private final HikariDataSourceProvider dataSourceProvider;
    private final RuntimeParameters runtimeParameters;
    private final Map<String, Future<DBConnectionPool>> connectionPools;
    private final TypeToProbeMapper typeToProbeMapper;
    private final Map<String, Probe> probesByName;
    private final Logger logger;
    private WebServer webServer;

    public Patterdale(HikariDataSourceProvider dataSourceProvider, RuntimeParameters runtimeParameters, Map<String, Future<DBConnectionPool>> connectionPools, TypeToProbeMapper typeToProbeMapper, Map<String, Probe> probesByName, Logger logger) {
        this.dataSourceProvider = dataSourceProvider;
        this.runtimeParameters = runtimeParameters;
        this.connectionPools = connectionPools;
        this.typeToProbeMapper = typeToProbeMapper;
        this.probesByName = probesByName;
        this.logger = logger;
    }

    public static void main(String[] args) {

        Options options = new Options();
        Option configApp = new Option("c", "config.file", true, "Config File");
        configApp.setRequired(true);
        options.addOption(configApp);
        Option passwordFile = new Option("p", "passwords.file", true, "Passwords File");
        passwordFile.setRequired(true);
        options.addOption(passwordFile);
        Option logbackFile = new Option("lg", "logback.file", true, "Logback File");
        logbackFile.setRequired(false);
        options.addOption(logbackFile);
        Option statusPage = new Option("sp", "status.page", true, "Status Page");
        statusPage.setRequired(false);
        options.addOption(statusPage);

        HelpFormatter formatter = new HelpFormatter();
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            System.out.println(e.getMessage());
            formatter.printHelp("Application arguments info", options);
            System.exit(1);
            return;
        }

        if (cmd.hasOption("config.file")) {
            System.setProperty("config.file", cmd.getOptionValue("config.file"));
        }
        if (cmd.hasOption("logback.file")) {
            System.setProperty("logback.configurationFile", cmd.getOptionValue("logback.file"));
        }
        if (cmd.hasOption("status.page")) {
            System.setProperty("status.page", cmd.getOptionValue("status.page"));
        }

        Logger logger = LoggerFactory.getLogger("application");

        PatterdaleConfig patterdaleConfig = new ConfigUnmarshaller(logger).parseConfig(new File(cmd.getOptionValue("config.file")));

        Passwords passwords = new PasswordsUnmarshaller(logger).parsePasswords(new File(cmd.getOptionValue("passwords.file")));

        RuntimeParameters runtimeParameters = patterdaleRuntimeParameters(patterdaleConfig);

        HikariDataSourceProvider dataSourceProvider = dataSourceProvider(runtimeParameters.databases());

        Map<String, Future<DBConnectionPool>> futureConnections = initialDatabaseConnections(dataSourceProvider, logger, passwords, runtimeParameters);

        Map<String, Probe> probesByName = runtimeParameters.probes().stream().collect(toMap(probe -> probe.name, probe -> probe));

        logger.debug("starting Patterdale!");
        new Patterdale(dataSourceProvider, runtimeParameters, futureConnections, new TypeToProbeMapper(logger), probesByName, logger)
                .start();
    }

    public void start() {
        logger.info("logback.configurationFile = " + System.getProperty("logback.configurationFile"));
        CollectorRegistry registry = new CollectorRegistry();

        List<OracleSQLProbe> probes = runtimeParameters.databases().stream()
                .flatMap(this::createProbes)
                .collect(toList());

        long cacheDuration = Math.max(runtimeParameters.cacheDuration(), 1);
        logger.info("Using database scrape cache duration of {} seconds.", cacheDuration);

        webServer = new JettyWebServerBuilder(logger)
                .withServer(serverWithStatisticsCollection(registry, runtimeParameters.httpPort(), runtimeParameters.httpHost()))
                .registerMetricsEndpoint(
                        "/metrics",
                        new MetricsUseCase(
                                logger,
                                probes,
                                runtimeParameters,
                                () -> Executors.newFixedThreadPool(probes.size())
                        ),
                        runtimeParameters,
                        registry,
                        cacheDuration
                )
                .build();

        try {
            webServer.start();
            logger.info("Web server started successfully at {}", webServer.baseUrl());
        } catch (Exception e) {
            logger.error("Error occurred starting Jetty Web Server.", e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                stop();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }));
    }

    public void stop() throws Exception {
        webServer.stop();
        logger.info("Shutting down.");
    }

    public static Map<String, Future<DBConnectionPool>> initialDatabaseConnections(HikariDataSourceProvider dataSourceProvider,
                                                                                   Logger logger,
                                                                                   Passwords passwords,
                                                                                   RuntimeParameters runtimeParameters) {
        ExecutorService executor = Executors.newFixedThreadPool(runtimeParameters.databases().size());
        return runtimeParameters.databases().stream()
                .collect(Collectors.toMap(databaseDefinition -> databaseDefinition.name,
                        databaseDefinition -> executor.submit(() -> connectionPool(dataSourceProvider, logger, passwords, runtimeParameters, databaseDefinition))));
    }

    private static HikariDataSourceProvider dataSourceProvider(List<DatabaseDefinition> databases) {
        if (databases.isEmpty())
            throw new IllegalArgumentException("No database definitions were provided, see `configuration.md`.");
        else if (databases.get(0).jdbcUrl.startsWith("jdbc:h2"))
            return new H2DataSourceProvider();
        else
            return new OracleDataSourceProvider();
    }

    private static HikariDBConnectionPool connectionPool(HikariDataSourceProvider dataSourceProvider, Logger logger,
                                                         Passwords passwords,
                                                         RuntimeParameters runtimeParameters,
                                                         DatabaseDefinition databaseDefinition) {
        return new HikariDBConnectionPool(new HikariDBConnection(
                dataSourceProvider.dataSource(runtimeParameters, databaseDefinition, passwords, logger)));
    }

    private Stream<OracleSQLProbe> createProbes(DatabaseDefinition databaseDefinition) {
        return Arrays.stream(databaseDefinition.probes)
                .map(probeName -> typeToProbeMapper.createProbe(databaseDefinition, connectionPools.get(databaseDefinition.name), lookupProbe(probeName), runtimeParameters));
    }

    public Probe lookupProbe(String probeName) throws IllegalArgumentException {
        Probe probe = probesByName.get(probeName);
        if (probe == null) {
            throw new IllegalArgumentException(format("Probe '%s' not defined", probeName));
        }

        return probe;
    }
}
