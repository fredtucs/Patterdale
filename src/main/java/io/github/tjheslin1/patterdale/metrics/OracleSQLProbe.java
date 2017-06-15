/*
 * Copyright 2017 Thomas Heslin <tjheslin1@gmail.com>.
 *
 * This file is part of Patterdale-jvm.
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
package io.github.tjheslin1.patterdale.metrics;

import io.github.tjheslin1.patterdale.ValueType;
import io.github.tjheslin1.patterdale.database.DBConnectionPool;
import org.slf4j.Logger;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Function;

import static io.github.tjheslin1.patterdale.metrics.ProbeResult.failure;
import static java.lang.String.format;

public class OracleSQLProbe extends ValueType implements SQLProbe {

    private final String sql;
    private final Function<ResultSet, ProbeResult> sqlResultCheck;
    private final DBConnectionPool connectionPool;
    private final Logger logger;

    public OracleSQLProbe(String sql, Function<ResultSet, ProbeResult> sqlResultCheck, DBConnectionPool connectionPool, Logger logger) {
        this.sql = sql;
        this.sqlResultCheck = sqlResultCheck;
        this.connectionPool = connectionPool;
        this.logger = logger;
    }

    @Override
    public ProbeResult probe() {
        try (Connection connection = connectionPool.pool().connection();
             PreparedStatement preparedStatement = connection.prepareStatement(sql)) {
            ResultSet resultSet = preparedStatement.executeQuery();

            if (!resultSet.next()) {
                return failure(format("Did not receive a result from query '%s'", sql));
            }
            return sqlResultCheck.apply(resultSet);
        } catch (Exception e) {
            String message = format("Error occurred executing sql: '%s'", sql);
            logger.error(message, e);
            return failure(message);
        }
    }
}
