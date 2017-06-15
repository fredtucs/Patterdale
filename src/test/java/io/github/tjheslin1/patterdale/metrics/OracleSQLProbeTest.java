package io.github.tjheslin1.patterdale.metrics;

import io.github.tjheslin1.patterdale.database.DBConnection;
import io.github.tjheslin1.patterdale.database.DBConnectionPool;
import org.assertj.core.api.WithAssertions;
import org.junit.Test;
import org.slf4j.Logger;
import testutil.WithMockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.function.Function;

public class OracleSQLProbeTest implements WithAssertions, WithMockito {

    private static final String SQL = "SQL";
    private static final String TEST_MESSAGE = "testMessage";

    private final Function<ResultSet, ProbeResult> sqlResultCheck = (rs) -> ProbeResult.success(TEST_MESSAGE);

    private final ResultSet resultSet = mock(ResultSet.class);
    private final PreparedStatement preparedStatement = mock(PreparedStatement.class);
    private final Connection connection = mock(Connection.class);
    private final DBConnection dbConnection = mock(DBConnection.class);
    private final DBConnectionPool dbConnectionPool = mock(DBConnectionPool.class);
    private final Logger logger = mock(Logger.class);

    private final OracleSQLProbe oracleSQLProbe = new OracleSQLProbe(SQL, sqlResultCheck, dbConnectionPool, logger);

    @Test
    public void probeReturnsSuccess() throws Exception {
        when(resultSet.next()).thenReturn(true);
        when(resultSet.getInt(1)).thenReturn(1);
        when(preparedStatement.executeQuery()).thenReturn(resultSet);
        when(connection.prepareStatement(any())).thenReturn(preparedStatement);
        when(dbConnection.connection()).thenReturn(connection);
        when(dbConnectionPool.pool()).thenReturn(dbConnection);

        ProbeResult probeResult = oracleSQLProbe.probe();

        assertThat(probeResult).isEqualTo(ProbeResult.success(TEST_MESSAGE));
    }
}