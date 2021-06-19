package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.sql.Connection;
import java.sql.Statement;
import ru.fusionsoft.dbgit.integration.primitives.Patch;

public class ConnectionPatchExecutingStatement implements Patch<Connection> {
    private final CharSequence sqlStatementChars;

    public ConnectionPatchExecutingStatement(CharSequence sqlStatementChars) {
        this.sqlStatementChars = sqlStatementChars;
    }

    @Override
    public final void apply(Connection connection) throws Exception {
        try (final Statement statement = connection.createStatement()) {
            statement.execute(String.valueOf(sqlStatementChars));
        }
    }
}
