package ru.fusionsoft.dbgit.integration.primitives.patch;

import java.io.PrintStream;
import java.sql.Connection;
import java.sql.Statement;
import java.text.MessageFormat;
import ru.fusionsoft.dbgit.integration.primitives.Patch;
import ru.fusionsoft.dbgit.integration.primitives.chars.LinesOfUnsafeScalar;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;

public class ConnectionPatchExecutingStatement implements Patch<Connection> {
    private final CharSequence sqlStatementChars;
    private final PrintStream printStream;

    public ConnectionPatchExecutingStatement(CharSequence sqlStatementChars, PrintStream printStream) {
        this.sqlStatementChars = sqlStatementChars;
        this.printStream = printStream;
    }
    
    public ConnectionPatchExecutingStatement(CharSequence sqlStatementChars) {
        this.sqlStatementChars = sqlStatementChars;
        this.printStream = new DefaultPrintStream();
    }

    @Override
    public final void apply(Connection connection) throws Exception {
        printStream.println(MessageFormat.format(
            "{0} # {1}",
            connection.getMetaData().getURL(),
            new LinesOfUnsafeScalar(sqlStatementChars).list().size() > 1 
                ? "\n" + sqlStatementChars
                : sqlStatementChars
        ));
        try (final Statement statement = connection.createStatement()) {
            statement.execute(String.valueOf(sqlStatementChars));
        }
    }
}
