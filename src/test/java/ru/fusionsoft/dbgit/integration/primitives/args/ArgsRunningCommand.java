package ru.fusionsoft.dbgit.integration.primitives.args;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOfLines;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsWithBoundary;

public class ArgsRunningCommand extends ArgsExplicit {
    public ArgsRunningCommand(CharSequence commandName) {
        super(
            ( System.getenv("ComSpec") == null )
            ? new CharSequence[]{commandName}
            : new CharSequence[]{System.getenv("ComSpec"), "/C", commandName}
        );
    }
    public ArgsRunningCommand(Args commandRunArgs) {
        super(
            ( System.getenv("ComSpec") == null )
                ? new CharSequence[]{"sh", "-c", new CharsWithBoundary(new CharsOfLines(commandRunArgs), "\"")}
                : new CharSequence[]{System.getenv("ComSpec"), "/C", new CharsOfLines(commandRunArgs)}
        );
    }
    public ArgsRunningCommand(Path executablePath) {
        this(
            new CharsOf<>(
                Object::toString,
                executablePath
            )
        );
    } 

}
