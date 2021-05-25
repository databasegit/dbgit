package ru.fusionsoft.dbgit.integration.primitives.args;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.path.PathRelativeTo;

public class ArgsRunningCommand extends ArgsExplicit {
    public ArgsRunningCommand(CharSequence commandName) {
        super(
            System.getenv("ComSpec"),
            "/C",
            commandName
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
