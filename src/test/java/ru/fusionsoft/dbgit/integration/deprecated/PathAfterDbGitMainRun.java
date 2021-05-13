package ru.fusionsoft.dbgit.integration.deprecated;

import java.nio.file.Path;
import java.util.Arrays;
import ru.fusionsoft.dbgit.integration.primitives.Args;
import ru.fusionsoft.dbgit.integration.primitives.path.PathEnvelope;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class PathAfterDbGitMainRun extends PathEnvelope {
    public PathAfterDbGitMainRun(Args args, Path origin) {
        super(() -> {
            final boolean originIsNotAppWorkingDirectory = (
                ! new CurrentWorkingDirectory()
                .toAbsolutePath()
                .toString()
                .equals(origin.toAbsolutePath().toString())
            );
            if ( originIsNotAppWorkingDirectory ) {
                throw new RuntimeException(
                    "Given path:" 
                    + "\n->" + origin.toAbsolutePath().toString()
                    + "\nis not an App's working directory:"
                    + "\n->" + new CurrentWorkingDirectory().toAbsolutePath().toString()
                    + "\nSo App cant apply to the given Path"
                );
            }
            origin.toString();
            System.out.println("\n> dbgit " + String.join(
                " ",
                Arrays.asList(args.values())
            ));
            return new WithDbGitMainRunScalar<>(args, origin).value();
        });
    }
}
