package ru.fusionsoft.dbgit.integration.primitives.path.specific.dbgit.scenarios;

import java.io.PrintStream;
import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.patch.specific.PathPatchDbGitRestoreFromDbToDb;
import ru.fusionsoft.dbgit.integration.primitives.path.PathPatched;
import ru.fusionsoft.dbgit.integration.primitives.printstream.DefaultPrintStream;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitLink;
import ru.fusionsoft.dbgit.integration.primitives.args.specific.ArgsDbGitRestore;

public class PathAfterDbGitRestoreFromDbToDb extends PathPatched {
    public PathAfterDbGitRestoreFromDbToDb(
        ArgsDbGitLink sourceDbLinkArgs, 
        ArgsDbGitLink targetDbLinkArgs, 
        CharSequence dbIgnoreChars, 
        ArgsDbGitRestore restoreArgs,
        PrintStream printStream,
        Path workingDirectory
    ) {
        super(
            new PathPatchDbGitRestoreFromDbToDb(
              sourceDbLinkArgs, targetDbLinkArgs, dbIgnoreChars, restoreArgs, printStream
            ),
            workingDirectory
        );
    }

    public PathAfterDbGitRestoreFromDbToDb(
        ArgsDbGitLink sourceDbLinkArgs,
        ArgsDbGitLink targetDbLinkArgs,
        CharSequence dbIgnoreChars,
        ArgsDbGitRestore restoreArgs,
        Path workingDirectory
    ) {
        this(
            sourceDbLinkArgs,
            targetDbLinkArgs,
            dbIgnoreChars,
            restoreArgs,
            new DefaultPrintStream(),
            workingDirectory
        );

    }
    
    public PathAfterDbGitRestoreFromDbToDb(
        ArgsDbGitLink sourceDbLinkArgs,
        ArgsDbGitLink targetDbLinkArgs,
        CharSequence dbIgnoreChars,
        Path workingDirectory
    ) {
        this(
            sourceDbLinkArgs, 
            targetDbLinkArgs, 
            dbIgnoreChars, 
            new ArgsDbGitRestore("-r", "-v"), 
            new DefaultPrintStream(), 
            workingDirectory
        );
    }
}
