package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import java.nio.file.Path;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharsOf;
import ru.fusionsoft.dbgit.integration.primitives.path.specific.CurrentWorkingDirectory;

public class ArgsDbGitClone extends ArgsExplicit {
    public ArgsDbGitClone(CharSequence repoUrl, CharSequence directoryToCloneToChars) {
        super("clone", repoUrl, "--directory", "\"" + directoryToCloneToChars + "\"");
    }
    public ArgsDbGitClone(CharSequence repoUrl, Path directoryToCloneTo) {
        this(repoUrl, new CharsOf<>(()->directoryToCloneTo.toAbsolutePath().toString()));
    }
    public ArgsDbGitClone(CharSequence repoUrl) {
        this(repoUrl, ".");
    }
}
