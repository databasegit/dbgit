package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.args.ArgsDbGitAddRemote;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.GitTestRepoCredentials;

public class GitTestRepoAddRemoteArgs extends ArgsDbGitAddRemote {
    public GitTestRepoAddRemoteArgs(String url, String name, String user, String pass) {
        super(url, name, user, pass);
    }
    public GitTestRepoAddRemoteArgs(String url, String name, Credentials credentials) {
        this(url, name, credentials.username(), credentials.password());
    }
    public GitTestRepoAddRemoteArgs(String name) {
        this(
            "https://github.com/rocket-3/dbgit-test.git", 
            name, 
            new GitTestRepoCredentials()
        );
    }
}
