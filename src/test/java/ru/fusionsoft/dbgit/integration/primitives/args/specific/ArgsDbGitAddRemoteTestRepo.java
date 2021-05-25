package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.CredsOfGitTestRepo;

public class ArgsDbGitAddRemoteTestRepo extends ArgsDbGitAddRemote {
    public ArgsDbGitAddRemoteTestRepo(String url, String name, String user, String pass) {
        super(url, name, user, pass);
    }
    public ArgsDbGitAddRemoteTestRepo(String url, String name, Credentials credentials) {
        this(url, name, credentials.username(), credentials.password());
    }
    public ArgsDbGitAddRemoteTestRepo(String name) {
        this(
            "github.com/rocket-3/dbgit-test.git", 
            name, 
            new CredsOfGitTestRepo()
        );
    }    
    public ArgsDbGitAddRemoteTestRepo() {
        this("origin");
    }

}
