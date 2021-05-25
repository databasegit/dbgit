package ru.fusionsoft.dbgit.integration.primitives.args.specific;

import ru.fusionsoft.dbgit.integration.primitives.args.ArgsExplicit;

public class ArgsDbGitAddRemote extends ArgsExplicit {
    public ArgsDbGitAddRemote(String url, String name, String user, String pass) {
        super(
            "remote", "add", name,
            "https://" +
            user +
            ":" +
            pass +
            "@" +
            url
        );
    }
}
