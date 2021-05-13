package ru.fusionsoft.dbgit.integration.primitives.credentials;

import ru.fusionsoft.dbgit.integration.primitives.Credentials;

public class SimpleCredentials implements Credentials {

    private final String username;
    private final String password;

    public SimpleCredentials(String username, String password) {
        this.username = username;
        this.password = password;
    }

    @Override
    public final String username() {
        return this.username;
    }

    @Override
    public final String password() {
        return this.password;
    }
}
