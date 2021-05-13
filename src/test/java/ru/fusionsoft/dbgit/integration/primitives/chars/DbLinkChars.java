package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.text.MessageFormat;
import ru.fusionsoft.dbgit.integration.primitives.Credentials;

public class DbLinkChars extends CharSequenceEnvelope {
    public DbLinkChars(String url, String catalog, Credentials credentials) {
        super(() -> {
            return
                MessageFormat.format(
                    "url=jdbc:postgresql://{0}/{1}\n" +
                    "user={2}\n" +
                    "password={3}\n"
                    , url
                    , catalog
                    , credentials.username()
                    , credentials.password()
                );
        });
    }
}
