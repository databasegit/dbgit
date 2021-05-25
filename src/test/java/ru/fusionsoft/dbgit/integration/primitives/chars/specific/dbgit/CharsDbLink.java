package ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit;

import java.text.MessageFormat;
import ru.fusionsoft.dbgit.integration.primitives.Credentials;
import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class CharsDbLink extends CharSequenceEnvelope {
    public CharsDbLink(String url, String catalog, Credentials credentials) {
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
