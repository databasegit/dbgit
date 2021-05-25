package ru.fusionsoft.dbgit.integration.primitives.chars;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.CredsOfGitTestRepo;
import ru.fusionsoft.dbgit.integration.primitives.credentials.specific.CredsOfPgTestDatabase;

public class CharsWithMaskedCredentials extends CharSequenceEnvelope {
    public CharsWithMaskedCredentials(CharSequence origin) {
        super(() -> {
            final List<String> replacementsList = new ArrayList<>();
            try {
                replacementsList.add(new CredsOfGitTestRepo().password());
            } catch (Throwable e) { }
            try {
                replacementsList.add(new CredsOfPgTestDatabase().password());
            } catch (Throwable e) { }
            
            return String
            .valueOf(origin)
            .replaceAll(
                replacementsList
                .stream()
                .map(CharsQuotedToRegexPattern::new)
                .collect(Collectors.joining("|")), 
                "****"
            );
        });
    }
}
