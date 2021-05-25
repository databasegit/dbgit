package ru.fusionsoft.dbgit.integration.primitives.chars.specific;

import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class UrlOfGitTestRepo extends CharSequenceEnvelope {
    public UrlOfGitTestRepo() {
        super(()-> "https://github.com/rocket-3/dbgit-test.git");
    }
}
