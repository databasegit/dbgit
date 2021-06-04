package ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit;

import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class CharsDbGitConfigDefault extends CharSequenceEnvelope {
    public CharsDbGitConfigDefault() {
        super(()->{
            return "[core]\n"
                   + "MAX_ROW_COUNT_FETCH = 10000\n"
                   + "LIMIT_FETCH = true\n"
                   + "LOG_ROTATE = 31\n"
                   + "LANG = ENG\n"
                   + "SCRIPT_ROTATE = 31\n"
                   + "TO_MAKE_BACKUP = false\n"
                   + "BACKUP_TO_SCHEME = true\n"
                   + "BACKUP_TABLEDATA = true\n"
                   + "PORTION_SIZE = 50000\n"
                   + "TRY_COUNT = 1000\n"
                   + "TRY_DELAY = 10\n";
        });
    }
}
