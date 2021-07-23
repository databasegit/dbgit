package ru.fusionsoft.dbgit.integration.primitives.chars.specific.dbgit;

import ru.fusionsoft.dbgit.integration.primitives.chars.CharSequenceEnvelope;

public class CharsDbIgnoreWithTypes extends CharSequenceEnvelope {
    public CharsDbIgnoreWithTypes() {
        super(()->{
            return "*\n" +
                   "!public/*.ts\n" +
                   "!public/*.sch\n" +
                   "!public/*.seq\n" +
                   "!public/*.tbl\n" +
                   "!public/*.pkg\n" +
                   "!public/*.trg\n" +
                   "!public/*.prc\n" +
                   "!public/*.fnc\n" +
                   "!public/*.vw\n" +
                   "!public/*.blob\n" +
                   "!public/*.udt\n" +
                   "!public/*.enum\n" +
                   "!public/*.domain\n";
        });
    }
}
