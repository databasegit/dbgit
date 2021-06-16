package ru.fusionsoft.dbgit.meta;

import java.util.regex.Pattern;

public class DbObjectNameInSqlPresence {
    private final CharSequence name;
    private final CharSequence sql;

    public DbObjectNameInSqlPresence(CharSequence name, CharSequence sql) {
        this.name = name;
        this.sql = sql;
    }
    
    public final boolean matches() {
        return Pattern
            .compile("\\b[a-zA-Z0-9\\.]?" + name)
            .matcher(sql)
            .find();
    }
}
