package ru.fusionsoft.dbgit.dbobjects;

import java.util.Set;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBDomain extends DBSQLObject {
    public DBDomain() {
    }

    public DBDomain(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String sql) {
        super(name, options, schema, owner, dependencies, sql);
    }
}
