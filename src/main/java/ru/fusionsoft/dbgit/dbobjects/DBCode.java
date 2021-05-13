package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.StringProperties;

import java.util.Set;

/**
 * Base class for all Objects BD with code style 
 * @author mikle
 *
 */
public class DBCode extends DBSQLObject {
    public DBCode() {
    }

    public DBCode(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String sql) {
        super(name, options, schema, owner, dependencies, sql);
    }
}
