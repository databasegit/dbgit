package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.util.Set;

public class DBIndex extends DBSQLObject {

	public DBIndex() {

	}
	public DBIndex(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String sql) {
		super(name, options, schema, owner, dependencies, sql);
	}
}
