package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.StringProperties;

import java.util.Set;

public class DBProcedure extends DBCode {

	public DBProcedure() { }
	public DBProcedure(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String sql) {
		super(name, options, schema, owner, dependencies, sql);
	}
}
