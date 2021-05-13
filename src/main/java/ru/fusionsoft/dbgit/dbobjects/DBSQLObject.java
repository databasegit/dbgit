package ru.fusionsoft.dbgit.dbobjects;

import java.util.Collections;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.util.Set;

/**
 * Base class for all objects where meta info use sql
 * @author mikle
 *
 */
public class DBSQLObject extends DBSchemaObject {

	protected String sql;

	public DBSQLObject() {
		super("", new StringProperties(), "", "", Collections.emptySet());
	}
	
	public DBSQLObject(String name, StringProperties options, String schema, String owner, Set<String> dependencies, String sql) {
		super(name, options, schema, owner, dependencies);
		this.sql = sql;
	}

	public String getHash() {
		CalcHash ch = new CalcHash();

		ch.addData(this.name);
		ch.addData(this.schema);
		ch.addData(this.owner);
		ch.addData(this.options.toString());
		ch.addData(this.sql);

		return ch.calcHashStr();
	}

	public String getSql() {
		return this.sql;
	}
	public void setSql(String ddl) {
		this.sql = ddl;
	}

}
