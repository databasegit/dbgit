package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

/**
 * Base class for all objects where meta info use sql
 * @author mikle
 *
 */
public class DBSQLObject extends DBSchemaObject {
	
	private String sql;
	
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(schema);
		ch.addData(name);
		ch.addData(sql);
		
		return ch.calcHashStr();
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
	
	
	
}
