package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

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
