package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

/**
 * Base class for all objects where meta info use sql
 * @author mikle
 *
 */
public class DBSQLObject extends DBSchemaObject {
	
	protected String sql;
	//protected String owner;
	
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(getSchema());
		ch.addData(getName());
		ch.addData(getSql());
		//ch.addData(getOwner());
		
		return ch.calcHashStr();
	}

	public String getSql() {
		return sql;
	}

	public void setSql(String sql) {
		this.sql = sql;
	}
	/*public String getOwner() {
		return owner;
	}
	
	public void setOwner(String owner) {
		this.owner=owner;
	}*/
	
	
}
