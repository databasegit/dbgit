package ru.fusionsoft.dbgit.dbobjects;

public class DBConstraint extends DBSQLObject {
	private DBTable table;

	public DBTable getTable() {
		return table;
	}

	public void setTable(DBTable table) {
		this.table = table;
	}

}
