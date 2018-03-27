package ru.fusionsoft.dbgit.dbobjects;

public class DBTableField implements IDBObject {
	private String name;
	private DBTable table;

	public String getHash() {
		// TODO Auto-generated method stub
		return null;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public DBTable getTable() {
		return table;
	}

	public void setTable(DBTable table) {
		this.table = table;
	}
	
	

}
