package ru.fusionsoft.dbgit.dbobjects;

public abstract class DBSchemaObject implements IDBObject {
	protected String name;
	protected DBSchema schema;
		
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public DBSchema getSchema() {
		return schema;
	}
	public void setSchema(DBSchema schema) {
		this.schema = schema;
	}
		
}
