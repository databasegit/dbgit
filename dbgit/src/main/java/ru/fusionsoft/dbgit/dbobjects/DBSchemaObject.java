package ru.fusionsoft.dbgit.dbobjects;

public abstract class DBSchemaObject implements IDBObject {
	protected String name;
	protected String schema;
		
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
		
}
