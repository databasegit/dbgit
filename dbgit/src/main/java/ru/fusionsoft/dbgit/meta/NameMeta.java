package ru.fusionsoft.dbgit.meta;

public class NameMeta {
	private String schema;
	private String name;
	private DBGitMetaType type;
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public String getName() {
		return name;
	}
	public void setName(String name) {
		this.name = name;
	}
	public DBGitMetaType getType() {
		return type;
	}
	public void setType(DBGitMetaType type) {
		this.type = type;
	}
	
	
}
