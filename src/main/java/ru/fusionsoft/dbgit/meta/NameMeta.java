package ru.fusionsoft.dbgit.meta;

import java.text.MessageFormat;

public class NameMeta {
	private String schema;
	private String name;
	private IDBGitMetaType type;
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
	public IDBGitMetaType getType() {
		return type;
	}
	public void setType(IDBGitMetaType type) {
		this.type = type;
	}
	public NameMeta(){};
	public NameMeta(String metaName){
		schema = metaName.substring(0, metaName.indexOf("/"));
		name = metaName.substring(metaName.indexOf("/") + 1, metaName.lastIndexOf("."));
		type = DBGitMetaType.valueByCode(metaName.substring(metaName.lastIndexOf(".") + 1));
	}
	public NameMeta(String schema, String name, DBGitMetaType type){
		setSchema(schema);
		setName(name);
		setType(type);
	}
	public NameMeta(IMetaObject imo){
		this(imo.getName());
	}
	public String getMetaName(){
		return MessageFormat.format("{0}/{1}.{2}", schema, name, type.getValue());
	}
}
