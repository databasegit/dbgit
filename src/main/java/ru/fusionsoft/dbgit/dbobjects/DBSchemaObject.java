package ru.fusionsoft.dbgit.dbobjects;


import java.util.HashSet;
import java.util.Set;

/**
 * Base class for database objects in scheme 
 * @author mikle
 *
 */
public abstract class DBSchemaObject implements IDBObject {
	protected String name;
	protected String schema;
	private Set<String> dependencies = new HashSet<>();
		
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

	public Set<String> getDependencies() {
		return dependencies;
	}
	public void setDependencies(Set<String> dependencies) {
		this.dependencies = dependencies;
	}
		
}
