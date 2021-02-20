package ru.fusionsoft.dbgit.dbobjects;


import ru.fusionsoft.dbgit.utils.StringProperties;
import ru.fusionsoft.dbgit.yaml.YamlOrder;

import java.util.HashSet;
import java.util.Set;

/**
 * Base class for database objects in scheme 
 * @author mikle
 *
 */
public abstract class DBSchemaObject extends DBOptionsObject {

	@YamlOrder(1)
	protected String schema;

	@YamlOrder(2)
	protected String owner;

	@YamlOrder(3)
	private Set<String> dependencies;


	public DBSchemaObject(String name, StringProperties options, String schema, String owner, Set<String> dependencies) {
		super(name, options);
		this.schema = schema;
		this.owner = owner;
		this.dependencies = dependencies;
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

	public String getOwner() {
		return owner;
	}
	public void setOwner(String owner) {
		this.owner=owner;
	}
		
}
