package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.util.HashSet;
import java.util.Set;

/**
 * Base class for all objects where meta info use sql
 * @author mikle
 *
 */
public class DBSQLObject extends DBSchemaObject {

	protected String sql;
	protected String owner;
	private StringProperties options = new StringProperties();

	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(getSchema());
		ch.addData(getName());
		ch.addData(getSql().trim().replaceAll("\\s+", ""));
		if (getOwner() != null)
			ch.addData(getOwner());

		return ch.calcHashStr();
	}

	public String getSql() {
		return options.get("ddl") != null ? options.get("ddl").toString() : "";
	}
	public void setSql(String ddl) { options.get("ddl").setData(ddl); }

	public String getOwner() {
		return owner;
	}

	public void setOwner(String owner) {
		this.owner=owner;
	}
	public StringProperties getOptions() {
		return options;
	}

	public void setOptions(StringProperties options) {
		this.options = options;
	}

}
