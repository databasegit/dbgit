package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBIndex extends DBSQLObject {
	//private DBTable table;
	private StringProperties options = new StringProperties();
	public DBIndex() {
		super();
	}
	public DBIndex(String name) {
		super();
		this.name = name;
	}


	public StringProperties getOptions() {
		return options;
	}

	public void setOptions(StringProperties options) {
		this.options = options;
	}

	public String getSql() {
		return options.get("ddl") != null ? options.get("ddl").toString() : "";
	}

	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(getSchema());
		ch.addData(getName());
		ch.addData(getSql().replaceAll("\\s+", "").toLowerCase());
		return ch.calcHashStr();
	}

}
