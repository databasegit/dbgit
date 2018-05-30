package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBView extends DBSQLObject {
	private StringProperties options = new StringProperties();
	public DBView(String name) {
		super();
		this.name = name;
	}
	public DBView() {
		super();
	}
	public StringProperties getOptions() {
		return options;
	}


	public void setOptions(StringProperties options) {
		this.options = options;
	}
}

