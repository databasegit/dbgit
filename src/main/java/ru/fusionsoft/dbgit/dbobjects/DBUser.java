package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBUser extends DBOptionsObject {
	public DBUser() {}
	
	public DBUser(String name) {
		super(name);
	}

	public DBUser(String name, StringProperties options) { super(name, options); }
}
