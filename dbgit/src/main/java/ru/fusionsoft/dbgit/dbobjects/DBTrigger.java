package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBTrigger extends DBCode {
	private StringProperties options = new StringProperties();
	public DBTrigger() {
		
	}
	
	public DBTrigger(String name) {
		this.name = name;
	}
	public StringProperties getOptions() {
		return options;
	}


	public void setOptions(StringProperties options) {
		this.options = options;
	}
}
