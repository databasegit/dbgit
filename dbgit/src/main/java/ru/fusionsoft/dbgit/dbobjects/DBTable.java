package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBTable extends DBSchemaObject {
	private StringProperties options = new StringProperties();
	
	

	public StringProperties getOptions() {
		return options;
	}



	public void setOptions(StringProperties options) {
		this.options = options;
	}



	public String getHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
