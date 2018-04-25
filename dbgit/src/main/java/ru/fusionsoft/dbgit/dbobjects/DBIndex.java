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
	
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		ch.addData(this.getOptions().toString());
		return ch.calcHashStr();
	}
	
}
