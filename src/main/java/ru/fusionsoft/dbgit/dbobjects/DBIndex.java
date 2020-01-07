package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBIndex extends DBSQLObject {
	//private DBTable table;
	public DBIndex() {
		super();
	}	
	public DBIndex(String name) {
		super();
		this.name = name;
	}

	
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		ch.addData(this.getOptions().toString());
		return ch.calcHashStr();
	}
	
}
