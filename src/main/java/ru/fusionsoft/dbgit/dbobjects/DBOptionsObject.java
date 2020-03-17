package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBOptionsObject implements IDBObject {
	private String name;	
	private StringProperties options = new StringProperties();
	
	public DBOptionsObject() {}

	public DBOptionsObject(String name) {
		this.setName(name);
	}

	public DBOptionsObject(String name, StringProperties options) {
		this.setName(name);
		this.setOptions(options);
	}
	
	public StringProperties getOptions() {
		return options;
	}
	
	public void setOptions(StringProperties opt) {
		options = opt;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		ch.addData(this.getOptions().toString());

		return ch.calcHashStr();
	}	
}
