package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

public class DBSchema implements IDBObject {
	private String name;

	
	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}


	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(name);

		return ch.calcHashStr();
	}
	
}
