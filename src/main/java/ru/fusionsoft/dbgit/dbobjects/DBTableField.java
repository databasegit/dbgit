package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

public class DBTableField implements IDBObject, Comparable<DBTableField> {
	private String name;
	private String typeSQL;
	private String typeMapping;
	
	private Boolean isPrimaryKey = false;

	public Boolean getIsPrimaryKey() {
		return isPrimaryKey;
	}

	public void setIsPrimaryKey(Boolean isPrimaryKey) {
		this.isPrimaryKey = isPrimaryKey;
	}

	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(this.getName());
		ch.addData(this.getTypeSQL());
		if (this.getTypeMapping() != null) {
			ch.addData(this.getTypeMapping());
		}
		ch.addData(isPrimaryKey.toString());

		return ch.calcHashStr();
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public String getTypeSQL() {
		return typeSQL;
	}

	public void setTypeSQL(String typeSQL) {
		this.typeSQL = typeSQL;
	}


	public String getTypeMapping() {
		return typeMapping;
	}

	public void setTypeMapping(String typeMapping) {
		this.typeMapping = typeMapping;
	}

	@Override
	public int compareTo(DBTableField o) {
		int res = - isPrimaryKey.compareTo(o.getIsPrimaryKey());
		if (res != 0) return res;
		return name.compareTo(o.getName());
	}
	

}
