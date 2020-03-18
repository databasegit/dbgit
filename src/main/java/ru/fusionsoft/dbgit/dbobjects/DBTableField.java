package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBTableField implements IDBObject, Comparable<DBTableField> {
	private String name;
	private String description;
	private String typeSQL;
	private FieldType typeUniversal;
	private int length;
	private int scale;
	private int precision;
	private boolean fixed;
	private Integer order = 0;
	private Boolean isNullable;
	private Boolean isNameExactly = false;
	private String defaultValue;	
	
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
		
		ch.addData(isPrimaryKey.toString());		

		return ch.calcHashStr();
	}

	public Boolean getIsNullable() { return isNullable; }

	public void setIsNullable(Boolean isNullable) { this.isNullable = isNullable; }

	public void setTypeUniversal(FieldType typeUniversal) {
		this.typeUniversal = typeUniversal;
	}
	
	public FieldType getTypeUniversal() {
		return typeUniversal;
	}
	
	public void setLength(int length) {
		this.length = length;
	}
	
	public int getLength() {			
		return length;
	}

	public void setScale(int scale) {
		this.scale = scale;
	}
	
	public int getScale() {
		return scale;
	}

	public void setPrecision(int precision) {
		this.precision = precision;
	}
	
	public int getPrecision() {
		return precision;
	}

	public void setFixed(boolean fixed) {
		this.fixed = fixed;
	}
	
	public boolean getFixed() {
		return fixed;
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

	public void setOrder(Integer order) {
		this.order = order;
	}
	
	public Integer getOrder() {
		return order;
	}
	
	public void setNameExactly(Boolean isNameExactly) {
		this.isNameExactly = isNameExactly;
	}
	
	public Boolean getNameExactly() {
		return isNameExactly;
	}
	
	public String getDefaultValue() {
		return this.defaultValue;
	}

	public void setDefaultValue(String defaultValue) {
		this.defaultValue = defaultValue;
	}

	public String getDescription() {
		return this.description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
	@Override
	public int compareTo(DBTableField o) {
		int res = - isPrimaryKey.compareTo(o.getIsPrimaryKey());
		if (res != 0) return res;
		return name.compareTo(o.getName());
	}
	

}
