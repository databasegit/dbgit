package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

public class DBConstraint extends DBSQLObject {
	private String columnName; 
	private String foreignTableName;
	private String foreignColumnName;
	private String constraintType;
	public String getColumnName() {
		return columnName;
	}
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	public String getForeignTableName() {
		return foreignTableName;
	}
	public void setForeignTableName(String foreignTableName) {
		this.foreignTableName = foreignTableName;
	}
	public String getForeignColumnName() {
		return foreignColumnName;
	}
	public void setForeignColumnName(String foreignColumnName) {
		this.foreignColumnName = foreignColumnName;
	}
	public String getConstraintType() {
		return constraintType;
	}
	public void setConstraintType(String constraintType) {
		this.constraintType = constraintType;
	}
	
	
	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(getSchema());
		ch.addData(getName());
		//ch.addData(getSql());
		//TODO !!!
		
		return ch.calcHashStr();
	}

}
