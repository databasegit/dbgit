package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

public class DBConstraint extends DBSQLObject {
	private String constraintDef; 
	private String constraintType;
	public String getConstraintDef() {
		return constraintDef;
	}
	public void setConstraintDef(String constraintDef) {
		this.constraintDef = constraintDef;
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
