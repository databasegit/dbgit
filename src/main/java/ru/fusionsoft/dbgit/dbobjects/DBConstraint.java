package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.utils.CalcHash;

import java.util.Objects;

public class DBConstraint extends DBSQLObject {
	private String constraintType;
	
	public String getConstraintType() {
		return constraintType;
	}
	public void setConstraintType(String constraintType) {
		this.constraintType = constraintType;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;
		DBConstraint that = (DBConstraint) o;
		return Objects.equals(constraintType, that.constraintType)
			&& getName().equalsIgnoreCase(that.getName())
			&& getSchema().equalsIgnoreCase(that.getSchema())
			&& getSql().replaceAll("\\s+", "").equalsIgnoreCase(that.getSql().replaceAll("\\s+", ""));
//			&& getOptions().get("tablespace").getData().equalsIgnoreCase(that.getOptions().get("tablespace").getData());
	}

	public String getHash() {
		CalcHash ch = new CalcHash();
		ch.addData(getSchema());
		ch.addData(getName());
		ch.addData(getSql().replaceAll("\\s+", "").toLowerCase());
		//TODO !!!
		
		return ch.calcHashStr();
	}

}
