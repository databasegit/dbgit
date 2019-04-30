package ru.fusionsoft.dbgit.dbobjects;

import java.sql.ResultSet;

public class DBTableData {	
	public static final int ERROR_LIMIT_ROWS = 1;
	
	protected int errorFlag = 0;
	protected ResultSet resultSet;
	
	public int getErrorFlag() {
		return errorFlag;
	}

	public void setErrorFlag(int errorFlag) {
		this.errorFlag = errorFlag;
	}

	

	public ResultSet getResultSet() {
		return resultSet;
	}

	public void setResultSet(ResultSet resultSet) {
		this.resultSet = resultSet;
	}
	
	
}
