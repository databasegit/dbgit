package ru.fusionsoft.dbgit.dbobjects;

import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.ExceptionDBGitTableData;

import java.io.Closeable;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class DBTableData implements AutoCloseable, Closeable {
	public static final int ERROR_LIMIT_ROWS = 1;
	
	final protected int errorFlag ;
	final protected ResultSet resultSet;
	final protected Statement statement;

	public DBTableData(Connection connection, String query) throws SQLException {
		this.errorFlag = 0;
		this.statement = connection.createStatement();
		this.resultSet = statement.executeQuery(query);
	}
	public DBTableData(int errorFlag) {
		this.errorFlag = errorFlag;
		this.resultSet = null;
		this.statement = null;
	}

	public int errorFlag() {
		return errorFlag;
	}
	public ResultSet resultSet() {

		return resultSet;
// 		TODO find usages and adapt to recover from the ExceptionDBGitTableData
//		if(resultSet != null){
//			return resultSet;
//		} else {
//			final String msg = DBGitLang.getInstance()
//				.getValue("errors", "dataTable", "errorTableData")
//				.withParams(String.valueOf(errorFlag));
//
//			throw new ExceptionDBGitTableData(msg, errorFlag);
//		}
	}


	@Override
	public void close() {
		try{
			this.resultSet.close();
			this.statement.close();
		} catch (SQLException ex){
		 	throw new ExceptionDBGitRunTime(ex);
		}
	}
}
