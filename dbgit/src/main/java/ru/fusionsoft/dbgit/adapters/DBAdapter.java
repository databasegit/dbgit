package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;

public abstract class DBAdapter implements IDBAdapter {
	protected Connection connect;
	protected Boolean isExec = true;
	protected OutputStream streamSql = null;
	
	public void setConnection(Connection conn) {
		connect = conn;
	}
	
	public Connection getConnection() {
		return connect;
	} 
	
	public void setDumpSqlCommand(OutputStream stream, Boolean isExec) {
		this.streamSql = stream;
		this.isExec = isExec;
	}
	
}
