package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

public abstract class DBRestoreAdapter implements IDBAdapterRestoreMetaData {
	protected Connection connect = null;
	
	public void setConnection(Connection conn) {
		connect = conn;
	}
}
