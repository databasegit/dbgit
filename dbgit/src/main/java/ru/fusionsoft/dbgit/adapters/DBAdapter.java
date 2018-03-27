package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;

import javax.xml.validation.meta.IMapMetaObject;
import javax.xml.validation.meta.IMetaObject;

public abstract class DBAdapter implements IDBAdapter {
	protected Connection connect;
	protected Boolean isExec = true;
	protected OutputStream streamSql = null;
	
	@Override
	public void setConnection(Connection conn) {
		connect = conn;
	}
	
	@Override
	public Connection getConnection() {
		return connect;
	} 
	
	@Override
	public void setDumpSqlCommand(OutputStream stream, Boolean isExec) {
		this.streamSql = stream;
		this.isExec = isExec;
	}
	
	@Override
	public void restoreDataBase(IMapMetaObject updateObjs) {
		for (IMetaObject obj : updateObjs.values()) {
			getFactoryRestore().getAdapterRestore(obj.getType(), getConnection()).restoreMetaObject(obj);
		}
	}
}
