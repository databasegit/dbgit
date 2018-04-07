package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;

import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;

/**
 * <div class="en">The base adapter adapter class. Contains general solutions independent of a particular database</div>
 * <div class="ru">Базовый класс адаптера БД. Содержит общие решения, независимые от конкретной БД</div>
 * 
 * @author mikle
 *
 */
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
			getFactoryRestore().getAdapterRestore(obj.getType(), this).restoreMetaObject(obj);
		}
	}
	
	@Override
	public void deleteDataBase(IMapMetaObject deleteObjs) {
		for (IMetaObject obj : deleteObjs.values()) {
			getFactoryRestore().getAdapterRestore(obj.getType(), this).removeMetaObject(obj);
		}
	}
}
