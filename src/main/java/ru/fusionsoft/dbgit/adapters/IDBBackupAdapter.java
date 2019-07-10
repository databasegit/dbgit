package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public interface IDBBackupAdapter {
	
	public static final String PREFIX = "BACKUP$";	
	
	public void setAdapter(IDBAdapter adapter);

	public IDBAdapter getAdapter();
	
	public IMetaObject backupDBObject(IMetaObject obj) throws Exception;
	
	public void restoreDBObject(IMetaObject obj) throws Exception;
	
	public boolean isToSaveData();
	
	public void setToSaveData(boolean toSaveData);
	
	public boolean createSchema(StatementLogging stLog, String schema);

	public void saveToSchema(boolean saveToSchema);
	
	public boolean isSaveToSchema();
}
