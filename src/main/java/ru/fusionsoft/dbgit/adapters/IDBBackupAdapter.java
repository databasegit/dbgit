package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.NameMeta;
import ru.fusionsoft.dbgit.statement.StatementLogging;

import java.sql.SQLException;

public interface IDBBackupAdapter {
	
	public static final String PREFIX = "BACKUP$";	
	
	public void setAdapter(IDBAdapter adapter);

	public IDBAdapter getAdapter();
	
	public IMetaObject backupDBObject(IMetaObject obj) throws  Exception;

	public void backupDatabase(IMapMetaObject backupObjs) throws Exception;

	public void restoreDBObject(IMetaObject obj) throws Exception;
	
	public boolean isToSaveData();
	
	public void setToSaveData(boolean toSaveData);
	
	public boolean createSchema(StatementLogging stLog, String schema);

	public void saveToSchema(boolean saveToSchema);
	
	public boolean isSaveToSchema();
	
	public boolean isExists(String owner, String objectName) throws SQLException;

	public void dropIfExists(String owner, String objectName, StatementLogging stLog) throws SQLException;

	public void dropIfExists(IMetaObject imo, StatementLogging stLog) throws SQLException, Exception;
}
