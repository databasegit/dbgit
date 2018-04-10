package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreTablePostgres extends DBRestoreAdapter {

	@Override
	public void restoreMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub

	}
	
	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		
		try {
			
			MetaTable tblMeta = (MetaTable)obj;
			DBTable tbl = tblMeta.getTable();
			
			st.execute("DROP TABLE "+tbl.getSchema()+"."+tbl.getName());
		
			// TODO Auto-generated method stub
		} catch (Exception e) {
			throw new ExceptionDBGitRestore("Error remove "+obj.getName(), e);
		} finally {
			st.close();
		}
	}

}
