package ru.fusionsoft.dbgit.postgres;
import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.statement.StatementLogging;


public class DBRestoreSchemaPostgres extends DBRestoreAdapter {
	@Override
	public void restoreMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if (obj instanceof MetaSchema) {
				MetaSchema sch = (MetaSchema)obj;
				st.execute("CREATE SCHEMA "+sch.getObjectOption().getName());
				}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: cast to MetaSchema failed.");
			}			
		} catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}
	}
	
	public void removeMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
	}

}
