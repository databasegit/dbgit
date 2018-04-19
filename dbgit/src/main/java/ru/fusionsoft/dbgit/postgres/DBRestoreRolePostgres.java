package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaRole;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreRolePostgres extends DBRestoreAdapter{

	@Override
	public void restoreMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if (obj instanceof MetaRole) {
				MetaRole role = (MetaRole)obj;
				st.execute("CREATE ROLE "+role.getObjectOption().getName());
				}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: cast to MetaRole failed.");
			}			
		} catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}
		
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub
		
	}

}
