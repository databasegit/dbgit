package ru.fusionsoft.dbgit.postgres;
import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.StringProperties;


public class DBRestoreSchemaPostgres extends DBRestoreAdapter {
	@Override
	public void restoreMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if (obj instanceof MetaSchema) {
				MetaSchema changedsch = (MetaSchema)obj;								
				Map<String, DBSchema> schs = adapter.getSchemes();
				boolean exist = false;
				for(DBSchema sch:schs.values()) {
					if(changedsch.getObjectOption().getName().equals(sch.getName())){
						exist = true;
						st.execute("ALTER SCHEMA "+ changedsch.getObjectOption().getName() +" OWNER TO "+ 
						changedsch.getObjectOption().getOptions().getChildren().get("usename").getData());
							
					}
				}
				if(!exist){
					String schemaOwner = changedsch.getObjectOption().getOptions().getChildren().get("usename").getData();
					st.execute("CREATE SCHEMA "+changedsch.getObjectOption().getName() +" AUTHORIZATION "+ 
					changedsch.getObjectOption().getOptions().getChildren().get("usename").getData());
				}
				else {
					
				}
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
