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


public class DBRestoreSchemaPostgres extends DBRestoreAdapter {
	@Override
	public void restoreMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if (obj instanceof MetaSchema) {
				MetaSchema restoreSchema = (MetaSchema)obj;								
				Map<String, DBSchema> schs = adapter.getSchemes();
				boolean exist = false;
				if(!(schs.isEmpty() || schs == null)) {
					for(DBSchema sch:schs.values()) {
						if(restoreSchema.getObjectOption().getName().equals(sch.getName())){
							exist = true;
							//String test1 = changedsch.getObjectOption().getName();
							//String test2 = changedsch.getObjectOption().getOptions().getChildren().get("usename").getData();
							if(!restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData().equals(sch.getOptions().getChildren().get("usename").getData())) {
								st.execute("ALTER SCHEMA "+ restoreSchema.getObjectOption().getName() +" OWNER TO "+ 
								restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData());
							}
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					st.execute("CREATE SCHEMA "+restoreSchema.getObjectOption().getName() +" AUTHORIZATION "+ 
					restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData());
					//TODO Восстановление привилегий	
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore SCHEMAS.");
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
