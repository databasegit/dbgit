package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.meta.MetaView;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreViewPostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaView) {
				MetaView restoreView = (MetaView)obj;								
				//Map<String, DBView> views = adapter.getViews(obj);
				boolean exist = false;
				/*if(!(schs.isEmpty() || schs == null)) {
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
					//st.execute("CREATE SCHEMA "+restoreSchema.getObjectOption().getName() +" AUTHORIZATION "+ 
					//restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData());
					//TODO Восстановление привилегий	
				}*/
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
		
		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
