package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreSchemaOracle extends DBRestoreAdapter {
	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
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
							
							/*if(!restoreSchema.getObjectOption().getOptions().getChildren().get("owner").getData().equals(sch.getOptions().getChildren().get("owner").getData())) {
								st.execute("CREATE USER "+ restoreSchema.getObjectOption().getName() +" IDENTIFIED BY "+ 
								restoreSchema.getObjectOption().getOptions().getChildren().get("OWNER").getData());
							}*/
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					st.execute("CREATE USER " +
							restoreSchema.getObjectOption().getOptions().getChildren().get("owner").getData() 
							+ " IDENTIFIED BY "+ 
							setPassword(restoreSchema));
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
		return true;
	}
	
	public void removeMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
	}
	
	private String setPassword(MetaSchema restoreSchema) {
		return restoreSchema.getObjectOption().getName();
	}
}
