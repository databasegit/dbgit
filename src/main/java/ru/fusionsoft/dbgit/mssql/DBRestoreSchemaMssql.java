package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSchema;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.util.Map;


public class DBRestoreSchemaMssql extends DBRestoreAdapter {
	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreSchema").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaSchema) {
				MetaSchema restoreSchema = (MetaSchema)obj;
				Map<String, DBSchema> schs = adapter.getSchemes();
				boolean exist = false;
				if(!(schs.isEmpty() || schs == null)) {
					for(DBSchema sch:schs.values()) {
						if(restoreSchema.getObjectOption().getName().equals(sch.getName())){
							exist = true;
							// TODO MSSQL restore Schema script
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
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}
		return true;
	}

	public void removeMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
	}

}
