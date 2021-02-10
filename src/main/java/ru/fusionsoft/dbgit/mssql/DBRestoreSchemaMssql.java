package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
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

							if(!DBGitConfig.getInstance().getToIgnoreOnwer(false)){
								if(!restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData().equals(sch.getOptions().getChildren().get("usename").getData())) {
								st.execute("ALTER SCHEMA "+ restoreSchema.getObjectOption().getName() +" OWNER TO "+
										restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData());
								}
							}
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
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
					obj.getName()
					,  "schema", obj.getType().getValue()
				));
			}
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			st.close();
		}
		return true;
	}

	public void removeMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
	}

}
