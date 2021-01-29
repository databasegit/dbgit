package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTrigger;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreTriggerPostgres extends DBRestoreAdapter {


	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTrigger) {
				MetaTrigger restoreTrigger = (MetaTrigger)obj;
				Map<String, DBTrigger> trgs = adapter.getTriggers(restoreTrigger.getSqlObject().getSchema());
				boolean exist = false;
				if(!(trgs.isEmpty() || trgs == null)) {
					for(DBTrigger trg:trgs.values()) {
						if(restoreTrigger.getSqlObject().getName().equals(trg.getName())){
							exist = true;
							if(!restoreTrigger.getSqlObject().getSql().equals(trg.getSql())) {
								String query = "DROP TRIGGER IF EXISTS "+restoreTrigger.getSqlObject().getName()+" ON "+restoreTrigger.getSqlObject().getOptions().get("trigger_table")+";\n";
								query+=restoreTrigger.getSqlObject().getSql()+";";
								st.execute(query);
							}
							//TODO Восстановление привилегий
						}
					}
				}
				if(!exist){
					st.execute(restoreTrigger.getSqlObject().getSql());
					//TODO Восстановление привилегий
				}
			}
			else
			{
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "trigger", obj.getType().getValue()
                ));
			}




		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			st.close();
		}









		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if(! (obj instanceof MetaTrigger)) throw new ExceptionDBGit("Wrong IMetaObject type, expected: trg, was: " + obj.getType().getValue());
			MetaTrigger trgMeta = (MetaTrigger) obj;
			DBTrigger trg = (DBTrigger) trgMeta.getSqlObject();
			if (trg == null) return;

			String schema = getPhisicalSchema(trg.getSchema());
			st.execute("DROP FUNCTION IF EXISTS "+adapter.escapeNameIfNeeded(schema)+"."+adapter.escapeNameIfNeeded(trg.getName()));

		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

}
