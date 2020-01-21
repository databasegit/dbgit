package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaFunction;
import ru.fusionsoft.dbgit.meta.MetaTrigger;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

public class DBRestoreTriggerMssql extends DBRestoreAdapter {


	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreTrigger").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTrigger) {
				MetaTrigger restoreTrigger = (MetaTrigger)obj;
				DBSQLObject restoringDBT = restoreTrigger.getSqlObject();
				String triggerName = restoringDBT.getName();
				String triggerSchema = restoringDBT.getSchema();
				Map<String, DBTrigger> triggers = adapter.getTriggers(triggerSchema);

				if(triggers.containsKey(triggerName)){
					DBTrigger existingDBT = triggers.get(triggerName);
					boolean ddlsDiffer = !restoringDBT.getSql().equals(existingDBT.getSql());

					if(ddlsDiffer) {
						st.execute(MessageFormat.format("DROP TRIGGER {0}.{1}", existingDBT.getOwner(), existingDBT.getName()));
						st.execute(restoreTrigger.getSqlObject().getSql());
					}

					//TODO should never differ, I guess
					//boolean ownersDiffer = !restoringDBF.getOwner().equals(existingDBF.getOwner());
					/*if(ownersDiffer) {
						String ddl = MessageFormat.format(
								"ALTER SCHEMA {0} TRANSFER {1}.{2}",
								restoringDBF.getOwner(), existingDBF.getOwner(), triggerName
						);
						st.execute(ddl);
					}*/
				} else {
					String ddl = restoringDBT.getSql();
					if(!ddl.isEmpty()){
						st.execute(ddl);
					} else {
						ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "encrypted").withParams(triggerName));
					}
				}
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}

		}
		catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}









		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
