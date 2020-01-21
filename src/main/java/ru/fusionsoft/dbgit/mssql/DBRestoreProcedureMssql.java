package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaProcedure;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

public class DBRestoreProcedureMssql extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restorePrc").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaProcedure) {
				MetaProcedure restoreProcedure = (MetaProcedure)obj;
				DBSQLObject restoringProc = restoreProcedure.getSqlObject();
				String procedureName = restoringProc.getName();
				String procedureSchema = restoringProc.getSchema();

				if(adapter.getProcedures(procedureSchema).containsKey(procedureName)) {
					DBProcedure existingProc = adapter.getProcedure(procedureSchema, procedureName);

					if(!restoringProc.getSql().equals(existingProc.getSql())) {
                        st.execute(MessageFormat.format("DROP PROCEDURE {0}.{1}", existingProc.getOwner(), existingProc.getName()));
                        st.execute(restoreProcedure.getSqlObject().getSql(), "/");
						//TODO Восстановление привилегий
					}
				}
				else{
					st.execute(restoreProcedure.getSqlObject().getSql(), "/");
					//TODO Восстановление привилегий
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
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
			st.close();
		}

		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub
	}

}
