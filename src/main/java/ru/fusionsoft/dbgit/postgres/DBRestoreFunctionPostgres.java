package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaFunction;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBRestoreFunctionPostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaFunction) {
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreFnc").withParams(obj.getName()), 1);

				MetaFunction restoreFunction = (MetaFunction)obj;
				String restoreFunctionName = restoreFunction.getSqlObject().getName();
				Map<String, DBFunction> functions = adapter.getFunctions(restoreFunction.getSqlObject().getSchema());
				boolean exist = false;
				if(!(functions.isEmpty() || functions == null)) {
					for(DBFunction fnc:functions.values()) {
						if(restoreFunctionName.equals(fnc.getName())){
							exist = true;

							//if codes differ
							if( !restoreFunction.getSqlObject().getSql()
								.replace(" ", "")
								.equals(fnc.getSql().replace(" ", ""))
							) {
								st.execute(restoreFunction.getSqlObject().getSql());
							}

							//if owners differ
							if(!restoreFunction.getSqlObject().getOwner().equals(fnc.getOwner())) {
								StringProperties restoreProcArgs = restoreFunction.getSqlObject().getOptions().get("arguments");
								String args = restoreProcArgs != null ? restoreProcArgs.getData().replaceAll("(\\w+ \\w+) (DEFAULT [^\\,\\n]+)(\\,|\\b)", "$1") : "";

								st.execute(MessageFormat.format("ALTER FUNCTION {0}.{1}({2}) OWNER TO {3}"
									, restoreFunction.getUnderlyingDbObject().getSchema()
									, DBAdapterPostgres.escapeNameIfNeeded(restoreFunctionName)
									, args
									, restoreFunction.getSqlObject().getOwner()));
							}
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					st.execute(restoreFunction.getSqlObject().getSql());
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
			ConsoleWriter.detailsPrintlnRed(e.getLocalizedMessage());
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
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
			if(! (obj instanceof MetaFunction)) throw new ExceptionDBGit("Wrong IMetaObject type, expected: fnc, was: " + obj.getType().getValue());
			MetaFunction fncMeta = (MetaFunction) obj;
			DBFunction fnc = (DBFunction) fncMeta.getSqlObject();
			if (fnc == null) return;

			String schema = getPhisicalSchema(fnc.getSchema());
			st.execute("DROP FUNCTION "+DBAdapterPostgres.escapeNameIfNeeded(schema)+"."+DBAdapterPostgres.escapeNameIfNeeded(fnc.getName()));
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

}
