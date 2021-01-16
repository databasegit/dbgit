package ru.fusionsoft.dbgit.postgres;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaProcedure;
import ru.fusionsoft.dbgit.meta.NameMeta;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

public class DBRestoreProcedurePostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaProcedure) {
				MetaProcedure restoreProc = (MetaProcedure)obj;
				NameMeta nm = new NameMeta(restoreProc);
				String restoreProcName = nm.getName();
				Map<String, DBProcedure> procs = adapter.getProcedures(nm.getSchema());
				boolean exist = false;
				if(!(procs.isEmpty() || procs == null)) {
					for(DBProcedure prc : procs.values()) {
						if(restoreProcName.equals(prc.getName())){
							exist = true;

							//if codes differ
							if( !restoreProc.getSqlObject().getSql()
								.replaceAll("\\s+", "")
								.equals(prc.getSql().replaceAll("\\s+", ""))
							) {
								st.execute(restoreProc.getSqlObject().getSql());
							}

							//if owners differ
							if(!restoreProc.getSqlObject().getOwner().equals(prc.getOwner())) {
								StringProperties restoreProcArgs = restoreProc.getSqlObject().getOptions().get("arguments");
								String args = restoreProcArgs != null ? restoreProcArgs.getData().replaceAll("(\\w+ \\w+) (DEFAULT [^\\,\\n]+)(\\,|\\b)", "$1") : "";

								st.execute(MessageFormat.format("ALTER PROCEDURE {0}.{1}({2}) OWNER TO {3}"
									, nm.getSchema()
									, adapter.escapeNameIfNeeded(restoreProcName)
									, args
									, restoreProc.getSqlObject().getOwner()));
							}
							//TODO Восстановление привилегий
						}
					}
				}
				if(!exist){
					st.execute(restoreProc.getSqlObject().getSql());
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
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			st.close();
		}
		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception
	{
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if(! (obj instanceof MetaProcedure)) throw new ExceptionDBGit("Wrong IMetaObject type, expected: prc, was: " + obj.getType().getValue());
			MetaProcedure prcMeta = (MetaProcedure) obj;
			DBProcedure prc = (DBProcedure) prcMeta.getSqlObject();
			if (prc == null) return;

			String schema = getPhisicalSchema(prc.getSchema());
			st.execute("DROP PROCEDURE "+schema+"."+adapter.escapeNameIfNeeded(prc.getName()));
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}

	}

}
