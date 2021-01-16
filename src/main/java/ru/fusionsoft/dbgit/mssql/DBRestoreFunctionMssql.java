package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBSQLObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaFunction;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

public class DBRestoreFunctionMssql extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaFunction) {
				MetaFunction restoreFunction = (MetaFunction)obj;
				DBSQLObject restoringDBF = restoreFunction.getSqlObject();
				String functionName = restoreFunction.getSqlObject().getName();
				Map<String, DBFunction> functions = adapter.getFunctions(restoreFunction.getSqlObject().getSchema());

				if(functions.containsKey(functionName)){
					DBFunction existingDBF = functions.get(functionName);
					boolean ddlsDiffer = !restoringDBF.getSql().equals(existingDBF.getSql());
					boolean ownersDiffer = !restoringDBF.getOwner().equals(existingDBF.getOwner());

					if(ddlsDiffer) {
                        st.execute(MessageFormat.format("DROP FUNCTION {0}.{1}", existingDBF.getOwner(), existingDBF.getName()));
                        st.execute(restoreFunction.getSqlObject().getSql());
                    }
					if(ownersDiffer) {
					    //TODO remove sp_changeowner usage in other methods
						String ddl = MessageFormat.format(
                            "ALTER SCHEMA {0} TRANSFER {1}.{2}",
                            restoringDBF.getOwner(), existingDBF.getOwner(), functionName
						);
						st.execute(ddl);
					}
				} else {
                    st.execute(restoreFunction.getSqlObject().getSql());
                }
                //TODO Восстановление привилегий
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
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			st.close();
		}
		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
