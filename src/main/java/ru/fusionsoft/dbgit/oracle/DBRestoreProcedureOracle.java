package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaProcedure;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;


public class DBRestoreProcedureOracle extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restorePrc").withParams(obj.getName()), 1);
		try {						
			if (obj instanceof MetaProcedure) {
				MetaProcedure restoreProcedure = (MetaProcedure)obj;								
				Map<String, DBProcedure> prcds = adapter.getProcedures(restoreProcedure.getSqlObject().getSchema());
				boolean exist = false;
				if(!(prcds.isEmpty() || prcds == null)) {
					for(DBProcedure prcd:prcds.values()) {
						if(restoreProcedure.getSqlObject().getName().equals(prcd.getName())){
							exist = true;					
							if(!restoreProcedure.getSqlObject().getSql().equals(prcd.getSql())) {
								st.execute(restoreProcedure.getSqlObject().getSql(), "/");
							}
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
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