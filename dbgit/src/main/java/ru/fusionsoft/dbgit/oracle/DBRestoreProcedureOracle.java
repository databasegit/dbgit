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
		ConsoleWriter.detailsPrint("Restoring procedure " + obj.getName() + "...", 1);
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
								st.execute(restoreProcedure.getSqlObject().getSql());
							}
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					st.execute(restoreProcedure.getSqlObject().getSql());
					//TODO Восстановление привилегий	
				}
				ConsoleWriter.detailsPrintlnGreen("OK");
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed("FAIL");
				throw new ExceptionDBGitRestore("Error restore: Unable to restore Triggers.");
			}			
			
			
			
			
		}
		catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed("FAIL");
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
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