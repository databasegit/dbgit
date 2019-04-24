package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
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
		ConsoleWriter.detailsPrint("Restoring trigger " + obj.getName() + "...", 1);
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
				ConsoleWriter.detailsPrintlnRed("FAIL");
				throw new ExceptionDBGitRestore("Error restore: Unable to restore Triggers.");
			}			
			
			
			
			
		}
		catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed("FAIL");
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen("OK");
			st.close();
		}
		
		
		
		
		
		
		
		
		
		return true;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
