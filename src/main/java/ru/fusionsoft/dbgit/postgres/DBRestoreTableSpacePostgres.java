package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTableSpace;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreTableSpacePostgres extends DBRestoreAdapter{

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint("Restoring tablespace " + obj.getName() + "...", 1);
		try {
			if (obj instanceof MetaTableSpace) {
				MetaTableSpace restoreTableSpace = (MetaTableSpace)obj;								
				Map<String, DBTableSpace> tblspaces = adapter.getTableSpaces();
				boolean exist = false;
				if(!(tblspaces.isEmpty() || tblspaces == null)) {
					for(DBTableSpace tblspace:tblspaces.values()) {
						if(restoreTableSpace.getObjectOption().getName().equals(tblspace.getName())){
							exist = true;
							
							String restorename = restoreTableSpace.getObjectOption().getName();
							String restoreowner = restoreTableSpace.getObjectOption().getOptions().getChildren().get("usename").getData();							
							String restoreloc = restoreTableSpace.getObjectOption().getOptions().getChildren().get("pg_tablespace_location").getData();
							
							st.execute("alter tablespace "+ restorename +" reset (seq_page_cost, random_page_cost, effective_io_concurrency)");
							
							String currentname = tblspace.getName();
							String currentowner = tblspace.getOptions().getChildren().get("usename").getData();
							String currentloc = tblspace.getOptions().getChildren().get("pg_tablespace_location").getData();
							
							if(!restoreowner.equals(currentowner)) {
								st.execute("alter tablespace "+ restorename +" owner to "+ restoreowner);
							}
							
							if(restoreTableSpace.getObjectOption().getOptions().getChildren().containsKey("spcoptions")) {
								String options = restoreTableSpace.getObjectOption().getOptions().getChildren().get("spcoptions").getData().replaceAll("[\\{\\}]", "");
								st.execute("alter tablespace "+ restorename +" set ("+ options+")");
							}

								
							/*if() {
							//TODO Restore location ???
							}*/
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					
					//TODO Восстановление привилегий	
				}
				else {
					
				}
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed("FAIL");
				throw new ExceptionDBGitRestore("Error restore: Unable to restore TABLE SPACES.");
			}			
		} catch (Exception e) {
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
