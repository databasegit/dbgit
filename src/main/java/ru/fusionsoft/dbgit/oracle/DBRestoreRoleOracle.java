package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaRole;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreRoleOracle extends DBRestoreAdapter{

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint("Restoring role " + obj.getName() + "...", 1);
		try {
			if (obj instanceof MetaRole) {
				MetaRole restoreRole = (MetaRole)obj;
				Map<String, DBRole> roles = adapter.getRoles();
				boolean exist = false;
				if(!(roles.isEmpty() || roles == null)) {
					for(DBRole role:roles.values()) {
						if(restoreRole.getObjectOption().getName().equals(role.getName())){
							exist = true;
							
						}
							//TODO Восстановление привилегий							
					}
				}
			
				if(!exist){
					//String rolconnect = "", rolresource = "", roldba = "";
					String q = "GRANT ";
					if(restoreRole.getObjectOption().getOptions().getChildren().get("GRANTED_ROLE").getData().equals("CONNECT")) {
						//rolconnect = "CONNECT";
						q += "CONNECT";
					}
					
					if(restoreRole.getObjectOption().getOptions().getChildren().get("GRANTED_ROLE").getData().equals("RESOURCE")) {
						//rolresource = "RESOURCE";
						q += ", RESOURCE";
					}
					
					if(restoreRole.getObjectOption().getOptions().getChildren().get("GRANTED_ROLE").getData().equals("DBA")) {
						//roldba = "DBA";
						q += ", DBA";
					}
					
					st.execute(q + 
							" TO " + restoreRole.getObjectOption().getOptions().getChildren().get("GRANTEE").getData());
					
					//TODO Восстановление привилегий	
				}
				
				//TODO restore memberOfRole
				ConsoleWriter.detailsPrintlnGreen("OK");
		}
			else
			{
				ConsoleWriter.detailsPrintlnRed("FAIL");
				throw new ExceptionDBGitRestore("Error restore: Unable to restore ROLES.");
			}			
		} catch (Exception e) {
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
