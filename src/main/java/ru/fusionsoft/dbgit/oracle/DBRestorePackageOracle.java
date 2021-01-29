package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaPackage;
import ru.fusionsoft.dbgit.meta.MetaProcedure;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestorePackageOracle extends DBRestoreAdapter {
	
	private StatementLogging st;

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaPackage) {
				MetaPackage restorePackage = (MetaPackage) obj;								
				Map<String, DBPackage> pkgs = adapter.getPackages(restorePackage.getSqlObject().getSchema());
				boolean exist = false;
				if(!(pkgs.isEmpty() || pkgs == null)) {
					for(DBPackage pkg : pkgs.values()) {
						if(restorePackage.getSqlObject().getName().equals(pkg.getName())){
							exist = true;
							String sql = restorePackage.getSqlObject().getSql();
							if(!sql.equals(pkg.getSql())) 								
								executeSql(sql);
							
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){
					 executeSql(restorePackage.getSqlObject().getSql());
					//TODO Восстановление привилегий	
				}
				ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			}
			else
			{
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "package", obj.getType().getValue()
                ));
			}			
			
		}
		catch (Exception e) {
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
	
	private void executeSql(String sql) throws Exception {
		int index = sql.indexOf("CREATE OR REPLACE PACKAGE BODY");
		
		String sqlHead = sql.substring(0, index);								
		String sqlBody = sql.substring(index);	
		
		st.execute(sqlHead, "/");
		st.execute(sqlBody, "/");
	}

}
