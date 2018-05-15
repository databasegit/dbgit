package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreTableDataPostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if (obj instanceof MetaTableData) {	
			MetaTableData currentTableData;
			MetaTableData restoreTableData = (MetaTableData)obj;
			GitMetaDataManager gitMetaMng = GitMetaDataManager.getInctance();
			IMetaObject currentMetaObj = gitMetaMng.getCacheDBMetaObject(obj.getName());
			if (currentMetaObj instanceof MetaTableData) {
				currentTableData = (MetaTableData)currentMetaObj;				
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore Table Data.");
			}		
			if(Integer.valueOf(step).equals(0)) {
				removeTableConstraintsPostgres(restoreTableData.getMetaTable());
				return false;
			}
			if(Integer.valueOf(step).equals(1)) {
				restoreTableDataPostgres(restoreTableData);
				return false;
			}
			if(Integer.valueOf(step).equals(2)) {
				restoreTableConstraintPostgres(restoreTableData.getMetaTable());
				return false;
			}
			return true;
		}
		else
		{
			throw new ExceptionDBGitRestore("Error restore: Unable to restore Table Data.");
		}		
	}

	
	public void restoreTableDataPostgres(MetaTableData restoreTableData) {
		
		
	}


	public void restoreTableConstraintPostgres(MetaTable table) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {	
				for(DBConstraint constrs :table.getConstraints().values()) {
					if(!constrs.getConstraintType().equals("p")) {				
					st.execute("alter table "+ table.getTable().getName() +" add constraint "+ constrs.getName() + " "+constrs.getConstraintDef());
					}
				}						
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+table.getTable().getName(), e);
		} finally {
			st.close();
		}			
	}
	
	public void removeTableConstraintsPostgres(MetaTable table) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {					
				ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraintsCount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+table.getTable().getSchema()+"."+table.getTable().getName()+"'::regclass");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraintsCount"));
				if(constraintsCount.intValue()>0) {
					Map<String, DBConstraint> constraints = table.getConstraints();
					for(DBConstraint constrs :constraints.values()) {
						st.execute("alter table "+ table.getTable().getName() +" drop constraint "+constrs.getName());
					}
				}		
		}
		catch(Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+table.getTable().getName(), e);
		}		
	}
	
	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
