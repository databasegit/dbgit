package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.util.Map;
import java.sql.Connection;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;

public class DBRestoreTablePostgres extends DBRestoreAdapter {

	@Override
	public void restoreMetaObject(IMetaObject obj) throws Exception {

		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;		
				Map<String, DBTable> tables = adapter.getTables(restoreTable.getTable().getSchema());
				boolean exist = false;
				if(!(tables.isEmpty() || tables == null)) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equals(table.getName())){
							exist = true;
							Map<String, DBIndex> currentIndexes = adapter.getIndexes(restoreTable.getTable().getSchema(), restoreTable.getTable().getName());
							String owner = restoreTable.getTable().getOptions().get("tableowner").getData();
							if(!owner.equals(table.getOptions().get("tableowner").getData())) {
								st.execute("alter table "+ restoreTable.getTable().getName() + " owner to "+ owner);
							}
														
							if(restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")) {
								String tablespace = restoreTable.getTable().getOptions().get("tablespace").getData();
								st.execute("alter table "+ restoreTable.getTable().getName() + " set tablespace "+ tablespace);
							}
							else if(table.getOptions().getChildren().containsKey("tablespace")) {
								st.execute("alter table "+ restoreTable.getTable().getName() + " set tablespace pg_default");
							}
							
							//if(restoreTable.getIndexes().isEmpty()) {
								if(!currentIndexes.isEmpty()) {
									st.execute("drop index tb1_pkey");
									
							//	}
								
							}
							//else(){
								
							//}
							//String test2 = restoreTable.getTable().getOptions().get("tablespace").getData();
							//if(!restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData().equals(sch.getOptions().getChildren().get("usename").getData())) {
							//	st.execute("ALTER SCHEMA "+ restoreSchema.getObjectOption().getName() +" OWNER TO "+ 
							//	restoreSchema.getObjectOption().getOptions().getChildren().get("usename").getData());
							//}
							//TODO Восстановление привилегий							
						}
					}
				}
				if(!exist){			
					
					if(restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")) {
						String tablespace = restoreTable.getTable().getOptions().get("tablespace").getData();
						st.execute("create table "+ restoreTable.getTable().getName() + "() tablespace "+ tablespace);
						st.execute("alter table "+ restoreTable.getTable().getName() + " owner to "+ restoreTable.getTable().getOptions().get("tableowner").getData());
						
					}
					else {
						st.execute("create table "+ restoreTable.getTable().getName() + "()");
						st.execute("alter table "+ restoreTable.getTable().getName() + " owner to "+ restoreTable.getTable().getOptions().get("tableowner").getData());
					}
					
					//TODO restore indexes

					//TODO Восстановление привилегий	
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore SCHEMAS.");
			}			
		} catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}

	}
	
	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		
		try {
			
			MetaTable tblMeta = (MetaTable)obj;
			DBTable tbl = tblMeta.getTable();
			
			st.execute("DROP TABLE "+tbl.getSchema()+"."+tbl.getName());
		
			// TODO Auto-generated method stub
		} catch (Exception e) {
			throw new ExceptionDBGitRestore("Error remove "+obj.getName(), e);
		} finally {
			st.close();
		}
	}

}
