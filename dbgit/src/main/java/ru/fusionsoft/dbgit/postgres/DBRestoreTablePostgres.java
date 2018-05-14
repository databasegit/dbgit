package ru.fusionsoft.dbgit.postgres;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Map;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;
import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
public class DBRestoreTablePostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if(Integer.valueOf(step).equals(0)) {
			restoreTablePostgres(obj);
			return false;
		}
		if(Integer.valueOf(step).equals(1)) {
			restoreTableFieldsPostgres(obj);
			return false;
		}
		if(Integer.valueOf(step).equals(2)) {
			restoreTableIndexesPostgres(obj);
			return false;
		}
		if(Integer.valueOf(step).equals(3)) {
			restoreTableConstraintPostgres(obj);
			return false;
		}
		return true;
	}
	
	public void restoreTablePostgres(IMetaObject obj) throws Exception
	{
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
							//Map<String, DBIndex> currentIndexes = adapter.getIndexes(restoreTable.getTable().getSchema(), restoreTable.getTable().getName());
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
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore Table.");
			}						
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}			
	}
	public void restoreTableFieldsPostgres(IMetaObject obj) throws Exception
	{
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
							Map<String, DBTableField> currentFileds = adapter.getTableFields(restoreTable.getTable().getSchema(), restoreTable.getTable().getName());
							MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFileds);
							
							if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
								for(DBTableField tblField:diffTableFields.entriesOnlyOnLeft().values()) {
										String as = "alter table "+ restoreTable.getTable().getName() +" add column " + tblField.getName()  + " " + tblField.getTypeSQL();
										st.execute("alter table "+ restoreTable.getTable().getName() +" add column " + tblField.getName()  + " " + tblField.getTypeSQL());
								}								
							}
							
							if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
								for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
									st.execute("alter table "+ restoreTable.getTable().getName() +" drop column "+ tblField.getName());
								}								
							}
							
							if(!diffTableFields.entriesDiffering().isEmpty()) {						
								for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
									if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
										st.execute("alter table "+ restoreTable.getTable().getName() +" rename column "+ tblField.rightValue().getName() +" to "+ tblField.leftValue().getName());
									}
																	
									if(!tblField.leftValue().getTypeSQL().equals(tblField.rightValue().getTypeSQL())) {
										st.execute("alter table "+ restoreTable.getTable().getName() +" alter column "+ tblField.leftValue().getName() +" type "+ tblField.leftValue().getTypeSQL());
									}
								}								
							}										
						}						
					}
				}
				if(!exist){								
					for(DBTableField tblField:restoreTable.getFields().values()) {
							st.execute("alter table "+ restoreTable.getTable().getName() +" add column " + tblField.getName()  + " " + tblField.getTypeSQL());
					}
				}
				
				removeTableConstraintPostgres(obj);
				// set primary key
				for(DBConstraint tableconst: restoreTable.getConstraints().values()) {
					if(tableconst.getConstraintType().equals("p")) {
						st.execute("alter table "+ restoreTable.getTable().getName() +" add constraint "+ tableconst.getName() + " "+tableconst.getConstraintDef());
						break;
					}
				}
				
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore TableFields.");
			}						
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}			
	}
	public void restoreTableIndexesPostgres(IMetaObject obj) throws Exception
	{
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
							Map<String, DBIndex> currentIndexes = adapter.getIndexes(table.getSchema(), table.getName());
							MapDifference<String, DBIndex> diffInd = Maps.difference(restoreTable.getIndexes(), currentIndexes);
							if(!diffInd.entriesOnlyOnLeft().isEmpty()) {
								for(DBIndex ind:diffInd.entriesOnlyOnLeft().values()) {
									if(ind.getOptions().getChildren().containsKey("tablespace")) {
										st.execute(ind.getSql()+" tablespace "+ind.getOptions().get("tablespace").getData());
									}
									else {
										st.execute(ind.getSql());
									}
								}								
							}
							if(!diffInd.entriesOnlyOnRight().isEmpty()) {
								for(DBIndex ind:diffInd.entriesOnlyOnRight().values()) {
									st.execute("drop index "+ind.getName());
								}								
							}
							
							if(!diffInd.entriesDiffering().isEmpty()) {
								for(ValueDifference<DBIndex>  ind:diffInd.entriesDiffering().values()) {
									if(ind.leftValue().getOptions().getChildren().containsKey("tablespace")) {
										if(ind.rightValue().getOptions().getChildren().containsKey("tablespace") && !ind.leftValue().getOptions().get("tablespace").getData().equals(ind.rightValue().getOptions().get("tablespace").getData())) {
											st.execute("alter index "+ind.leftValue().getName() +" set tablespace "+ind.leftValue().getOptions().get("tablepace"));	
										}
																			
									}
									else if(ind.rightValue().getOptions().getChildren().containsKey("tablespace")) {
										st.execute("alter index "+ind.leftValue().getName() +" set tablespace pg_default");	
									}									
								}								
							}										
						}						
					}
				}
				if(!exist){								
					for(DBIndex ind:restoreTable.getIndexes().values()) {						
							if(ind.getOptions().getChildren().containsKey("tablespace")) {
								String as = ind.getSql()+" tablespace "+ind.getOptions().get("tablespace").getData();
								st.execute(ind.getSql()+" tablespace "+ind.getOptions().get("tablespace").getData());
							}
							else {						
								st.execute(ind.getSql());
							}						
					}
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore TableIndexes.");
			}						
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}			
	}
	public void restoreTableConstraintPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;		
				for(DBConstraint constrs :restoreTable.getConstraints().values()) {
					if(!constrs.getConstraintType().equals("p")) {				
					st.execute("alter table "+ restoreTable.getTable().getName() +" add constraint "+ constrs.getName() + " "+constrs.getConstraintDef());
					}
				}
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to restore TableConstraints.");
			}						
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		} finally {
			st.close();
		}			
	}
	
	public void removeTableConstraintPostgres(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraintsCount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+restoreTable.getTable().getSchema()+"."+restoreTable.getTable().getName()+"'::regclass");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraintsCount"));
				if(constraintsCount.intValue()>0) {
					Map<String, DBConstraint> constraints = adapter.getConstraints(restoreTable.getTable().getSchema(),restoreTable.getTable().getName());
					for(DBConstraint constrs :constraints.values()) {
						st.execute("alter table "+ restoreTable.getTable().getName() +" drop constraint "+constrs.getName());
					}
				}				
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to remove TableConstraints.");
			}						
		}
		catch (Exception e) {
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
