package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.MapDifference.ValueDifference;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreTableOracle extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if(Integer.valueOf(step).equals(0)) {
			restoreTableOracle(obj);
			return false;
		}
		/*if(Integer.valueOf(step).equals(1)) {
			restoreTableFieldsOracle(obj);
			return false;
		}*/
		if(Integer.valueOf(step).equals(-21)) {
			restoreTableIndexesOracle(obj);
			return false;
		}
		if(Integer.valueOf(step).equals(-1)) {
			restoreTableConstraintOracle(obj);
			return false;
		}
		return true;
	}
	
	public void restoreTableOracle(IMetaObject obj) throws Exception
	{
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				
				MetaTable restoreTable = (MetaTable)obj;	
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String tblName = schema+"."+restoreTable.getTable().getName();
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "table").withParams(tblName) + "\n", 1);

				Map<String, DBTable> tables = adapter.getTables(schema.toUpperCase());
				boolean exist = false;
				if(!(tables.isEmpty() || tables == null)) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equalsIgnoreCase(table.getName())){
							exist = true;
																									
						}						
					}
				}
				if(!exist){			
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "createTable"), 2);
					String ddl = "";
					if (restoreTable.getTable().getOptions().get("ddl") != null)
						ddl = restoreTable.getTable().getOptions().get("ddl").getData()
							.replace("\"" + restoreTable.getTable().getSchema().toUpperCase() + "\"", "\"" + schema.toUpperCase() + "\"");
					else {
						DBTable table = restoreTable.getTable();
						
						Comparator<DBTableField> comparator = (o1, o2) -> o1.getOrder().compareTo(o2.getOrder());
						
						ddl = "create table " + schema + "." + table.getName() +
								" (" + restoreTable.getFields().values().stream()
									.sorted(comparator)
									.map(field -> "" + (adapter.isReservedWord(field.getName()) ? "\"" + field.getName() + "\" " : field.getName()) + " " + field.getTypeSQL())
									.collect(Collectors.joining(", ")) + ")";
					}
					st.execute(ddl);					
					
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				} else {
				//restore tabl fields
							Map<String, DBTableField> currentFileds = adapter.getTableFields(schema, restoreTable.getTable().getName().toLowerCase());
							MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFileds);
							
							if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
								ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addColumns"), 2);
								
								Comparator<DBTableField> comparator = (o1, o2) -> o1.getOrder().compareTo(o2.getOrder());
								
								List<DBTableField> values = diffTableFields.entriesOnlyOnLeft().values().stream().collect(Collectors.toList());
								values.sort(comparator);
								
								for(DBTableField tblField : values) {
										st.execute("alter table "+ tblName +" add " + tblField.getName()  + " " + tblField.getTypeSQL());
								}								
								ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
							}
							
							if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
								ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "droppingColumns"), 2);
								for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
									st.execute("alter table "+ tblName +" drop column "+ tblField.getName());
								}								
								ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
							}
							
							if(!diffTableFields.entriesDiffering().isEmpty()) {
								ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "modifyColumns"), 2);

								for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
									if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
										st.execute("alter table "+ tblName +" rename column "+ tblField.rightValue().getName() +" to "+ tblField.leftValue().getName());
									}

									if(!tblField.leftValue().getTypeUniversal().equals(tblField.rightValue().getTypeUniversal())) {
										st.execute("alter table "+ tblName +" modify "+ tblField.leftValue().getName() +" "+ tblField.leftValue().getTypeSQL());
									}
								}		
								ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
							}						
				}
				
				ResultSet rs = st.executeQuery("SELECT COUNT(cons.constraint_name) constraintscount \n" + 
						"FROM all_constraints cons \n" + 
						"WHERE upper(owner) = upper('" + schema + "') and upper(table_name) = upper('" + restoreTable.getTable().getName()+ "') and constraint_name not like 'SYS%' and cons.constraint_type = 'P'");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraintscount"));
				if(constraintsCount.intValue()>0) {
					removeTableConstraintsOracle(obj);					
				}	
				// set primary key
				boolean flagPkCreated = false;
				for(DBConstraint tableconst: restoreTable.getConstraints().values()) {
					if(tableconst.getConstraintType().equals("p")) {
						ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addPk"), 2);
						
						if (tableconst.getOptions().get("ddl").toString().toLowerCase().startsWith("alter table")) 
							st.execute(tableconst.getOptions().get("ddl").toString().replace(" " +tableconst.getSchema() + ".", " " + schema + "."));
						else if (tableconst.getOptions().get("ddl").toString().toLowerCase().startsWith("primary key"))
							st.execute("alter table "+ tblName +" add constraint PK_"+ tableconst.getName() + tableconst.getOptions().get("ddl").toString());
						ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
						flagPkCreated = true;
						break;
					}
				}
				
				if (!flagPkCreated) {
					for(DBTableField field: restoreTable.getFields().values()) {
						if (field.getIsPrimaryKey()) {
							ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addPk"), 2);
							st.execute("alter table "+ tblName +" add constraint pk_" + restoreTable.getTable().getName() + "_" + field.getName() + " PRIMARY KEY (" + field.getName() + ")");
							ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));							
							break;
						}
					}
				}
				
													
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (SQLException e1) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage()));
			//throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage());
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}			
	}
	
	public void restoreTableFieldsOracle(IMetaObject obj) throws Exception
	{
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;	
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String tblName = schema+"."+restoreTable.getTable().getName();
				Map<String, DBTable> tables = adapter.getTables(schema);
				boolean exist = false;
				if(!(tables.isEmpty() || tables == null)) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equals(table.getName())){
							exist = true;																									
						}						
					}
				}
				if(!exist){								
					st.execute(restoreTable.getTable().getOptions().get("ddl").getData());			
				}
				//restore tabl fields
							Map<String, DBTableField> currentFileds = adapter.getTableFields(schema, restoreTable.getTable().getName());
							MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFileds);
							
							if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
								for(DBTableField tblField:diffTableFields.entriesOnlyOnLeft().values()) {
										st.execute("alter table "+ tblName +" add " + tblField.getName()  + " " + tblField.getTypeSQL());
								}								
							}
							
							if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
								for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
									st.execute("alter table "+ tblName +" drop column "+ tblField.getName());
								}								
							}
							
							if(!diffTableFields.entriesDiffering().isEmpty()) {						
								for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
									if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
										st.execute("alter table "+ tblName +" rename column "+ tblField.rightValue().getName() +" to "+ tblField.leftValue().getName());
									}
																	
									if(!tblField.leftValue().getTypeSQL().equals(tblField.rightValue().getTypeSQL())) {
										st.execute("alter table "+ tblName +" modify "+ tblField.leftValue().getName() +" "+ tblField.leftValue().getTypeSQL());
									}
								}								
							}						
						
				ResultSet rs = st.executeQuery("SELECT COUNT(cons.constraint_name)\n" + 
						"FROM all_constraints cons \n" + 
						"WHERE upper(owner) = upper('" + schema + "') and upper(table_name) = upper('" + tblName+ "') and constraint_name not like 'SYS%' and cons.constraint_type = 'P'");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraintscount"));
				if(constraintsCount.intValue()>0) {
					removeTableConstraintsOracle(obj);
				}
				// set primary key
				for(DBConstraint tableconst: restoreTable.getConstraints().values()) {
					if(tableconst.getConstraintType().equals("p")) {
						st.execute(restoreTable.getConstraints().get("constraintDef").getOptions().get("ddl").toString());
						//st.execute("alter table "+ tblName +" add constraint PK_"+ tableconst.getName() + " primary key ("+tableconst.getName() + ")");
						break;
					}
				}									
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (SQLException e1) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage()));
			//throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage());
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}			
	}
	
	public void restoreTableIndexesOracle(IMetaObject obj) throws Exception
	{
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreIndex").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;	
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());						
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				Map<String, DBTable> tables = adapter.getTables(schema);
				boolean exist = false;
				if(!(tables.isEmpty() || tables == null)) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equals(table.getName())){
							exist = true;
							Map<String, DBIndex> currentIndexes = adapter.getIndexes(schema, table.getName());
							MapDifference<String, DBIndex> diffInd = Maps.difference(restoreTable.getIndexes(), currentIndexes);
							if(!diffInd.entriesOnlyOnLeft().isEmpty()) {
								for(DBIndex ind:diffInd.entriesOnlyOnLeft().values()) {
									ConsoleWriter.println(ind.getSql());
									if (ind.getSql().length() > 5 )
										st.execute(ind.getSql());
								}								
							}
							if(!diffInd.entriesOnlyOnRight().isEmpty()) {
								for(DBIndex ind:diffInd.entriesOnlyOnRight().values()) {
									st.execute("drop index "+schema+"."+ind.getName());
								}								
							}
							
							//not fact, will check in future										
						}						
					}
				}
				if(!exist){								
					for(DBIndex ind:restoreTable.getIndexes().values()) {						
						st.execute(ind.getSql());						
					}
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (SQLException e1) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage());
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}			
	}
	
	public void restoreTableConstraintOracle(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreConstr").withParams(obj.getName()), 1);
		try {
			if (obj instanceof MetaTable) {
				MetaTable restoreTable = (MetaTable)obj;
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				for(DBConstraint constrs :restoreTable.getConstraints().values()) {
					if(!constrs.getConstraintType().equalsIgnoreCase("P")) {				
						//String tblName = schema+"."+restoreTable.getTable().getName();
						
						st.execute(constrs.getOptions().get("ddl").toString().replace(" " + constrs.getSchema() + ".", " " + schema + "."));
					}
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}			
		} catch (SQLException e1) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage()));
			//throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage());
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}	
	}
	
	public void removeTableConstraintsOracle(IMetaObject obj) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {			
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				Map<String, DBConstraint> constraints = table.getConstraints();
				for(DBConstraint constrs :constraints.values()) {
					
					String dropQuery = "declare cnt number;\r\n" + 
							"begin    \r\n" + 
							"select count(*) into cnt from ALL_CONSTRAINTS where upper(CONSTRAINT_NAME) = upper('" + constrs.getName() + "') and upper(owner) = upper('" + schema + "');\r\n" + 
							"   if (cnt > 0) \r\n" + 
							"    then \r\n" + 
							"        execute immediate('alter table " + schema + "." + table.getTable().getName() + " drop constraint " + constrs.getName() + "');\r\n" + 
							"    end if;\r\n" + 
							"end;";	

					
					st.execute(dropQuery, "/");
				}
				//}	
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to remove TableConstraints.");
			}	

		} catch (SQLException e1) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage()));
			//throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage());
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		}		
	}
	
	/*public void removeIndexesOracle(IMetaObject obj) throws Exception {		
		
	}*/
	
	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		
		try {
			
			MetaTable tblMeta = (MetaTable)obj;
			DBTable tbl = tblMeta.getTable();
			//String schema = getPhisicalSchema(tbl.getSchema());
			
			st.execute("DROP TABLE " +tbl.getName());
		
			// TODO Auto-generated method stub
		} catch (SQLException e1) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()) + "\n"+ e1.getLocalizedMessage());
		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

}
