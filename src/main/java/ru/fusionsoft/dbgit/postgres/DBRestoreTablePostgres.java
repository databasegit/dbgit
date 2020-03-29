package ru.fusionsoft.dbgit.postgres;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;
import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
public class DBRestoreTablePostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if(Integer.valueOf(step).equals(0)) {
			restoreTablePostgres(obj);
			return false;
		}

		if(Integer.valueOf(step).equals(1)) {
			restoreTableIndexesPostgres(obj);
			return false;
		}
		
		if(Integer.valueOf(step).equals(-1)) {
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
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema().toLowerCase());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String tblName = schema+"." + DBAdapterPostgres.escapeNameIfNeeded(restoreTable.getTable().getName());
				
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "table").withParams(tblName) + "\n", 1);
				
				Map<String, DBTable> tables = adapter.getTables(schema);
				boolean exist = false;
				if(!(tables == null || tables.isEmpty() )) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equalsIgnoreCase(table.getName())){
							exist = true;
							//Map<String, DBIndex> currentIndexes = adapter.getIndexes(restoreTable.getTable().getSchema(), restoreTable.getTable().getName());
							//String owner = restoreTable.getTable().getOptions().get("owner").getData();
							//if(!owner.equals(table.getOptions().get("owner").getData())) {
							//	st.execute("alter table "+ tblName + " owner to "+ schema);
							//}
														
							if(restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")) {
								String tablespace = restoreTable.getTable().getOptions().get("tablespace").getData();
								st.execute("alter table "+ tblName + " set tablespace "+ tablespace);
							}
							else if(table.getOptions().getChildren().containsKey("tablespace")) {
								st.execute("alter table "+ tblName + " set tablespace pg_default");
							}													
						}						
					}
				}
				if(!exist){								
					ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "createTable"), 2);
					String ownerName = restoreTable.getTable().getOptions().get("owner").getData();
					//Map<String, DBRole> roles = adapter.getRoles();
					
					if(restoreTable.getTable().getOptions().getChildren().containsKey("tablespace")) {
						String tablespace = restoreTable.getTable().getOptions().get("tablespace").getData();
						String querry ="create table "+ tblName + "() tablespace "+ tablespace +";\n";
						querry+="alter table "+ tblName + " owner to "+ ownerName + ";";
						st.execute(querry);			
					}
					else {
						String querry = "create table " + tblName + " ()" + ";\n";
						querry+="alter table "+ tblName + " owner to "+ ownerName + ";";
						st.execute(querry);	
					}				
					
					ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				}
				
				if (restoreTable.getTable().getComment() != null && restoreTable.getTable().getComment().length() > 0)
					st.execute("COMMENT ON TABLE " + tblName + " IS '" + restoreTable.getTable().getComment() + "'");
				
					//restore tabl fields
					Map<String, DBTableField> currentFileds = adapter.getTableFields(schema.toLowerCase(), restoreTable.getTable().getName().toLowerCase());
					MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFileds);

					if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
						ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addColumns"), 2);

						Comparator<DBTableField> comparator = Comparator.comparing(DBTableField::getOrder);

						List<DBTableField> values = diffTableFields.entriesOnlyOnLeft().values().stream().collect(Collectors.toList());
						values.sort(comparator);

						for(DBTableField tblField : values) {
								st.execute(
								"alter table "+ tblName +" add column "
									+ DBAdapterPostgres.escapeNameIfNeeded(tblField.getName()) + " "
									+ tblField.getTypeSQL().replace("NOT NULL", "")
								);

								if (tblField.getDescription() != null && tblField.getDescription().length() > 0)
									st.execute(
									"COMMENT ON COLUMN " + tblName + "."
										+ DBAdapterPostgres.escapeNameIfNeeded(tblField.getName())
										+ " IS '" + tblField.getDescription() + "'"
									);

								if (!tblField.getIsNullable()) {
									st.execute(
									"alter table " + tblName
										+ " alter column " + DBAdapterPostgres.escapeNameIfNeeded(tblField.getName())
										+ " set not null"
									);
								}

								if (tblField.getDefaultValue() != null && tblField.getDefaultValue().length() > 0) {
									st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName())
											+ " SET DEFAULT " + tblField.getDefaultValue());
								}

						}
						ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
					}
							
					if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
						ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "droppingColumns"), 2);
						for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
							st.execute("alter table "+ tblName +" drop column "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.getName()));
						}
						ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
					}

					if(!diffTableFields.entriesDiffering().isEmpty()) {
						ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "modifyColumns"), 2);
						for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
							if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
								st.execute(
								"alter table "
									+ tblName
									+" rename column "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.rightValue().getName())
									+" to "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName())
								);
							}

							if (restoreTable.getTable().getComment() != null && restoreTable.getTable().getComment().length() > 0)
								st.execute("COMMENT ON COLUMN " + tblName + "." + ((adapter.isReservedWord(tblField.leftValue().getName()) || tblField.leftValue().getNameExactly()) ? "\"" + tblField.leftValue().getName() + "\" " : tblField.leftValue().getName()) + " IS '" + tblField.leftValue().getDescription() + "'");

							if(!tblField.leftValue().getTypeSQL().equals(tblField.rightValue().getTypeSQL())
									&& tblField.rightValue().getTypeUniversal() != FieldType.BOOLEAN) {
								st.execute(
								"alter table "
									+ tblName
									+" alter column "+ DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName())
									+" type "+ tblField.leftValue().getTypeSQL().replace("NOT NULL", "")
								);
								if (!tblField.leftValue().getIsNullable()) {
									st.execute(
									"alter table " + tblName
										+ " alter column " + DBAdapterPostgres.escapeNameIfNeeded(tblField.leftValue().getName())
										+ " set not null"
									);
								}
							}
						}
						ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
					}

					ResultSet rs = st.executeQuery("SELECT count(*) constraints_count\r\n" +
							"       FROM pg_catalog.pg_constraint con\r\n" +
							"            INNER JOIN pg_catalog.pg_class rel\r\n" +
							"                       ON rel.oid = con.conrelid\r\n" +
							"            INNER JOIN pg_catalog.pg_namespace nsp\r\n" +
							"                       ON nsp.oid = connamespace\r\n" +
							"       WHERE lower(contype) <> 'p' and upper(nsp.nspname) = upper('" + schema + "')\r\n" +
							"             AND upper(rel.relname) = upper('" + restoreTable.getTable().getName() + "')");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraints_count"));
				if(constraintsCount.intValue()>0) {
					removeTableConstraintsPostgres(obj);
				}
				
				ResultSet rsPrimary = st.executeQuery("SELECT count(*) constraints_count\r\n" + 
						"       FROM pg_catalog.pg_constraint con\r\n" + 
						"            INNER JOIN pg_catalog.pg_class rel\r\n" + 
						"                       ON rel.oid = con.conrelid\r\n" + 
						"            INNER JOIN pg_catalog.pg_namespace nsp\r\n" + 
						"                       ON nsp.oid = connamespace\r\n" + 
						"       WHERE lower(contype) = 'p' and upper(nsp.nspname) = upper('" + schema + "')\r\n" + 
						"             AND upper(rel.relname) = upper('" + restoreTable.getTable().getName() + "')");
				rsPrimary.next();
	
				// set primary key
				if (rsPrimary.getInt("constraints_count") == 0) {
					boolean flagPkCreated = false;
					for(DBConstraint tableconst: restoreTable.getConstraints().values()) {
						if(tableconst.getConstraintType().equals("p")) {
							ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addPk"), 2);
							st.execute(
							"alter table "+ tblName
								+" add constraint "+ DBAdapterPostgres.escapeNameIfNeeded(tableconst.getName())
								+ " "+tableconst.getSql().replace(" " +tableconst.getSchema() + ".", " " + schema + ".")
							);
							ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
							flagPkCreated = true;
							break;
						}
					}		
					
					if (!flagPkCreated) {
						for(DBTableField field: restoreTable.getFields().values()) {
							if (field.getIsPrimaryKey()) {
								ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "addPk"), 2);
								
								String constrName = "pk_" + restoreTable.getTable().getName() + "_" + field.getName();
								if (constrName.contains("."))
									constrName = "\"" + constrName + "\"";
								
								st.execute(
								"alter table "+ tblName
									+ " add constraint " + DBAdapterPostgres.escapeNameIfNeeded(constrName)
									+ " PRIMARY KEY (" + DBAdapterPostgres.escapeNameIfNeeded(field.getName()) + ")"
								);
								ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));							
								break;
							}
						}
					}
				}
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			connect.commit();
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
				String schema = getPhisicalSchema(restoreTable.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String tblName = schema+"."+restoreTable.getTable().getName();				
				Map<String, DBTable> tables = adapter.getTables(schema);
				boolean exist = false;
				if(!(tables == null || tables.isEmpty() )) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equals(table.getName())){
							exist = true;
							Map<String, DBTableField> currentFileds = adapter.getTableFields(schema, restoreTable.getTable().getName());
							MapDifference<String, DBTableField> diffTableFields = Maps.difference(restoreTable.getFields(),currentFileds);
							
							if(!diffTableFields.entriesOnlyOnLeft().isEmpty()){
								for(DBTableField tblField:diffTableFields.entriesOnlyOnLeft().values()) {
										st.execute("alter table "+ tblName +" add column " + tblField.getName()  + " " + tblField.getTypeSQL());

										if (tblField.getDescription() != null && tblField.getDescription().length() > 0)
											st.execute("COMMENT ON COLUMN " + tblName + "." + ((adapter.isReservedWord(tblField.getName()) || tblField.getNameExactly()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " IS '" + tblField.getDescription() + "'");
										
										if (!tblField.getIsNullable()) {
											st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " set not null");
										}										
										
										if (tblField.getDefaultValue() != null && tblField.getDefaultValue().length() > 0) {
											st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) 
													+ " SET DEFAULT " + tblField.getDefaultValue());
										}										
								}								
							}
							
							if(!diffTableFields.entriesOnlyOnRight().isEmpty()) {
								for(DBTableField tblField:diffTableFields.entriesOnlyOnRight().values()) {
									st.execute("alter table "+ tblName +" drop column IF EXISTS "+ tblField.getName());
								}								
							}
							
							if(!diffTableFields.entriesDiffering().isEmpty()) {						
								for(ValueDifference<DBTableField> tblField:diffTableFields.entriesDiffering().values()) {
									if(!tblField.leftValue().getName().equals(tblField.rightValue().getName())) {
										st.execute("alter table "+ tblName +" rename column "+ tblField.rightValue().getName() +" to "+ tblField.leftValue().getName());
									}
																	
									if(!tblField.leftValue().getTypeSQL().equals(tblField.rightValue().getTypeSQL())) {
										st.execute("alter table "+ tblName +" alter column "+ tblField.leftValue().getName() +" type "+ tblField.leftValue().getTypeSQL());
									}
								}								
							}						
						}						
					}
				}
				if(!exist){								
					for(DBTableField tblField:restoreTable.getFields().values()) {
							st.execute("alter table "+ tblName +" add column " + tblField.getName()  + " " + tblField.getTypeSQL());
							
							if (tblField.getDescription() != null && tblField.getDescription().length() > 0)
								st.execute("COMMENT ON COLUMN " + tblName + "." + ((adapter.isReservedWord(tblField.getName()) || tblField.getNameExactly()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " IS '" + tblField.getDescription() + "'");

							if (!tblField.getIsNullable()) {
								st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) + " set not null");
							}										
							
							if (tblField.getDefaultValue() != null && tblField.getDefaultValue().length() > 0) {
								st.execute("alter table " + tblName + " alter column " + (adapter.isReservedWord(tblField.getName()) ? "\"" + tblField.getName() + "\" " : tblField.getName()) 
										+ " SET DEFAULT " + tblField.getDefaultValue());
							}										

					}
				}
				
				ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraintscount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+tblName+"'::regclass");
				rs.next();
				Integer constraintsCount = Integer.valueOf(rs.getString("constraintscount"));
				if(constraintsCount.intValue()>0) {
					removeTableConstraintsPostgres(obj);
				}
				// set primary key
				for(DBConstraint tableconst: restoreTable.getConstraints().values()) {
					if(tableconst.getConstraintType().equals("p")) {
						st.execute("alter table "+ tblName +" add constraint "+ DBAdapterPostgres.escapeNameIfNeeded(tableconst.getName()) + " "+tableconst.getSql().replace(" " + tableconst.getSql() + ".", " " + schema + "."));
						break;
					}
				}
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}			
	}
	public void restoreTableIndexesPostgres(IMetaObject obj) throws Exception
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
				if(!(tables == null || tables.isEmpty()) ) {
					for(DBTable table:tables.values()) {
						if(restoreTable.getTable().getName().equals(table.getName())){
							exist = true;
														
							Map<String, DBIndex> currentIndexes = adapter.getIndexes(schema, table.getName());
							MapDifference<String, DBIndex> diffInd = Maps.difference(restoreTable.getIndexes(), currentIndexes);
							if(!diffInd.entriesOnlyOnLeft().isEmpty()) {
								for(DBIndex ind:diffInd.entriesOnlyOnLeft().values()) {
									if(ind.getOptions().getChildren().containsKey("tablespace")) {
										st.execute(ind.getSql().replace(" INDEX ", " INDEX IF NOT EXISTS ")+" tablespace "+ind.getOptions().get("tablespace").getData());
									}
									else {
										st.execute(ind.getSql().replace(" INDEX ", " INDEX IF NOT EXISTS "));
									}
								}								
							}
							if(!diffInd.entriesOnlyOnRight().isEmpty()) {
								for(DBIndex ind:diffInd.entriesOnlyOnRight().values()) {
									st.execute("drop index IF EXISTS "+schema+"."+ DBAdapterPostgres.escapeNameIfNeeded(ind.getName()));
								}								
							}
							
							if(!diffInd.entriesDiffering().isEmpty()) {
								for(ValueDifference<DBIndex>  ind:diffInd.entriesDiffering().values()) {
									if(ind.leftValue().getOptions().getChildren().containsKey("tablespace")) {
										if(ind.rightValue().getOptions().getChildren().containsKey("tablespace") && !ind.leftValue().getOptions().get("tablespace").getData().equals(ind.rightValue().getOptions().get("tablespace").getData())) {
											st.execute(
											"alter index "
												+schema+"."+DBAdapterPostgres.escapeNameIfNeeded(ind.leftValue().getName())
												+" set tablespace "+ind.leftValue().getOptions().get("tablepace")
											);
										}
									}
									else if(ind.rightValue().getOptions().getChildren().containsKey("tablespace")) {
										st.execute("alter index "+schema+"."+DBAdapterPostgres.escapeNameIfNeeded(ind.leftValue().getName()) +" set tablespace pg_default");
									}									
								}								
							}										
						}						
					}
				}
				if(!exist){								
					ConsoleWriter.println("count of indexes: " + restoreTable.getIndexes().size());					
					
					for(DBIndex ind:restoreTable.getIndexes().values()) {

						if(ind.getOptions().getChildren().containsKey("tablespace")) {
							st.execute(ind.getSql().replace(" INDEX ", " INDEX IF NOT EXISTS ")+" tablespace "+ind.getOptions().get("tablespace").getData());
						}
						else {						
							st.execute(ind.getSql().replace(" INDEX ", " INDEX IF NOT EXISTS "));
						}						
					}
				}
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}			
	}
	public void restoreTableConstraintPostgres(IMetaObject obj) throws Exception {
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
					if(!constrs.getConstraintType().equalsIgnoreCase("p")) {				
						st.execute(
						"alter table "+ schema+"."+DBAdapterPostgres.escapeNameIfNeeded(restoreTable.getTable().getName())
							+ " add constraint " + DBAdapterPostgres.escapeNameIfNeeded(constrs.getName()) + " " + constrs.getSql()
							.replace(" " + constrs.getSchema() + ".", " " + schema + ".")
							.replace("REFERENCES ", "REFERENCES " + schema + ".")
						);
					}
				}
			}
			else
			{
				ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}						
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}			
	}
	public void removeTableConstraintsPostgres(IMetaObject obj) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {			
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;
				String schema = getPhisicalSchema(table.getTable().getSchema());
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				//String s = "SELECT COUNT(*) as constraintscount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+table.getTable().getSchema()+"."+table.getTable().getName()+"'::regclass";
				//ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraintscount FROM pg_catalog.pg_constraint r WHERE r.conrelid = '"+table.getTable().getSchema()+"."+table.getTable().getName()+"'::regclass");
				//rs.next();
				//Integer constraintsCount = Integer.valueOf(rs.getString("constraintscount"));
				//if(constraintsCount.intValue()>0) {
				Map<String, DBConstraint> constraints = table.getConstraints();
				for(DBConstraint constrs :constraints.values()) {
					if (!constrs.getConstraintType().equalsIgnoreCase("p"))
						st.execute(
						"alter table "+ schema+"."+DBAdapterPostgres.escapeNameIfNeeded(table.getTable().getName())
							+" drop constraint IF EXISTS "+DBAdapterPostgres.escapeNameIfNeeded(constrs.getName())
						);
				}
				//}
			}
			else
			{
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
			}	
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()), e);
		}		
	}
	
	/*public void removeIndexesPostgres(IMetaObject obj) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {			
			if (obj instanceof MetaTable) {
				MetaTable table = (MetaTable)obj;				
				Map<String, DBIndex> indexes = table.getIndexes();
				for(DBIndex index :indexes.values()) {
					st.execute("DROP INDEX IF EXISTS "+index.getName());
				}			
			}
			else
			{
				throw new ExceptionDBGitRestore("Error restore: Unable to remove TableIndexes.");
			}	
		}
		catch(Exception e) {
			throw new ExceptionDBGitRestore("Error restore "+obj.getName(), e);
		}		
	}*/
	
	public void removeMetaObject(IMetaObject obj) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		
		try {			
			MetaTable tblMeta = (MetaTable)obj;
			DBTable tbl = tblMeta.getTable();
			
			if (tbl == null) return;			
			
			String schema = getPhisicalSchema(tbl.getSchema());
			schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
			st.execute("DROP TABLE IF EXISTS "+schema+"."+DBAdapterPostgres.escapeNameIfNeeded(tbl.getName()));
		
			// TODO Auto-generated method stub
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRemoveError").withParams(obj.getName()), e);
		} finally {
			st.close();
		}
	}

}
