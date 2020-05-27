package ru.fusionsoft.dbgit.postgres;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.data_table.BooleanData;
import ru.fusionsoft.dbgit.data_table.DateData;
import ru.fusionsoft.dbgit.data_table.ICellData;
import ru.fusionsoft.dbgit.data_table.LongData;
import ru.fusionsoft.dbgit.data_table.MapFileData;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.data_table.StringData;
import ru.fusionsoft.dbgit.data_table.TextFileData;
import ru.fusionsoft.dbgit.data_table.TreeMapRowData;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.statement.PrepareStatementLogging;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

import com.google.common.collect.MapDifference;
import com.google.common.collect.MapDifference.ValueDifference;
import com.google.common.collect.Maps;

public class DBRestoreTableDataPostgres extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		if (obj instanceof MetaTableData) {	
			MetaTableData currentTableData;
			MetaTableData restoreTableData = (MetaTableData)obj;
			GitMetaDataManager gitMetaMng = GitMetaDataManager.getInstance();
			//TODO не факт что в кеше есть мета описание нашей таблицы, точнее ее не будет если при старте ресторе таблицы в бд не было совсем
			
			IMetaObject currentMetaObj = gitMetaMng.getCacheDBMetaObject(obj.getName());
			
			if (currentMetaObj instanceof MetaTableData || currentMetaObj == null) {				
				
				if(Integer.valueOf(step).equals(0)) {
					removeTableConstraintsPostgres(restoreTableData.getMetaTable());
					return false;
				}
				if(Integer.valueOf(step).equals(1)) {
					String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());					
					schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
					
					if (currentMetaObj != null) {
						currentTableData = (MetaTableData) currentMetaObj;
					} else {					
						currentTableData = new MetaTableData();
						currentTableData.setTable(restoreTableData.getTable());
						currentTableData.getTable().setSchema(schema);
						
						currentTableData.setMapRows(new TreeMapRowData());
						currentTableData.setDataTable(restoreTableData.getDataTable());
					}
					currentTableData.getmapRows().clear();
				
					if (getAdapter().getTable(schema, currentTableData.getTable().getName()) != null) {
						currentTableData.setDataTable(getAdapter().getTableData(schema, currentTableData.getTable().getName()));
					
						ResultSet rs = currentTableData.getDataTable().getResultSet();
						
						TreeMapRowData mapRows = new TreeMapRowData(); 
						
						MetaTable metaTable = new MetaTable(currentTableData.getTable());
						metaTable.loadFromDB(currentTableData.getTable());

						if (rs != null) {
							while(rs.next()) {
								RowData rd = new RowData(rs, metaTable);
								mapRows.put(rd.calcRowKey(metaTable.getIdColumns()), rd);
							}
						}
						currentTableData.setMapRows(mapRows);
					}
					/*
					ConsoleWriter.println("curr:");
					currentTableData.getmapRows().keySet().forEach(key -> ConsoleWriter.println("hash: " + key));
					ConsoleWriter.println("rest:");
					restoreTableData.getmapRows().keySet().forEach(key -> ConsoleWriter.println("hash: " + key));
					*/
					restoreTableDataPostgres(restoreTableData,currentTableData);
					return false;
				}
				if(Integer.valueOf(step).equals(-2)) {
					restoreTableConstraintPostgres(restoreTableData.getMetaTable());
					return false;
				}
				return true;
			}
			else
			{
				//TODO WTF????
				throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
				//return true;
			}					
		}
		else
		{
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
		}		
	}
	
	public void restoreTableDataPostgres(MetaTableData restoreTableData, MetaTableData currentTableData) throws Exception{	
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {			
			if (restoreTableData.getmapRows() == null)
				restoreTableData.setMapRows(new TreeMapRowData());
			
			String fields = "";
			if (restoreTableData.getmapRows().size() > 0) {
				//fields = keysToString(restoreTableData.getmapRows().firstEntry().getValue().getData().keySet().stream().map(DBAdapterPostgres::escapeNameIfNeeded).collect(Collectors.toSet())) + " values ";

				Comparator<DBTableField> comparator = Comparator.comparing(DBTableField::getOrder);
				fields = "(" + restoreTableData.getMetaTableFromFile().getFields().entrySet().stream()
						.sorted(Comparator.comparing(e -> e.getValue().getOrder()))
						.map(entry -> DBAdapterPostgres.escapeNameIfNeeded(entry.getValue().getName()))
						.collect(Collectors.joining(", "))
						+ ") values ";

			}

			MapDifference<String, RowData> diffTableData = Maps.difference(restoreTableData.getmapRows(),currentTableData.getmapRows());
			String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());
			
			schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
			String tblNameUnescaped = schema + "." + restoreTableData.getTable().getName();
			String tblNameEscaped = schema + "." + DBAdapterPostgres.escapeNameIfNeeded(restoreTableData.getTable().getName());

			ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "tableData").withParams(tblNameUnescaped) + "\n", 1);
			
			ResultSet rsTypes = st.executeQuery("select column_name, data_type from information_schema.columns \r\n" + 
					"where lower(table_schema||'.'||table_name) = lower('" + tblNameUnescaped + "')");

			HashMap<String, String> colTypes = new HashMap<String, String>();
			while (rsTypes.next()) {
				colTypes.put(rsTypes.getString("column_name"), rsTypes.getString("data_type"));
			}
			
			
			if(!diffTableData.entriesOnlyOnLeft().isEmpty()) {
				
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "inserting"), 2);
				
				for(RowData rowData:diffTableData.entriesOnlyOnLeft().values()) {
					ArrayList<String> fieldsList = new ArrayList<String>(rowData.getData().keySet().stream().map(DBAdapterPostgres::escapeNameIfNeeded).collect(Collectors.toList()));

					String insertQuery = "insert into " + tblNameEscaped +
							fields+valuesToString(rowData.getData().values(), colTypes, fieldsList) + ";\n";
					
					ConsoleWriter.detailsPrintLn(insertQuery);
					
					PrepareStatementLogging ps = new PrepareStatementLogging(connect, insertQuery, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
					int i = 0;
										
					for (ICellData data : rowData.getData().values()) {
						i++;
						ConsoleWriter.detailsPrintLn(data.getSQLData());						
						
						ResultSet rs = st.executeQuery("select data_type from information_schema.columns \r\n" + 
								"where lower(table_schema||'.'||table_name) = lower('" + tblNameUnescaped + "') and lower(column_name) = '" + fieldsList.get(i - 1) + "'");
						
						boolean isBoolean = false;
						while (rs.next()) {							
							if (rs.getString("data_type").contains("boolean")) {
								isBoolean = true;
							}
						}

						//ps = setValues(data, i, ps, isBoolean);
					}
					
					//if (adapter.isExecSql())
					//	ps.execute();
					//ps.close();
					
					st.execute(insertQuery);
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			
			if(!diffTableData.entriesOnlyOnRight().isEmpty()){
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "deleting"), 2);
				String deleteQuery="";
				Map<String,String> primarykeys = new HashMap();
				for(RowData rowData:diffTableData.entriesOnlyOnRight().values()) {
					Map<String, ICellData> tempcols = rowData.getData();
					String[] keysArray = rowData.getKey().split("_");
					for(String key:keysArray) {
						for (String o : tempcols.keySet()) {
							if (tempcols.get(o) == null || tempcols.get(o).convertToString() == null) continue;
							if (tempcols.get(o).convertToString().equals(key)) {
						       primarykeys.put(o, tempcols.get(o).convertToString());
						       tempcols.remove(o);
						       break;
						    }
						}
					}
					String delFields="(";
					String delValues="(";	
					StringJoiner fieldJoiner = new StringJoiner(",");
					StringJoiner valuejoiner = new StringJoiner(",");	
					for (Map.Entry<String, String> entry : primarykeys.entrySet()) {																	
						fieldJoiner.add("\""+entry.getKey()+"\"");
						valuejoiner.add("\'"+entry.getValue()+"\'");												
					}
					delFields+=fieldJoiner.toString()+")";
					delValues+=valuejoiner.toString()+")";
					primarykeys.clear();
					if (delValues.length() > 3)
						deleteQuery+="delete from " + tblNameUnescaped+
							" where " + delFields + " = " + delValues + ";\n";
					if(deleteQuery.length() > 50000 ){
						st.execute(deleteQuery);
						deleteQuery = "";
					}
				}
				if(deleteQuery.length()>1) {
					st.execute(deleteQuery);
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			
			if(!diffTableData.entriesDiffering().isEmpty()) {
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "updating"), 2);
				String updateQuery="";
				Map<String,String> primarykeys = new HashMap();
				for(ValueDifference<RowData> diffRowData:diffTableData.entriesDiffering().values()) {	
					if(!diffRowData.leftValue().getHashRow().equals(diffRowData.rightValue().getHashRow())) {
						Map<String, ICellData> tempCols = diffRowData.leftValue().getData();
						String[] keysArray = diffRowData.leftValue().getKey().split("_");
						for(String key:keysArray) {
							for (String o : tempCols.keySet()) {
								if (tempCols.get(o) == null || tempCols.get(o).convertToString() == null) continue;
								if (tempCols.get(o).convertToString().equals(key)) {
									primarykeys.put(o, tempCols.get(o).convertToString());
									tempCols.remove(o);
									break;
							    }
							}
						}
						if(!tempCols.isEmpty()) {
							String keyFields="(";
							String keyValues="(";	
							StringJoiner fieldJoiner = new StringJoiner(",");
							StringJoiner valuejoiner = new StringJoiner(",");	
							for (Map.Entry<String, String> entry : primarykeys.entrySet()) {																	
								fieldJoiner.add("\""+entry.getKey()+"\"");
								valuejoiner.add("\'"+entry.getValue()+"\'");												
							}
							keyFields+=fieldJoiner.toString()+")";
							keyValues+=valuejoiner.toString()+")";
							primarykeys.clear();
							
							StringJoiner updfieldJoiner = new StringJoiner(",");
							StringJoiner updvaluejoiner = new StringJoiner(",");
							String updFields="(";
							String updValues="(";
							
							for (Map.Entry<String, ICellData> entry : tempCols.entrySet()) {																	
								updfieldJoiner.add("\""+entry.getKey()+"\"");
								//updvaluejoiner.add("\'"+entry.getValue().convertToString()+"\'");
								//updvaluejoiner.add("?");
							}
							
							ArrayList<String> fieldsList = new ArrayList<String>(diffRowData.leftValue().getData().keySet());
							
							updFields+=updfieldJoiner.toString()+")";
							updValues+=updvaluejoiner.toString()+")";							
							
							updateQuery="update "+tblNameEscaped+
									" set "+updFields + " = " + valuesToString(tempCols.values(), colTypes, fieldsList) + " where " + keyFields+ "=" +keyValues+";\n";							
							
							ConsoleWriter.detailsPrintLn(updateQuery);
							
							PrepareStatementLogging ps = new PrepareStatementLogging(connect, updateQuery, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
							int i = 0;
							
							ConsoleWriter.detailsPrintLn("vals for " + keyValues + ":" + diffRowData.leftValue().getData().values());
							for (ICellData data : diffRowData.leftValue().getData().values()) {
								i++;
								ConsoleWriter.detailsPrintLn(data.getSQLData());						
								
								ResultSet rs = st.executeQuery("select data_type from information_schema.columns \r\n" + 
										"where lower(table_schema||'.'||table_name) = lower('" + tblNameUnescaped + "') and lower(column_name) = '" + fieldsList.get(i - 1) + "'");
								
								boolean isBoolean = false;
								while (rs.next()) {							
									if (rs.getString("data_type").toLowerCase().contains("boolean")) {
										isBoolean = true;
									}
								}
								//ps = setValues(data, i, ps, isBoolean);
							}
							/*
							if (adapter.isExecSql())
								ps.execute();
							ps.close();
							updateQuery = "";
							*/
							
							
							
							//if(updateQuery.length() > 50000 ){
								st.execute(updateQuery);
								updateQuery = "";
							//}
							
						}
						
					}
				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
				if(updateQuery.length()>1) {
					ConsoleWriter.println(updateQuery);
					st.execute(updateQuery);
				}
			}			
			
		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(restoreTableData.getTable().getSchema() + "." + restoreTableData.getTable().getName()), e);
		} finally {
			st.close();
		}
	}
	
	public String valuesToString(Collection<ICellData> datas, HashMap<String, String> colTypes, ArrayList<String> fieldsList) throws ExceptionDBGit, IOException {
		String values="(";
		StringJoiner joiner = new StringJoiner(",");
		int i = 0;
		for (ICellData data : datas) {		
			boolean isBoolean = ((colTypes.get(fieldsList.get(i)) != null) && (colTypes.get(fieldsList.get(i)).toLowerCase().contains("boolean")));
			
			if (data instanceof TextFileData) {
				if (((TextFileData) data).getFile() == null || ((TextFileData) data).getFile().getName().contains("null")) {
					joiner.add("null");
				} else {				
					FileInputStream fis = new FileInputStream(((MapFileData) data).getFile());	
					BufferedReader br = new BufferedReader(new InputStreamReader(fis));
					
					StringBuilder sb = new StringBuilder();
				    String line;
				    while(( line = br.readLine()) != null ) {
				    	sb.append( line );
				    	sb.append( '\n' );
				    }
					br.close();
					
					fis.close();
					String res = "'" + sb.toString().replace("'", "''")
							.replace("\\", "\\\\")
							.replace("\n", "' || chr(10) || '")
							.replace("\0", "' || '\\000' || '")
							+ "'";
					
					if (res.endsWith(" || chr(10) || ''")) {
						res = res.substring(0, res.length() - " || chr(10) || ''".length());	
					}
					joiner.add(res);
					
					
				}
			} else if (data instanceof MapFileData) {
				if (((MapFileData) data).getFile() == null || ((MapFileData) data).getFile().getName().contains("null")) {
					joiner.add("null");
				} else {
			
					FileInputStream fis = new FileInputStream(((MapFileData) data).getFile());	
					
					int byteRead;
					StringBuilder sb = new StringBuilder();
				    
		            while ((byteRead = fis.read()) != -1) {
		            	String hex = Integer.toHexString(byteRead);
		            	if (hex.length() == 1) hex = "0" + hex;
		            	
		            	sb.append(hex);
		            }
		            fis.close();
		            joiner.add("decode('" + sb.toString() + "', 'hex')");
		            
		            /*
					joiner1.add("decode('" + sb.toString().replace("'", "''")
							.replace("\\", "\\\\")
							.replace("\n", "' || chr(10) || '")
							.replace("\0", "' || '\\000' || '")
							+ "', 'escape')");*/
				}
			} else if (data instanceof DateData) { 
				Date date = ((DateData) data).getDate();
				SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
				if (date != null) 
					joiner.add("TO_TIMESTAMP('" + format.format(date) + "', 'YYYYMMDDHH24MISS')");
				else
					joiner.add("null");
			} else if (data instanceof BooleanData) {
				if (((BooleanData) data).getValue() != null)								
					joiner.add(((BooleanData) data).getValue().toString());
				else
					joiner.add("null");
			} else if (data instanceof LongData) {
				String dt = data.getSQLData().replace("'", "");
				
				if (isBoolean) {
					if (dt == null || dt.equals(""))
						joiner.add("null");
					else if (dt.equals("1"))
						joiner.add("true");
					else
						joiner.add("false");
				} else {							
					if (!dt.equals("")) 
						joiner.add(dt);
					else
						joiner.add("null");
				}
			} else {
				String dt = ((StringData) data).getValue();
				if (isBoolean) {
					if (dt == null || dt.equals(""))
						joiner.add("null");
					else if (dt.startsWith("t") || dt.startsWith("T") || dt.equals("1") || dt.startsWith("y") || dt.startsWith("Y"))
						joiner.add("true");								
					else
						joiner.add("false");
				} else {							
					if (dt != null)								
						joiner.add("'" + dt.replace("'", "''") + "'");
					else
						joiner.add("null");
				}	
				//joiner.add(data.getSQLData());
				
			}			
			
			//joiner.add("?");
			i++;
		}
		//ConsoleWriter.println("joiner: " + joiner1);
		values+=joiner.toString()+")";
		return values;		
	}
	
	public String keysToString(Set<String> keys) {
		String fields="";
		if(keys.size()>1) {
			String[] fieldsArray = keys.toArray(new String[keys.size()]);	
			fields="("+(fieldsArray[0].equals(fieldsArray[0].toLowerCase()) ? fieldsArray[0] : "\"" + fieldsArray[0] + "\"");
			for(int i=1;i<fieldsArray.length;i++) {
				fields+=","+(fieldsArray[i].equals(fieldsArray[i].toLowerCase()) ? fieldsArray[i] : "\"" + fieldsArray[i] + "\"");
			}
			fields+=")";
		}
		else {
			String[] fieldsArray = keys.toArray(new String[keys.size()]);	
			fields="("+fieldsArray[0]+")";
		}
		return fields;		
	}

	public void restoreTableConstraintPostgres(MetaTable table) throws Exception {
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreConstr").withParams(table.getName()), 1);
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
		try {	
				for(DBConstraint constrs :table.getConstraints().values()) {
					if(!constrs.getConstraintType().equals("p")) {				
					st.execute("alter table "+schema+"."+ table.getTable().getName() +" add constraint "+ constrs.getName() + " "+constrs.getOptions().get("ddl").toString());
					}
				}						
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(schema + "." + table.getTable().getName()), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			st.close();
		}			
	}
	
	public void removeTableConstraintsPostgres(MetaTable table) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		StatementLogging stCnt = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
		String tblName = schema + "." +table.getTable().getName();
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "delConstr").withParams(table.getName()), 1);
		try {					
			ResultSet rs = stCnt.executeQuery("SELECT *\r\n" + 
					"       FROM pg_catalog.pg_constraint con\r\n" + 
					"            INNER JOIN pg_catalog.pg_class rel\r\n" + 
					"                       ON rel.oid = con.conrelid\r\n" + 
					"            INNER JOIN pg_catalog.pg_namespace nsp\r\n" + 
					"                       ON nsp.oid = connamespace\r\n" + 
					"       WHERE upper(nsp.nspname) = upper('" + schema + "')\r\n" + 
					"             AND upper(rel.relname) = upper('" + table.getTable().getName() + "')");
			//rs.next();
			//Integer constraintsCount = Integer.valueOf(rs.getString("constraints_count"));
			//ConsoleWriter.println("cnstrCnt = " + constraintsCount);
			//if(constraintsCount.intValue()>0) {
			//	Map<String, DBConstraint> constraints = table.getConstraints();
				/*
				for(DBConstraint constrs :constraints.values()) {
					if(!constrs.getConstraintType().equals("p")) {
						st.execute("alter table "+schema+"."+ table.getTable().getName() +" drop constraint if exists "+constrs.getName());
					}
				}*/
				
				while (rs.next()) {
					if(!rs.getString("contype").equals("p")) {
						st.execute("alter table "+schema+"."+ table.getTable().getName() +" drop constraint if exists "+rs.getString("conname"));
					}					
				}
			//}	
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));

		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "cannotRestore").withParams(schema + "." + table.getTable().getName()), e);
		}		
	}
	
	private PrepareStatementLogging setValues(ICellData data, int i, PrepareStatementLogging ps, boolean isBoolean) throws Exception {
		if (data instanceof TextFileData) {
			File file = ((TextFileData) data).getFile();
			if (file.exists()) {
				FileInputStream fis = new FileInputStream(file);
				BufferedReader br = new BufferedReader(new InputStreamReader(fis));
				
				StringBuilder sb = new StringBuilder();
			    String line;
			    while(( line = br.readLine()) != null ) {
			    	sb.append( line );
			    	sb.append( '\n' );
			    }
				ps.setString(i, sb.toString());
				br.close();
			} else {
				ps.setNull(i, java.sql.Types.NULL);
			}
		} else 	if (data instanceof MapFileData) {
			File file = ((MapFileData) data).getFile();
			if (file.exists()) {
				FileInputStream fis = new FileInputStream(file);						
				ps.setBinaryStream(i, fis, file.length());
			} else {
				ps.setNull(i, java.sql.Types.NULL);
			}
		} else if (data instanceof LongData) {
			String dt = data.getSQLData().replace("'", "");
			
			if (isBoolean) {
				if (dt == null || dt.equals(""))
					ps.setNull(i, java.sql.Types.NULL);
				else if (dt.equals("1"))
					ps.setBoolean(i, true);
				else
					ps.setBoolean(i, false);
			} else {							
				if (!dt.equals("")) 
					ps.setDouble(i, Double.parseDouble(dt));
				else
					ps.setNull(i, java.sql.Types.NULL);
			}
		} else if (data instanceof DateData) {
			if (((DateData) data).getDate() != null)								
				ps.setDate(i, ((DateData) data).getDate());
			else
				ps.setNull(i, java.sql.Types.NULL);								
		} else if (data instanceof BooleanData) {
			if (((BooleanData) data).getValue() != null)								
				ps.setBoolean(i, ((BooleanData) data).getValue());
			else
				ps.setNull(i, java.sql.Types.NULL);
		} else {
			String dt = ((StringData) data).getValue();
			if (isBoolean) {
				if (dt == null || dt.equals(""))
					ps.setNull(i, java.sql.Types.NULL);
				else if (dt.startsWith("t") || dt.startsWith("T") || dt.equals("1") || dt.startsWith("y") || dt.startsWith("Y"))
					ps.setBoolean(i, true);									
				else
					ps.setBoolean(i, false);
			} else {							
				if (dt != null)								
					ps.setString(i, dt);
				else
					ps.setNull(i, java.sql.Types.NULL);
			}
		}
		
		return ps;
	}
	
	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
