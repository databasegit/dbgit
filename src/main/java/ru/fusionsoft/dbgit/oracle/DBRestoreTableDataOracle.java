package ru.fusionsoft.dbgit.oracle;

import java.io.BufferedReader;
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
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import com.google.common.collect.MapDifference;
import com.google.common.collect.Maps;
import com.google.common.collect.MapDifference.ValueDifference;

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
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.statement.PrepareStatementLogging;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreTableDataOracle extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {

		if (obj instanceof MetaTableData) {
			MetaTableData restoreTableData = (MetaTableData) obj;
			
			String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());					
			schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
			
			IMetaObject currentMetaObj = GitMetaDataManager.getInctance().getCacheDBMetaObject(obj.getName());
			MetaTableData currentTableData = (MetaTableData) currentMetaObj;
						
			if(Integer.valueOf(step).equals(0)) {
				removeTableConstraintsOracle(restoreTableData.getMetaTable());
				return false;
			}
			if(Integer.valueOf(step).equals(1)) {
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

					ConsoleWriter.println("ids: " + metaTable.getIdColumns());
					
					if (rs != null) {
						while(rs.next()) {
							RowData rd = new RowData(rs, metaTable);
							mapRows.put(rd.calcRowKey(metaTable.getIdColumns()), rd);
						}
					}
					currentTableData.setMapRows(mapRows);
				}
				
				restoreTableDataOracle(restoreTableData, currentTableData);
				return false;
			}
			if(Integer.valueOf(step).equals(2)) {
				restoreTableConstraintOracle(restoreTableData.getMetaTable());
			}
			
			return true;
		} else {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(obj.getName()));
		}
	}

	private void restoreTableDataOracle(MetaTableData restoreTableData, MetaTableData currentTableData) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			String tblName = getPhisicalSchema(restoreTableData.getTable().getSchema()) + "." + restoreTableData.getTable().getName();

			ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "tableData").withParams(tblName) + "\n", 1);

			String insertQuery= "";
			
			if (restoreTableData.getmapRows() == null)
				restoreTableData.setMapRows(new TreeMapRowData());
			
			String fields = "";
			if (restoreTableData.getmapRows().size() > 0)
				fields = "(" + restoreTableData.getmapRows().firstEntry().getValue().getData().keySet().stream()
					.map(d -> adapter.isReservedWord(d.toString()) ? "\"" + d.toString() + "\"" : d.toString())
					.collect(Collectors.joining(",")) + ")";
			MapDifference<String, RowData> diffTableData = Maps.difference(restoreTableData.getmapRows(), currentTableData == null ? new TreeMap<String, RowData>() : currentTableData.getmapRows());

			ResultSet rsTypes = st.executeQuery("select column_name, data_type from ALL_TAB_COLUMNS \r\n" + 
					"where lower(owner||'.'||table_name) = lower('" + tblName + "')");

			HashMap<String, String> colTypes = new HashMap<String, String>();
			while (rsTypes.next()) {
				colTypes.put(rsTypes.getString("column_name"), rsTypes.getString("data_type"));
			}
			
			if(!diffTableData.entriesOnlyOnLeft().isEmpty()) {
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "inserting"), 2);
				for(RowData rowData:diffTableData.entriesOnlyOnLeft().values()) {
					ArrayList<String> fieldsList = new ArrayList<String>(rowData.getData().keySet());
					
					String values = 
							rowData.getData().values().stream()
							//.map(d -> d.getSQLData())
							.map(d -> "?")
							.collect(Collectors.joining(","));				
					insertQuery = "insert into "+tblName +
							fields + " values " + valuesToString(rowData.getData().values(), colTypes, fieldsList, true);
					boolean needSeparator = false;
					for (ICellData data : rowData.getData().values()) {
						if (data instanceof MapFileData) {
							needSeparator = true;
							insertQuery = getBlobQuery(rowData.getData().values(), insertQuery);
							break;
						}
					}
					
					ConsoleWriter.detailsPrintLn(insertQuery);
					if (needSeparator)
						st.execute(insertQuery, "/");
					else
						st.execute(insertQuery);

				}
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));

			}
			if(!diffTableData.entriesOnlyOnRight().isEmpty()) {
				boolean isSuccessful = true;
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "deleting"), 2);

				for(RowData rowData : diffTableData.entriesOnlyOnRight().values()) {
					Map<String,String> primarykeys = getKeys(rowData);
					
					if (primarykeys.size() == 0) {
						ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
						ConsoleWriter.printlnRed(lang.getValue("errors", "restore", "pkNotFound").withParams(tblName));
						isSuccessful = false;
						break;
					}
					
					String delParams = primarykeys.entrySet().stream()
						.map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
						.collect(Collectors.joining(" and "));
					
					String deleteQuery = "delete from " + tblName + " where " + delParams;

					st.execute(deleteQuery);					
				}		
				if (isSuccessful) ConsoleWriter.detailsPrintlnGreen("OK");

			}
			
			if(!diffTableData.entriesDiffering().isEmpty()) {
				boolean isSuccessful = true;
				ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "updating"), 2);
				for (ValueDifference<RowData> diffRowData:diffTableData.entriesDiffering().values()) {
					if (!diffRowData.leftValue().getHashRow().equals(diffRowData.rightValue().getHashRow())) {
						Map<String,String> primarykeys = getKeys(diffRowData.leftValue());
						
						if (primarykeys.size() == 0) {
							ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
							ConsoleWriter.printlnRed(lang.getValue("general", "restore", "pkNotFound").withParams(tblName));
							isSuccessful = false;
							break;
						}
						
						String updParams = primarykeys.entrySet().stream()
								.map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
								.collect(Collectors.joining(" and "));

						String updValues = diffRowData.leftValue().getData().entrySet().stream()
								.filter(entry -> !primarykeys.containsKey(entry.getKey()))
								.map(entry -> entry.getKey() + " = " + entry.getValue().getSQLData())
								.collect(Collectors.joining(", "));						
						
						ArrayList<String> fieldsList = new ArrayList<String>(diffRowData.leftValue().getData().keySet());
						
						if (updValues.length() > 0) {
							String updateQuery = "update " + tblName + " set " + valuesToString(diffRowData.leftValue().getData().values(), colTypes, fieldsList, false) + " where " + updParams;
							
							boolean needSeparator = false;
							for (ICellData data : diffRowData.leftValue().getData().values()) {
								if (data instanceof MapFileData) {
									needSeparator = true;
									updateQuery = getBlobQuery(diffRowData.leftValue().getData().values(), updateQuery);
									break;
								}
							}
							
							if (needSeparator)
								st.execute(updateQuery, "/");
							else
								st.execute(updateQuery);
						}
						
												
					}
				}		
				if (isSuccessful) ConsoleWriter.detailsPrintlnGreen("OK");
			}			

		} catch (Exception e) {
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(restoreTableData.getTable().getSchema() + "." + restoreTableData.getTable().getName()), e);
		} finally {
			st.close();
		}
	}
	
	private Map<String, String> getKeys(RowData rowData) {
		Map<String,String> primarykeys = new HashMap<String,String>();
		Map<String, ICellData> tempCols = rowData.getData();
		String[] keysArray = rowData.getKey().split("_");
        
        Stream.of(keysArray).forEach(key -> {
			for (Entry<String, ICellData> entry : tempCols.entrySet()){
				try {
					if (entry.getValue().convertToString().equals(key)) {
						primarykeys.put(entry.getKey(), entry.getValue().convertToString());
					}
				} catch (Exception e) {
					ConsoleWriter.printlnRed(lang.getValue("general", "restore", "pkNotFound").withParams(""));
					ConsoleWriter.printlnRed(e.getMessage());
				} 
			}			        	
        });		
		
		return primarykeys;
	}

	
	private void restoreTableConstraintOracle(MetaTable table) throws Exception {
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "restoreConstr").withParams(table.getName()), 1);
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
		try {	
			for(DBConstraint constraint : table.getConstraints().values()) {
				if(!constraint.getConstraintType().equalsIgnoreCase("p")) {				
					String query = "alter table " + schema + "." + table.getTable().getName() 
							+ " add constraint "+ constraint.getName() + " " +constraint.getOptions().get("ddl").toString();
					ConsoleWriter.println(query);
					st.execute(query);
				}
			}					
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(table.getTable().getName()), e);
		} finally {
			st.close();
		}			
	}

	private void removeTableConstraintsOracle(MetaTable table) throws Exception {
		ConsoleWriter.detailsPrint(lang.getValue("general", "restore", "delConstr").withParams(table.getName()), 1);
		IDBAdapter adapter = getAdapter();
		StatementLogging st = new StatementLogging(adapter.getConnection(), adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
		try {		
			for (DBConstraint constraint : adapter.getConstraints(schema, table.getTable().getName()).values()) {
				if(!constraint.getConstraintType().equalsIgnoreCase("p")) {
					String dropQuery = "declare cnt number;\r\n" + 
							"begin    \r\n" + 
							"select count(*) into cnt from ALL_CONSTRAINTS where upper(CONSTRAINT_NAME) = upper('" + constraint.getName() + "') and upper(owner) = upper('" + schema + "');\r\n" + 
							"   if (cnt > 0) \r\n" + 
							"    then \r\n" + 
							"        execute immediate('alter table " + schema + "." + table.getTable().getName() + " drop constraint " + constraint.getName() + "');\r\n" + 
							"    end if;\r\n" + 
							"end;";	
					
					//String query = "if (OBJECT_ID(" + schema + "." + constraint.getName() + ")) then begin alter table " + schema + "." + table.getTable().getName() 
					//		+ " drop constraint " + constraint.getName() + "; end";
					ConsoleWriter.println(dropQuery);
					st.execute(dropQuery, "/");
				}					
			}
			ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			ConsoleWriter.println(lang.getValue("errors", "restore", "objectRestoreError").withParams(e.getLocalizedMessage()));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "cannotRestore").withParams(schema + "." + table.getTable().getName()), e);
		} finally {
			st.close();
		}		
		
	}
	
	private String getBlobQuery(Collection<ICellData> datas, String query) throws Exception {
		String res = "declare\n";
		
		for (ICellData data : datas) {
			if (data instanceof TextFileData)
				res += "    b_" + data.hashCode() + " CLOB;\n";
			else if (data instanceof MapFileData)
				res += "    b_" + data.hashCode() + " BLOB;\n";
		}
		
		res += "begin\n";
		int i = 0;
		for (ICellData data : datas) {
			if (data instanceof TextFileData) {
				if (((TextFileData) data).getFile() != null && !((TextFileData) data).getFile().getName().contains("null")) {
					res += "    DBMS_LOB.CREATETEMPORARY(b_" + data.hashCode() + ",TRUE);\n";
					
					FileInputStream fis = new FileInputStream(((MapFileData) data).getFile());	
					
					int byteRead;
					StringBuilder sb = new StringBuilder();
				    
		            while ((byteRead = fis.read()) != -1) {
		            	String hex = Integer.toHexString(byteRead);
		            	if (hex.length() == 1) hex = "0" + hex;
		            	
		            	sb.append(hex);
		            	
		            	if (sb.length() == 2000) {
		            		res += "    dbms_lob.WRITEAPPEND (b_" + data.hashCode() + ", 1000, utl_raw.cast_to_varchar2(hextoraw('" + sb.toString() + "')));\n";
		            		sb.setLength(0);
		            	}
		            }
		            if (sb.length() > 0)
		            	res += "    dbms_lob.WRITEAPPEND (b_" + data.hashCode() + ", " + sb.length()/2 + ", utl_raw.cast_to_varchar2(hextoraw('" + sb.toString() + "')));\n";
		            fis.close();
				}
			} else if (data instanceof MapFileData) {
				if (((MapFileData) data).getFile() != null && !((MapFileData) data).getFile().getName().contains("null")) {
					res += "    DBMS_LOB.CREATETEMPORARY(b_" + data.hashCode() + ",TRUE);\n";
					
					FileInputStream fis = new FileInputStream(((MapFileData) data).getFile());	
					
					int byteRead;
					StringBuilder sb = new StringBuilder();
				    
		            while ((byteRead = fis.read()) != -1) {
		            	String hex = Integer.toHexString(byteRead);
		            	if (hex.length() == 1) hex = "0" + hex;
		            	
		            	sb.append(hex);
		            	
		            	if (sb.length() == 2000) {
		            		res += "    dbms_lob.WRITEAPPEND (b_" + data.hashCode() + ", 1000, hextoraw('" + sb.toString() + "'));\n";
		            		sb.setLength(0);
		            	}
		            }
		            if (sb.length() > 0)
		            	res += "    dbms_lob.WRITEAPPEND (b_" + data.hashCode() + ", " + sb.length()/2 + ", hextoraw('" + sb.toString() + "'));\n";
		            fis.close();
				}

			}
			i++;
		}
		
		
		res += "    " + query + ";\n";
		res += "end;\n";
		
		return res;
	}
	
	public String valuesToString(Collection<ICellData> datas, HashMap<String, String> colTypes, ArrayList<String> fieldsList, boolean isInsert) throws ExceptionDBGit, IOException {
		String values="";
		StringJoiner joiner = new StringJoiner(",");
		int i = 0;
		String val = "null";
		for (ICellData data : datas) {		
			boolean isBoolean = ((colTypes.get(fieldsList.get(i)) != null) && (colTypes.get(fieldsList.get(i)).toLowerCase().contains("boolean")));
			if (data instanceof MapFileData || data instanceof TextFileData) {
				if (((MapFileData) data).getFile() == null || ((MapFileData) data).getFile().getName().contains("null")) {
					val = "null";
				} else {					
		            val = "b_" + data.hashCode();
				}
			} else if (data instanceof DateData) { 
				Date date = ((DateData) data).getDate();
				SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");
				if (date != null) 
					val = "TO_TIMESTAMP('" + format.format(date) + "', 'YYYYMMDDHH24MISS')";
				else
					val = "null";
			} else if (data instanceof BooleanData) {
				if (((BooleanData) data).getValue() != null)								
					val = ((BooleanData) data).getValue() ? "1" : "0";
				else
					val = "null";
			} else if (data instanceof LongData) {
				String dt = data.getSQLData().replace("'", "");
				
				if (isBoolean) {
					if (dt == null || dt.equals(""))
						val = "null";
					else if (!dt.equals("1"))
						val = "0";
					else
						val = "1";
				} else {							
					if (!dt.equals("")) 
						val = dt;
					else
						val = "null";
				}
			} else {
				String dt = ((StringData) data).getValue();
				if (isBoolean) {
					if (dt == null || dt.equals(""))
						val = "null";
					else if (dt.startsWith("t") || dt.startsWith("T") || dt.equals("1") || dt.startsWith("y") || dt.startsWith("Y"))
						val = "1";								
					else
						val = "0";
				} else {							
					if (dt != null)								
						val = "'" + dt.replace("'", "''") + "'";
					else
						val = "null";
				}				
			}			
			if (!isInsert)
				val = (adapter.isReservedWord(fieldsList.get(i)) ? "\"" + fieldsList.get(i) + "\"" : fieldsList.get(i))  + " = " + val;
				
			joiner.add(val);
			i++;
		}
		
		values+=joiner.toString();
		if (isInsert) values = "(" + values + ")";
		return values;		
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {

	}

}
