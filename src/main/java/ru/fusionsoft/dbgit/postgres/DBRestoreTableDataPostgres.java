package ru.fusionsoft.dbgit.postgres;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.*;
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
import ru.fusionsoft.dbgit.meta.NameMeta;
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

			if (currentMetaObj == null || currentMetaObj instanceof MetaTableData) {
				
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
					//clears restoreTableData->getDataTable() too after ^ "else" clause
					currentTableData.getmapRows().clear();
				
					if (getAdapter().getTable(schema, currentTableData.getTable().getName()) != null) {
						//actually load data from database
						currentTableData.setDataTable(getAdapter().getTableData(schema, currentTableData.getTable().getName()));
						currentTableData.setFields(
							getAdapter().getTableFields(schema, currentTableData.getTable().getName())
								.entrySet().stream()
								.sorted(Comparator.comparing(x->x.getValue().getOrder()))
								.map( x->x.getKey() )
								.collect(Collectors.toList())
						);
					
						ResultSet rs = currentTableData.getDataTable().resultSet();
						
						TreeMapRowData mapRows = new TreeMapRowData(); 
						
						MetaTable metaTable = new MetaTable(currentTableData.getTable());
						//ALSO loads data from database if fields vary
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
                throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
                    obj.getName()
                    ,  "table data cached", obj.getType().getValue()
                ));
			}					
		}
		else
		{
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "metaTypeError").withParams(
				obj.getName()
				,  "table data", obj.getType().getValue()
			));
		}		
	}
	
	public void restoreTableDataPostgres(MetaTableData restoreTableData, MetaTableData currentTableData) throws Exception{
		//if empty restore table data -> delete all currentTableData
		if (currentTableData.getFields().size() == 0 ) {
			final CharSequence msg = DBGitLang.getInstance().getValue("errors", "restore", "currentFieldsListIsEmpty").toString();
			throw new ExceptionDBGit(msg);
		}
		if (restoreTableData.getmapRows() == null) {
			final CharSequence msg = DBGitLang.getInstance().getValue("errors", "restore", "emptyRowsList").toString();
			throw new ExceptionDBGit(msg);
		}

		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();

		MapDifference<String, RowData> diffTableData = Maps.difference(restoreTableData.getmapRows(), currentTableData.getmapRows());
		try (StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql())) {

			String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());
			String tblNameEscaped = adapter.escapeNameIfNeeded(schema) + "." + adapter.escapeNameIfNeeded(restoreTableData.getTable().getName());
			String fields = getFieldsPrefix(restoreTableData);
			Map<String, String> colTypes = getColumnDataTypes(restoreTableData.getMetaTable(), st);
			Set<String> keyNames = restoreTableData.getMetaTable().getFields().values().stream()
					.filter(DBTableField::getIsPrimaryKey)
					.map(DBTableField::getName)
					.collect(Collectors.toSet());

			//DELETE
			if (!diffTableData.entriesOnlyOnRight().isEmpty()) {
				ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "deleting"), messageLevel);
				StringBuilder deleteQuery = new StringBuilder();

				for (RowData rowData : diffTableData.entriesOnlyOnRight().values()) {
					StringJoiner fieldJoiner = new StringJoiner(",");
					StringJoiner valuejoiner = new StringJoiner(",");

					for( Map.Entry<String, ICellData> entry : rowData.getData(currentTableData.getFields()).entrySet()) {
						if (keyNames.contains(entry.getKey())) {
							fieldJoiner.add("\"" + entry.getKey() + "\"");
							final String value = entry.getValue().convertToString();
							if( value.matches("-?\\d+(\\.0)?") ) {
								valuejoiner.add( String.valueOf( (long) Double.parseDouble(value) ) );
							} else {
								valuejoiner.add("'" + value + "'");
							}
						}
					}

					String delFields = "(" + fieldJoiner.toString() + ")";
					String delValues = "(" + valuejoiner.toString() + ")";

					if (delValues.length() > 2){
						final String ddl = MessageFormat.format(
							"DELETE FROM {0} WHERE {1} = {2};",
							tblNameEscaped, delFields, delValues
						);
						deleteQuery.append(ddl).append("\n");
						ConsoleWriter.detailsPrintln(ddl, messageLevel + 1);
					}
					if (deleteQuery.length() > 50000) {
						st.execute(deleteQuery.toString());
						deleteQuery = new StringBuilder();
					}
				}
				if (deleteQuery.length() > 1) {
					st.execute(deleteQuery.toString());
				}
				ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
			}

			//UPDATE
			if (!diffTableData.entriesDiffering().isEmpty()) {
				ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "updating"), messageLevel);
				String updateQuery = "";
				Map<String, String> primarykeys = new HashMap();

				for (ValueDifference<RowData> diffRowData : diffTableData.entriesDiffering().values()) {
					if (!diffRowData.leftValue().getHashRow().equals(diffRowData.rightValue().getHashRow())) {

						Map<String, ICellData> tempCols = diffRowData.leftValue().getData(restoreTableData.getFields());
						for (String key : tempCols.keySet()) {
							if (tempCols.get(key) == null || tempCols.get(key).convertToString() == null) continue;
							if (keyNames.contains(key)) {
								primarykeys.put(key, tempCols.get(key).convertToString());
								tempCols.remove(key);
							}
						}


						if (!tempCols.isEmpty()) {
							StringJoiner keyFieldsJoiner = new StringJoiner(",");
							StringJoiner keyValuesJoiner = new StringJoiner(",");
							for (Map.Entry<String, String> entry : primarykeys.entrySet()) {
								keyFieldsJoiner.add("\"" + entry.getKey() + "\"");
								keyValuesJoiner.add("\'" + entry.getValue() + "\'");
							}
							primarykeys.clear();


							StringJoiner updFieldJoiner = new StringJoiner(",");
							tempCols.forEach( (key, value) -> {
								updFieldJoiner.add("\"" + key + "\"");
							});


							updateQuery = "UPDATE " + tblNameEscaped + " SET (" + updFieldJoiner.toString() + ") = "
							+ valuesToString(tempCols.values(), colTypes, restoreTableData.getFields()) + " "
							+ "WHERE (" + keyFieldsJoiner.toString() + ") = (" + keyValuesJoiner.toString() + ");\n";


							ConsoleWriter.detailsPrintln(updateQuery, messageLevel);
							st.execute(updateQuery);
							updateQuery = "";
						}

					}
				}

				ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
				if (updateQuery.length() > 1) {
					ConsoleWriter.println(updateQuery, messageLevel);
					st.execute(updateQuery);
				}
			}

			//INSERT
			if (!diffTableData.entriesOnlyOnLeft().isEmpty()) {
				ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "inserting"), messageLevel);
				for (RowData rowData : diffTableData.entriesOnlyOnLeft().values()) {

					String insertQuery = MessageFormat.format("INSERT INTO {0}{1}{2};"
						, tblNameEscaped, fields
						, valuesToString(rowData.getData(restoreTableData.getFields()).values(), colTypes, restoreTableData.getFields())
					);

					ConsoleWriter.detailsPrintln(insertQuery, messageLevel+1);
					st.execute(insertQuery);
				}
				ConsoleWriter.detailsPrintln(lang.getValue("general", "ok"), messageLevel);
			}

		} catch (Exception e) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(restoreTableData.getTable().getSchema() + "." + restoreTableData.getTable().getName())
				, e
			);
		}
	}
	
	public String valuesToString(Collection<ICellData> datas, Map<String, String> colTypes, List<String> fieldsList) throws ExceptionDBGit, IOException {
		String values="(";
		StringJoiner joiner = new StringJoiner(",");
		int i = 0;
		for (ICellData data : datas) {		
			boolean isBoolean = ((colTypes.get(fieldsList.get(i)) != null) && (colTypes.get(fieldsList.get(i)).toLowerCase().contains("boolean")));
			
			if (data instanceof TextFileData) {
				if (((TextFileData) data).getFile() == null || ((TextFileData) data).getFile().getName().contains("null")) {
					//TODO generalize this behaviour based on some parameter
					//TODO nulls if nullable, '' if not
					joiner.add("''");
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
//							.replace("\n", "' || chr(10) || '")
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
//			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").withParams(schema + "." + table.getTable().getName())
				, e
			);
		} finally {
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));
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
		String tblName = adapter.escapeNameIfNeeded(schema) + "." + adapter.escapeNameIfNeeded(table.getTable().getName());
		ConsoleWriter.detailsPrintln(lang.getValue("general", "restore", "delConstr").withParams(table.getName()), messageLevel);
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
						st.execute(
							"alter table "+ tblName +" drop constraint if exists "+adapter.escapeNameIfNeeded(rs.getString("conname")));
					}					
				}
			//}	
			ConsoleWriter.detailsPrintGreen(lang.getValue("general", "ok"));

		} catch (Exception e) {
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

	private String getFieldsPrefix(MetaTableData restoreTableData) throws ExceptionDBGit {
		return MessageFormat.format("({0}) values "
			, restoreTableData.getMetaTableFromFile().getFields().entrySet().stream()
				.sorted(Comparator.comparing(e -> e.getValue().getOrder()))
				.map(entry -> adapter.escapeNameIfNeeded(entry.getValue().getName()))
				.collect(Collectors.joining(", "))
		);
	}

	private Map<String, String> getColumnDataTypes(MetaTable tbl, StatementLogging st) throws SQLException {
		HashMap<String, String> colTypes = new HashMap<String, String>();
		NameMeta nm = new NameMeta(tbl);
		nm.setSchema(nm.getSchema());

		ResultSet rsTypes = st.executeQuery(MessageFormat.format(
			" select column_name, data_type from information_schema.columns \r\n" +
			" where lower(table_schema) = lower(''{0}'') AND lower(table_name) = lower(''{1}'')"
			, nm.getSchema(), nm.getName()
		));

		while (rsTypes.next()) {
			colTypes.put(rsTypes.getString("column_name"), rsTypes.getString("data_type"));
		}
		return colTypes;
	}


}
