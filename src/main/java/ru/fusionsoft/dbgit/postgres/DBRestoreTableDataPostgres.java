package ru.fusionsoft.dbgit.postgres;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;

import ru.fusionsoft.dbgit.adapters.DBRestoreAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.data_table.DateData;
import ru.fusionsoft.dbgit.data_table.ICellData;
import ru.fusionsoft.dbgit.data_table.LongData;
import ru.fusionsoft.dbgit.data_table.MapFileData;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.data_table.StringData;
import ru.fusionsoft.dbgit.data_table.TreeMapRowData;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
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
			GitMetaDataManager gitMetaMng = GitMetaDataManager.getInctance();
			//TODO не факт что в кеше есть мета описание нашей таблицы, точнее ее не будет если при старте ресторе таблицы в бд не было совсем
			
			IMetaObject currentMetaObj = gitMetaMng.getCacheDBMetaObject(obj.getName());
			
			if (currentMetaObj instanceof MetaTableData || currentMetaObj == null) {
				
				if (currentMetaObj != null)
					currentTableData = (MetaTableData) currentMetaObj;
				else {
					currentTableData = new MetaTableData();
					currentTableData.setTable(restoreTableData.getTable());
					currentTableData.setMapRows(new TreeMapRowData());
					currentTableData.setDataTable(restoreTableData.getDataTable());
					currentTableData.getmapRows().clear();
				}
				
				if(Integer.valueOf(step).equals(0)) {
					removeTableConstraintsPostgres(restoreTableData.getMetaTable());
					return false;
				}
				if(Integer.valueOf(step).equals(1)) {
					restoreTableDataPostgres(restoreTableData,currentTableData);
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
				//TODO WTF????
				throw new ExceptionDBGitRestore("Error restore: Unable to restore Table Data. "+obj.getClass().getName());
				//return true;
			}					
		}
		else
		{
			throw new ExceptionDBGitRestore("Error restore: Unable to restore Table Data.");
		}		
	}
	
	public void restoreTableDataPostgres(MetaTableData restoreTableData, MetaTableData currentTableData) throws Exception{	
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		try {
			String fields = keysToString(restoreTableData.getmapRows().firstEntry().getValue().getData().keySet()) + " values ";
			MapDifference<String, RowData> diffTableData = Maps.difference(restoreTableData.getmapRows(),currentTableData.getmapRows());
			String schema = getPhisicalSchema(restoreTableData.getTable().getSchema());
			String tblName = schema + "." + restoreTableData.getTable().getName();
			
			ConsoleWriter.detailsPrint("Restoring table data for " + tblName + "\n", 1);
			
			if(!diffTableData.entriesOnlyOnLeft().isEmpty()) {
				
				ConsoleWriter.detailsPrint("Inserting...", 2);
				for(RowData rowData:diffTableData.entriesOnlyOnLeft().values()) {
										
					String insertQuery = "insert into "+tblName +
							fields+valuesToString(rowData.getData().values()) + ";\n";
					
					ConsoleWriter.detailsPrintLn(insertQuery);
					PrepareStatementLogging ps = new PrepareStatementLogging(connect, insertQuery, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
					int i = 0;
					for (ICellData data : rowData.getData().values()) {
						i++;
						ConsoleWriter.detailsPrintLn(data.getSQLData());
						if (data instanceof MapFileData) {
							File file = ((MapFileData) data).getFile();
							FileInputStream fis = new FileInputStream(file);
							ps.setBinaryStream(i, fis, file.length());
						} else if (data instanceof LongData) {
							ps.setDouble(i, Double.parseDouble(data.getSQLData().replace("'", "")));
						} else if (data instanceof DateData) {
							ps.setDate(i, ((DateData) data).getDate());
						} else {
							ps.setString(i, ((StringData) data).getValue());
						}							
					}
					
					if (adapter.isExecSql())
						ps.execute();
					ps.close();
				}
				ConsoleWriter.detailsPrintlnGreen("OK");
			}
			
			if(!diffTableData.entriesOnlyOnRight().isEmpty()){
				ConsoleWriter.detailsPrint("Deleteng...", 2);
				String deleteQuery="";
				Map<String,String> primarykeys = new HashMap();
				for(RowData rowData:diffTableData.entriesOnlyOnRight().values()) {
					Map<String, ICellData> tempcols = rowData.getData();
					String[] keysArray = rowData.getKey().split("_");
					for(String key:keysArray) {
						for (String o : tempcols.keySet()) {
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
					deleteQuery+="delete from " + tblName+
							" where " + delFields + " = " + delValues + ";\n";
					if(deleteQuery.length() > 50000 ){
						st.execute(deleteQuery);
						deleteQuery = "";
					}
				}
				if(deleteQuery.length()>1) {
					st.execute(deleteQuery);
				}
				ConsoleWriter.detailsPrintlnGreen("OK");
			}
			
			if(!diffTableData.entriesDiffering().isEmpty()) {
				ConsoleWriter.detailsPrint("Updating...", 2);
				String updateQuery="";
				Map<String,String> primarykeys = new HashMap();
				for(ValueDifference<RowData> diffRowData:diffTableData.entriesDiffering().values()) {
					if(!diffRowData.leftValue().getHashRow().equals(diffRowData.rightValue().getHashRow())) {
						Map<String, ICellData> tempCols = diffRowData.leftValue().getData();
						String[] keysArray = diffRowData.leftValue().getKey().split("_");
						for(String key:keysArray) {
							for (String o : tempCols.keySet()) {
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
								updvaluejoiner.add("\'"+entry.getValue().convertToString()+"\'");												
							}
							updFields+=updfieldJoiner.toString()+")";
							updValues+=updvaluejoiner.toString()+")";
							updateQuery+="update "+tblName+
									" set "+updFields + " = " + updValues + " where " + keyFields+ "=" +keyValues+";\n";
							if(updateQuery.length() > 50000 ){
								st.execute(updateQuery);
								updateQuery = "";
							}
							
						}
						
					}
				}
				ConsoleWriter.detailsPrintlnGreen("OK");
				if(updateQuery.length()>1) {
					st.execute(updateQuery);
				}
			}
			
			
			
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore " + restoreTableData.getTable().getSchema() + "." + restoreTableData.getTable().getName() , e);
		} finally {
			st.close();
		}
	}
	
	public String valuesToString(Collection<ICellData> datas) {
		String values="(";
		StringJoiner joiner = new StringJoiner(",");
		for (ICellData data : datas) {			
			//joiner.add(data.getSQLData());
			joiner.add("?");
		}
		values+=joiner.toString()+")";
		return values;		
	}
	
	public String keysToString(Set<String> keys) {
		String fields="";
		if(keys.size()>1) {
			String[] fieldsArray = keys.toArray(new String[keys.size()]);	
			fields="("+fieldsArray[0];
			for(int i=1;i<fieldsArray.length;i++) {
				fields+=","+fieldsArray[i];
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
		ConsoleWriter.detailsPrint("Restoring constraints for " + table.getName() + "...", 1);
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		try {	
				for(DBConstraint constrs :table.getConstraints().values()) {
					if(!constrs.getConstraintType().equals("p")) {				
					st.execute("alter table "+schema+"."+ table.getTable().getName() +" add constraint "+ constrs.getName() + " "+constrs.getOptions().get("ddl").toString());
					}
				}						
		}
		catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed("FAIL");
			throw new ExceptionDBGitRestore("Error restore "+table.getTable().getName(), e);
		} finally {
			ConsoleWriter.detailsPrintlnGreen("OK");
			st.close();
		}			
	}
	
	public void removeTableConstraintsPostgres(MetaTable table) throws Exception {		
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		String tblName = schema + "." +table.getTable().getName();
		ConsoleWriter.detailsPrint("Deleting constraints for " + table.getName() + "...", 1);
		try {					
			ResultSet rs = st.executeQuery("SELECT COUNT(*) as constraints_count\n" +
					"FROM pg_catalog.pg_constraint const JOIN pg_catalog.pg_class cl ON (const.conrelid=cl.oid) WHERE cl.relname = '" + tblName + "'");
			rs.next();
			Integer constraintsCount = Integer.valueOf(rs.getString("constraints_count"));
			if(constraintsCount.intValue()>0) {
				Map<String, DBConstraint> constraints = table.getConstraints();
				for(DBConstraint constrs :constraints.values()) {
					if(!constrs.getConstraintType().equals("p")) {
						st.execute("alter table "+schema+"."+ table.getTable().getName() +" drop constraint "+constrs.getName());
					}
				}
			}	
			ConsoleWriter.detailsPrintlnGreen("OK");
		}
		catch(Exception e) {
			ConsoleWriter.detailsPrintlnRed("FAIL");
			throw new ExceptionDBGitRestore("Error restore "+tblName, e);
		}		
	}
	
	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
