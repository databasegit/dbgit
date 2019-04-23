package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
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
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.GitMetaDataManager;
import ru.fusionsoft.dbgit.data_table.ICellData;
import ru.fusionsoft.dbgit.data_table.RowData;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBRestoreTableDataOracle extends DBRestoreAdapter {

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {

		if (obj instanceof MetaTableData) {
			ConsoleWriter.println("It's tabledata. step " + step);			

			MetaTableData restoreTableData = (MetaTableData) obj;
			
			IMetaObject currentMetaObj = GitMetaDataManager.getInctance().getCacheDBMetaObject(obj.getName());
			MetaTableData currentTableData = (MetaTableData) currentMetaObj;
			
			if(Integer.valueOf(step).equals(0)) {
				removeTableConstraintsOracle(restoreTableData.getMetaTable());
				return false;
			}
			if(Integer.valueOf(step).equals(1)) {
				restoreTableDataOracle(restoreTableData, currentTableData);
				return false;
			}
			if(Integer.valueOf(step).equals(2)) {
				restoreTableConstraintOracle(restoreTableData.getMetaTable());
			}
			
			return true;
		} else {
			throw new ExceptionDBGitRestore("Error restore: Unable to restore Table Data.");
		}
	}

	private void restoreTableDataOracle(MetaTableData restoreTableData, MetaTableData currentTableData) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			String insertQuery= "";
			
			String fields = "(" + restoreTableData.getmapRows().firstEntry().getValue().getData().keySet().stream().map(d -> d.toString()).collect(Collectors.joining(",")) + ")";
			MapDifference<String, RowData> diffTableData = Maps.difference(restoreTableData.getmapRows(), currentTableData == null ? new TreeMap<String, RowData>() : currentTableData.getmapRows());
			String tblName = getPhisicalSchema(restoreTableData.getTable().getSchema()) + "." + restoreTableData.getTable().getName();

			if(!diffTableData.entriesOnlyOnLeft().isEmpty()) {
				for(RowData rowData:diffTableData.entriesOnlyOnLeft().values()) {
					String values = 
							rowData.getData().values().stream().map(d -> d.getSQLData()).collect(Collectors.joining(","));				
					insertQuery = "insert into "+tblName +
							fields + " values (" + values + ")";
					
					st.execute(insertQuery);

				}
			}
			
			if(!diffTableData.entriesOnlyOnRight().isEmpty()) {

				for(RowData rowData : diffTableData.entriesOnlyOnRight().values()) {
					Map<String,String> primarykeys = getKeys(rowData);
					
					if (primarykeys.size() == 0) {
						ConsoleWriter.printlnRed("PK not found for table " + tblName + ", cannot delete row!");
						continue;
					}
					
					String delParams = primarykeys.entrySet().stream()
						.map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
						.collect(Collectors.joining(" and "));
					
					String deleteQuery = "delete from " + tblName + " where " + delParams;

					st.execute(deleteQuery);					
				}				
			}
			
			if(!diffTableData.entriesDiffering().isEmpty()) {
				for (ValueDifference<RowData> diffRowData:diffTableData.entriesDiffering().values()) {
					if (!diffRowData.leftValue().getHashRow().equals(diffRowData.rightValue().getHashRow())) {
						Map<String,String> primarykeys = getKeys(diffRowData.leftValue());
						
						String updParams = primarykeys.entrySet().stream()
								.map(entry -> entry.getKey() + " = '" + entry.getValue() + "'")
								.collect(Collectors.joining(" and "));

						String updValues = diffRowData.leftValue().getData().entrySet().stream()
								.filter(entry -> !primarykeys.containsKey(entry.getKey()))
								.map(entry -> entry.getKey() + " = " + entry.getValue().getSQLData())
								.collect(Collectors.joining(", "));						
						
						if (updValues.length() > 0) {
							String updateQuery = "update " + tblName + " set " + updValues + " where " + updParams;
							st.executeQuery(updateQuery);
						}
												
					}
				}				
			}			

		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore " + restoreTableData.getTable().getSchema() + "." + restoreTableData.getTable().getName() , e);
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
					ConsoleWriter.printlnRed("Can't get PK for table");
					ConsoleWriter.printlnRed(e.getMessage());
				} 
			}			        	
        });		
		
		return primarykeys;
	}

	
	private void restoreTableConstraintOracle(MetaTable table) throws Exception {
		IDBAdapter adapter = getAdapter();
		Connection connect = adapter.getConnection();
		StatementLogging st = new StatementLogging(connect, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		try {	
			for(DBConstraint constraint : table.getConstraints().values()) {
				if(!constraint.getConstraintType().equalsIgnoreCase("p")) {				
					String query = "alter table " + schema + "." + table.getTable().getName() 
							+ " add constraint "+ constraint.getName() + " " +constraint.getConstraintDef();
					ConsoleWriter.println(query);
					st.execute(query);
				}
			}						
		}
		catch (Exception e) {
			throw new ExceptionDBGitRestore("Error restore " + table.getTable().getName(), e);
		} finally {
			st.close();
		}			
	}

	private void removeTableConstraintsOracle(MetaTable table) throws Exception {
		IDBAdapter adapter = getAdapter();
		StatementLogging st = new StatementLogging(adapter.getConnection(), adapter.getStreamOutputSqlCommand(), adapter.isExecSql());
		String schema = getPhisicalSchema(table.getTable().getSchema());
		try {		
			for (DBConstraint constraint : adapter.getConstraints(schema, table.getTable().getName()).values()) {
				if(!constraint.getConstraintType().equalsIgnoreCase("p")) {
					String query = "alter table " + schema + "." + table.getTable().getName() 
							+ " drop constraint " + constraint.getName();
					ConsoleWriter.println(query);
					st.execute(query);
				}					
			}
		}
		catch(Exception e) {
			throw new ExceptionDBGitRestore("Cannot restore " + schema + "." +table.getTable().getName(), e);
		} finally {
			st.close();
		}		
		
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {

	}

}
