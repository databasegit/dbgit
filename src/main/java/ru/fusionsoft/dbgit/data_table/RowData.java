package ru.fusionsoft.dbgit.data_table;

import java.io.IOException;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;

import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.utils.CalcHash;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class RowData {
	protected Map<String, ICellData> data = new LinkedHashMap<>();
	protected String hashRow;
	protected String key;
	protected MetaTable metaTable;
	
	public RowData(ResultSet rs, MetaTable metaTable) throws Exception {
		this.metaTable = metaTable;
		loadDataFromRS(rs);
	}
	
	public RowData(CSVRecord record, MetaTable metaTable, CSVRecord titleColumns) throws Exception {
		this.metaTable = metaTable;
		loadDataFromCSVRecord(record, titleColumns);
	}
	
	public void loadDataFromRS(ResultSet rs) throws Exception {
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {	
			String columnName = rs.getMetaData().getColumnName(i+1).toLowerCase();

			if (columnName.equalsIgnoreCase("DBGIT_ROW_NUM"))
				continue;
			
			ICellData cd = FactoryCellData.createCellData(metaTable.getFieldsMap().get(columnName).getTypeUniversal());
			
			if (cd.loadFromDB(rs, rs.getMetaData().getColumnName(i+1)))
				data.put(columnName, cd);
		}

		hashRow = calcRowHash();
		
		key = calcRowKey(metaTable.getIdColumns());
	}
	
	public void loadDataFromCSVRecord(CSVRecord record, CSVRecord titleColumns) throws Exception {

		if (record.size() != titleColumns.size()) {
			throw new ExceptionDBGit(DBGitLang.getInstance().getValue("errors", "dataTable", "differentCount"));
		}		
		
		for (int i = 0; i < record.size(); i++) {	
			String columnName = titleColumns.get(i);
			if (metaTable.getFieldsMap().get(columnName) == null) {
				throw new ExceptionDBGitRunTime(DBGitLang.getInstance().getValue("errors", "dataTable", "fieldNotFound").withParams(columnName)); 
			}
			
			ICellData cd = FactoryCellData.createCellData(metaTable.getFieldsMap().get(columnName).getTypeUniversal());
			cd.deserialize(record.get(i));
			
			data.put(columnName, cd);
		}
		hashRow = calcRowHash();
		
		key = calcRowKey(metaTable.getIdColumns());
	}
	
	public void saveDataToCsv(CSVPrinter csvPrinter, DBTable tbl) throws Exception {
		for (ICellData cd : getData().values())
			csvPrinter.print(cd.serialize(tbl));
		
		csvPrinter.println();
	}
	
	public String calcRowKey(List<String> idColumns) throws Exception {
		if (idColumns.size() > 0) {
			StringBuilder keyBuilder = new StringBuilder();
			for (String nmId : idColumns) {
				keyBuilder.append(data.get(nmId).convertToString() + "_");
			}
			return keyBuilder.toString();
		} else {
			return hashRow;
		}
	}
	
	public String calcRowHash() throws Exception {
		CalcHash ch = new CalcHash();
		for (ICellData cd : data.values()) {
			String str = cd.convertToString();
			if ( str != null)
				ch.addData(str);
		}
		return ch.calcHashStr();
	}

	public Map<String, ICellData> getData() {
		return data;
	}

	public String getHashRow() {
		return hashRow;
	}

	public String getKey() {
		return key;
	}

	public MetaTable getMetaTable() {
		return metaTable;
	}
	
	
}
