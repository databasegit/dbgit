package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.csv.CSVRecord;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.utils.CalcHash;

public class RowData {
	protected Map<String, String> data = new HashMap<>();
	protected String hashRow;
	protected String key;
	
	public RowData(ResultSet rs, List<String> idColumns) throws Exception {
		loadDataFromRS(rs, idColumns);
	}
	
	public RowData(CSVRecord record, List<String> idColumns, CSVRecord titleColumns) throws Exception {
		loadDataFromStrLine(record, idColumns, titleColumns);
	}
	
	public void loadDataFromRS(ResultSet rs, List<String> idColumns) throws Exception {
		
		CalcHash ch = new CalcHash();
		
		for (int i = 0; i < rs.getMetaData().getColumnCount(); i++) {
			//TODO Diff format
			String value = rs.getString(i+1);
			data.put(rs.getMetaData().getColumnName(i+1), value);
			if (value != null)
				ch.addData(value);
		}
		
		hashRow = ch.calcHashStr();
		
		key = calcRowHash(idColumns);
	}
	
	public void loadDataFromStrLine(CSVRecord record, List<String> idColumns, CSVRecord titleColumns) throws Exception {

		if (record.size() != titleColumns.size()) {
			throw new ExceptionDBGit("Different count columns title and line");
		}
		
		CalcHash ch = new CalcHash();
		
		for (int i = 0; i < record.size(); i++) {
			data.put(titleColumns.get(i), record.get(i));
			if (record.get(i) != null)
				ch.addData(record.get(i));
		}
		hashRow = ch.calcHashStr();
		
		key = calcRowHash(idColumns);
	}
	
	public String calcRowHash(List<String> idColumns) {
		if (idColumns.size() > 0) {
			StringBuilder keyBuilder = new StringBuilder();
			for (String nmId : idColumns) {
				keyBuilder.append(data.get(nmId)+"_");
			}
			return keyBuilder.toString();
		} else {
			return hashRow;
		}
	}

	public Map<String, String> getData() {
		return data;
	}

	public String getHashRow() {
		return hashRow;
	}

	public String getKey() {
		return key;
	}
	
	
}
