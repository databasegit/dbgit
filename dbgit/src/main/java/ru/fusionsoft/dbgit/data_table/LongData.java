package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;

import ru.fusionsoft.dbgit.dbobjects.DBTable;


public class LongData implements ICellData {
	private Long value;
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {
		value = rs.getLong(fieldname);
		return true;
	}
	
	@Override
	public String serialize(DBTable tbl) throws Exception {
		return convertToString();
	}
	
	@Override
	public void deserialize(String data) throws Exception {
		this.value = Long.decode(data);
		/*
		try {
			this.value = Long.decode(data);
		} catch (Exception e) {
			e.printStackTrace();
			value = 0L;
		}*/
	}
	
	@Override
	public String convertToString() {		 
		return value != null ?  value.toString() : null;
	}
	
	public Long getValue() {
		return value;
	}

	public void setValue(Long value) {
		this.value = value;
	}

	public Object getWriterForRapair() {
		return null;
	}
	
	public int addToGit() {
		return 0;
	}
	
	public int removeFromGit() {
		return 0;
	}
	
}
