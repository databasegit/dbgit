package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;

import ru.fusionsoft.dbgit.dbobjects.DBTable;


public class LongData implements ICellData {
	private double value;
	private boolean isNull = false;
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {		
		value = rs.getDouble(fieldname);
		if (rs.wasNull()) {
			isNull = true;
	    }
		return true;
	}
	
	@Override
	public String serialize(DBTable tbl) throws Exception {
		return convertToString();
	}
	
	@Override
	public void deserialize(String data) throws Exception {
		if (data == null) {
			isNull = true;
			value = 0;
		} else {
			isNull = false;
			value = Double.parseDouble(data);
		}
		//this.value = (data == null) ? null : Long.decode(data);
		//this.value = Long.decode(data);
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
		return !isNull ?  String.valueOf(value) : null;
	}
	
	public double getValue() {
		return value;
	}

	public void setValue(double value) {
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

	@Override
	public String getSQLData() {		
		return (isNull) ? "''" : "\'"+String.valueOf(value)+"\'";
	}

	public boolean isNull() {
		return isNull;
	}
}
