package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTable;

public class BooleanData implements ICellData {

	private boolean value;
	private boolean isNull = false;
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldName) throws Exception {
		value = rs.getBoolean(fieldName);
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
		//this.value = (data == null) ? null : Boolean.valueOf(data);
		if (data == null) {
			isNull = true;
			value = false;
		} else {
			isNull = false;
			value = Boolean.parseBoolean(data);
		}
	}

	@Override
	public int addToGit() throws ExceptionDBGit {
		return 0;
	}

	@Override
	public int removeFromGit() throws ExceptionDBGit {
		return 0;
	}

	@Override
	public String convertToString() throws Exception {
		return !isNull ? String.valueOf(value) : null;
	}

	@Override
	public Object getWriterForRapair() {
		return null;
	}
	
	public Boolean getValue() {
		if (isNull) return null;
		return value;
	}

	@Override
	public String getSQLData() {
		return (isNull) ? "''" : "\'"+String.valueOf(value)+"\'";
	}

}
