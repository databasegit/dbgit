package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTable;

public class BooleanData implements ICellData {

	private Boolean value;
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {
		value = rs.getBoolean(fieldname);
		if (rs.wasNull()) {
			value = null;
	    }
		return true;
	}

	@Override
	public String serialize(DBTable tbl) throws Exception {
		return convertToString();
	}

	@Override
	public void deserialize(String data) throws Exception {
		this.value = (data == null) ? null : Boolean.valueOf(data);
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
		return value != null ?  value.toString() : null;
	}

	@Override
	public Object getWriterForRapair() {
		return null;
	}

	@Override
	public String getSQLData() {
		return (value == null) ? "''" : "\'"+value.toString()+"\'";
	}

}
