package ru.fusionsoft.dbgit.data_table;

import java.sql.ResultSet;

import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.utils.Convertor;

public class StringData implements ICellData {
	private String value;
	
	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {
		value = rs.getString(fieldname);
		return true;
	}
	
	@Override
	public String serialize(DBTable tbl) throws Exception {
		return Convertor.EncodeBase64(value);
	}
	
	@Override
	public void deserialize(String data) throws Exception {
		this.value = (data == null) ? null : Convertor.DecodeBase64(data);
	}
	
	@Override
	public String convertToString() {
		return getValue();
	}
	
	public String getValue() {
		return value;
	}

	public void setValue(String value) {
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
		return (value == null) ? "''" : "\'"+value+"\'";
	}
	
}
