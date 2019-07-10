package ru.fusionsoft.dbgit.data_table;

import java.sql.Date;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.utils.Convertor;

public class DateData implements ICellData {
	Date value;
	SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {
		value = rs.getDate(fieldname);
		return true;
	}

	@Override
	public String serialize(DBTable tbl) throws Exception {
		return format.format(value);
	}

	@Override
	public void deserialize(String data) throws Exception {
		value = new Date(format.parse(data).getTime());
	}

	public void setValue(Date value) {
		this.value = value;
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
		return (value == null) ? "" : format.format(value);
	}

	@Override
	public Object getWriterForRapair() {
		return null;
	}

	@Override
	public String getSQLData() {
		return (value == null) ? "''" : "\'"+format.format(value)+"\'";
	}
	
	public Date getDate() {
		return value;
	}

}
