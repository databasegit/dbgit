package ru.fusionsoft.dbgit.data_table;

import java.sql.Date;
import java.sql.ResultSet;
import java.text.DateFormat;
import java.text.SimpleDateFormat;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.utils.Convertor;

public class DateData implements ICellData {
	//private Date value;
	private long value;
	private boolean isNull = false;
	public static SimpleDateFormat format = new SimpleDateFormat("yyyyMMddHHmmss");

	@Override
	public boolean loadFromDB(ResultSet rs, String fieldname) throws Exception {
		if (rs.getDate(fieldname) == null) {
			isNull = true;
			value = 0;
		} else
			value = rs.getDate(fieldname).getTime();

		return true;
	}

	@Override
	public String serialize(DBTable tbl) throws Exception {
		return isNull ? null : format.format(new Date(value));
	}

	@Override
	public void deserialize(String data) throws Exception {
		//value = (data == null) ? null :new Date(format.parse(data).getTime());
		if (data == null) {
			isNull = true;
			value = 0;
		} else {
			isNull = false;
			value = format.parse(data).getTime();
		}
	}

	public void setValue(Date value) {
		this.value = value.getTime();
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
		return isNull ? null : format.format(new Date(value));
	}

	@Override
	public Object getWriterForRapair() {
		return null;
	}

	@Override
	public String getSQLData() {
		return isNull ? "''" : "\'"+format.format(new Date(value))+"\'";
	}
	
	public Date getDate() {
		return new Date(value);
	}

}
