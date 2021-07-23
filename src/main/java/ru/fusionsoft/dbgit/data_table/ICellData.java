package ru.fusionsoft.dbgit.data_table;

import java.io.IOException;
import java.sql.ResultSet;

import java.sql.SQLException;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBTable;

public interface ICellData {
	
	public boolean loadFromDB(ResultSet rs, String fieldname) throws SQLException, ExceptionDBGit, IOException;
	
	public String serialize(DBTable tbl) throws Exception;
	
	public void deserialize(String data) throws Exception;
	
	
	public int addToGit() throws ExceptionDBGit;
	
	public int removeFromGit() throws ExceptionDBGit;
	
	/**
	 * for calc hash 
	 * @return
	 */
	public String convertToString() throws Exception;
	
	//TODO
	public Object getWriterForRapair();
	public String getSQLData();
}
