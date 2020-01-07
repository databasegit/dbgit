package ru.fusionsoft.dbgit.adapters;

public interface IFactoryDBConvertAdapter {

	public static final String ORACLE = "oracle";
	public static final String POSTGRES = "postgresql";
	public static final String MSSQL = "mssql";


	public IDBConvertAdapter getConvertAdapter(String objectType) throws Exception;
}
