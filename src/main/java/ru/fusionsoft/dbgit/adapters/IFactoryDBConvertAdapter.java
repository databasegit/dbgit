package ru.fusionsoft.dbgit.adapters;

public interface IFactoryDBConvertAdapter {
	public IDBConvertAdapter getConvertAdapter(String objectType) throws Exception;
}
