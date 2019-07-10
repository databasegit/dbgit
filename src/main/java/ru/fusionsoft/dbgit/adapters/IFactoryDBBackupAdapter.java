package ru.fusionsoft.dbgit.adapters;

public interface IFactoryDBBackupAdapter {
	public IDBBackupAdapter getBackupAdapter(IDBAdapter adapter) throws Exception;
}
