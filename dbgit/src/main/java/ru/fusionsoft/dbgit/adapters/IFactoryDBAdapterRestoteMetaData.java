package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.meta.DBGitMetaType;

public interface IFactoryDBAdapterRestoteMetaData {
	public IDBAdapterRestoreMetaData getAdapterRestore(DBGitMetaType tp, IDBAdapter adapter);
}
