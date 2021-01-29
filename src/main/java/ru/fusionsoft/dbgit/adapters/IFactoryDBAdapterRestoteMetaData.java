package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IDBGitMetaType;

public interface IFactoryDBAdapterRestoteMetaData {
	static int messageLevel = 1;
	public IDBAdapterRestoreMetaData getAdapterRestore(IDBGitMetaType tp, IDBAdapter adapter);
}
