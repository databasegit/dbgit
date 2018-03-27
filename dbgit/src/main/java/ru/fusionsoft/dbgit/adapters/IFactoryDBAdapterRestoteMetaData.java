package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import javax.xml.validation.meta.DBGitMetaType;

public interface IFactoryDBAdapterRestoteMetaData {
	public IDBAdapterRestoreMetaData getAdapterRestore(DBGitMetaType tp, Connection conn);
}
