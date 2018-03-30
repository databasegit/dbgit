package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.meta.IMetaObject;

public interface IDBAdapterRestoreMetaData {
	public void setAdapter(IDBAdapter adapter);
	public void restoreMetaObject(IMetaObject obj);
}
