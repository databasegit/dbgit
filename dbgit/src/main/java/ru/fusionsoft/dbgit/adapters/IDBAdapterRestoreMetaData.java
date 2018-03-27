package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import javax.xml.validation.meta.IMetaObject;

public interface IDBAdapterRestoreMetaData {
	public void setConnection(Connection conn);
	public void restoreMetaObject(IMetaObject obj);
}
