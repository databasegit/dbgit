package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

public abstract class DBRestoreAdapter implements IDBAdapterRestoreMetaData {
	protected IDBAdapter adapter = null;
	
	public void  setAdapter(IDBAdapter adapter) {
		this.adapter = adapter;
	}
}
