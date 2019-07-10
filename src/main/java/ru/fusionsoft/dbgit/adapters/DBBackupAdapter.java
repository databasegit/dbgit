package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.DBGitConfig;

public abstract class DBBackupAdapter implements IDBBackupAdapter {
	protected IDBAdapter adapter = null;
	
	private boolean toSaveData;
	private boolean saveToSchema;
 	
	public void  setAdapter(IDBAdapter adapter) {
		this.adapter = adapter;
	}
	
	public IDBAdapter getAdapter() {
		return adapter;
	}

	public boolean isToSaveData() {
		return toSaveData;
	}

	public void setToSaveData(boolean toSaveData) {
		this.toSaveData = toSaveData;
	}
	
	public void saveToSchema(boolean saveToSchema) {
		this.saveToSchema = saveToSchema;
	}
	
	public boolean isSaveToSchema() {
		return saveToSchema;
	}
	
}
