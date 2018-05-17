package ru.fusionsoft.dbgit.postgres;

import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class DBRestoreTriggerPostgres implements IDBAdapterRestoreMetaData {

	@Override
	public void setAdapter(IDBAdapter adapter) {
		// TODO Auto-generated method stub

	}

	@Override
	public IDBAdapter getAdapter() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

}
