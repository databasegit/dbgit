package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.meta.IMetaObject;

public class DBRestoreMetaNotSupport implements IDBAdapterRestoreMetaData {

	@Override
	public void  setAdapter(IDBAdapter adapter) {
	}
	
	@Override
	public void restoreMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
		//throw exception not support

	}
}
