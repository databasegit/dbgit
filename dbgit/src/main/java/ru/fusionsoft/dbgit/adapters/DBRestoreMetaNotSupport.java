package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.meta.IMetaObject;

/**
 * <div class="en">Database recovery adapter. Is a stub for meta descriptions that are not supported in a particular database.</div>
 * <div class="ru">Адаптер восстановления БД. Является заглушкой для мета описаний которые не поддерживаются в конкретной БД. </div>
 * 
 * @author mikle
 *
 */

public class DBRestoreMetaNotSupport implements IDBAdapterRestoreMetaData {

	@Override
	public void  setAdapter(IDBAdapter adapter) {
	}
	
	@Override
	public IDBAdapter getAdapter() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception {
		// TODO Auto-generated method stub
		//throw exception not support
		throw new ExceptionDBGit("Restore object "+obj.getName()+" NotSupport");
		
	}
	
	@Override
	public void removeMetaObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub
		//throw exception not support
	}
	
	@Override
	public String getPhisicalSchema(String schema) {
		// TODO Auto-generated method stub
		//throw exception not support
		return null;
	}
}
