package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

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
	public void restoreMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
		//throw exception not support

	}
	
	@Override
	public void removeMetaObject(IMetaObject obj) {
		// TODO Auto-generated method stub
		//throw exception not support
	}
}
