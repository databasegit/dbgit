package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.meta.IMetaObject;

/**
 * <div class="en">Recovery adapter for objects described on sql. Can be used for different databases.</div>
 * <div class="ru">Адаптер восстановления для объектов описанных на sql. Может использоваться для разных БД.</div>
 * 
 * @author mikle
 *
 */
public class DBRestoreMetaSql extends DBRestoreAdapter  {
	
	@Override
	public void restoreMetaObject(IMetaObject obj) {
		// restore 
	}
}
