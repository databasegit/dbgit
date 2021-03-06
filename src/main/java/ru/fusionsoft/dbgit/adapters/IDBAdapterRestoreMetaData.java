package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaTable;


/**
 * <div class="en">Adapter for database recovery. For each type of meta description, you can use your recovery adapter.</div>
 * <div class="ru">Адаптер для восстановления БД. Для каждого типа метаописания возможен свой адаптер восстановления. </div>
 * 
 * @author mikle
 *
 *
 */
public interface IDBAdapterRestoreMetaData {
	public void setAdapter(IDBAdapter adapter);
	
	public IDBAdapter getAdapter();
	
	public default boolean restoreMetaObject(IMetaObject obj) throws Exception {
		return restoreMetaObject(obj, 0);
	}
	
	public boolean restoreMetaObject(IMetaObject obj, int step) throws Exception;
	
	public void removeMetaObject(IMetaObject obj) throws Exception;
	
	public String getPhisicalSchema(String schema) throws ExceptionDBGit;
	
}
