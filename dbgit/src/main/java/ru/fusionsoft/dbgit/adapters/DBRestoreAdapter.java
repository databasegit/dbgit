package ru.fusionsoft.dbgit.adapters;

import java.sql.Connection;

/**
 * <div class="en">Base class of adapters of restoration of a DB. Contains general solutions independent of a particular database</div>
 * <div class="ru">Базовый класс адаптеров восстановления БД. Содержит общие решения, независимые от конкретной БД</div>
 * 
 * @author mikle
 *
 */
public abstract class DBRestoreAdapter implements IDBAdapterRestoreMetaData {
	protected IDBAdapter adapter = null;
	
	public void  setAdapter(IDBAdapter adapter) {
		this.adapter = adapter;
	}
}
