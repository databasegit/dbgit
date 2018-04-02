package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.postgres.DBAdapterPostgres;

/**
 * <div class="en">The factory of adapters for the database. 
 * Creates an adapter by reference to the Java driver from the .dblink file.
 * The created adapter is Singleton</div>
 * 
 * <div class="ru">Фабрика вдаптеров для БД. 
 * Создает адаптер по ссылке на джава драйвер из файла .dblink. 
 * Созданный адаптер - Singleton</div>
 * 
 * @author mikle
 *
 */
public class AdapterFactory {
	private static IDBAdapter adapter = null;
	
	public static IDBAdapter createAdapter() {
		if (adapter == null) {
			DBConnection conn = DBConnection.getInctance();
			//TODO
			//if conn params - create adapter
			adapter = new DBAdapterPostgres();
			adapter.setConnection(conn.getConnect());
		}
		
		return adapter;
	}
}
