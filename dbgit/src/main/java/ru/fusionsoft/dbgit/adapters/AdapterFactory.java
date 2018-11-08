package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.SchemaSynonim;
import ru.fusionsoft.dbgit.oracle.DBAdapterOracle;
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
	
	public static IDBAdapter createAdapter() throws ExceptionDBGit {
		if (adapter == null) {
			SchemaSynonim ss = SchemaSynonim.getInctance();
			DBConnection conn = DBConnection.getInctance();
			//TODO
			//if conn params - create adapter
			if (conn.getConnect().getClass().getName() == "oracle.jdbc.driver.T4CConnection") {
				adapter = new DBAdapterOracle();
			} else {
				adapter = new DBAdapterPostgres();
			}
			
			
			adapter.setConnection(conn.getConnect());
			adapter.registryMappingTypes();
			
			if (ss.getCountSynonim() > 0) {
				adapter = new DBAdapterProxy(adapter);
			}
		}
		
		return adapter;
	}
}
