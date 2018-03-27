package ru.fusionsoft.dbgit.adapters;

import ru.fusionsoft.dbgit.core.DBConnection;
import ru.fusionsoft.dbgit.postgres.DBAdapterPostgres;

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
