package ru.fusionsoft.dbgit.postgres;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.ResultSet;

import org.postgresql.largeobject.LargeObject;
import org.postgresql.largeobject.LargeObjectManager;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.data_table.MapFileData;

public class LargeBlobPg extends MapFileData {

	@Override
	public InputStream getBlobData(ResultSet rs, String fieldname) throws Exception {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		Connection connect =  adapter.getConnection();
		System.out.println(connect.getClass().getName());
		//TODO !!!!!!!!!
		//org.postgresql.jdbc4.Jdbc4Connection
		LargeObjectManager lobj = connect.unwrap(org.postgresql.PGConnection.class).getLargeObjectAPI();
		
		long oid = rs.getLong(fieldname);
		LargeObject obj = lobj.open(oid, LargeObjectManager.READ);
		
		return obj.getInputStream();		
	}
}
