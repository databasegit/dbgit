package ru.fusionsoft.dbgit.postgres;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableData;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.dbobjects.DBTableRow;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.dbobjects.DBUser;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;

public class DBAdapterPostgres extends DBAdapter {

	private FactoryDBAdapterRestorePostgres restoreFactory = new FactoryDBAdapterRestorePostgres();
	
	@Override
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		return restoreFactory;
	}
	
	@Override
	public void startUpdateDB() {
		// TODO Auto-generated method stub

	}

	@Override
	public void endUpdateDB() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public IMapMetaObject loadCustomMetaObjects() {
		return null;
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		Map<String, DBSchema> listScheme = new HashMap<String, DBSchema>();
		try {
			String query = "select *from pg_namespace";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBSchema scheme = new DBSchema(name);
				listScheme.put(name, scheme);
				System.out.println(name);
			}
			System.out.println("Collection schemes:");
			for(DBSchema schema:listScheme.values())
				System.out.println(schema.getName());
		}catch(Exception e) {
			e.printStackTrace();
		}
		//connect.cre
		//select *from pg_catalog.pg_namespace;
		return listScheme;
	}
	
	@Override
	public Map<String, DBTableSpace> getTableSpaces() {
		Map<String, DBTableSpace> listTableSpace = new HashMap<String, DBTableSpace>();
		String sql = "Select * from pg_tablespace";
		try {
			String query = "select *from pg_tablespace";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBTableSpace dbTableSpace = new DBTableSpace(name);
				listTableSpace.put(name, dbTableSpace);
			}
			System.out.println("Collection TableSpaces:");
			for(DBTableSpace tableSpace:listTableSpace.values())
				System.out.println(tableSpace.getName());
		}catch(Exception e) {
			e.printStackTrace();
		}
		return listTableSpace;
	}

	@Override
	public Map<String, DBSequence> getSequences(DBSchema schema) {
		Map<String, DBSequence> listSequence = new HashMap<String, DBSequence>();
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBSequence getSequence(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTable> getTables(DBSchema schema) {
		Map<String, DBTable> listTable = new HashMap<String, DBTable>();
		
		return listTable;
	}

	@Override
	public DBTable getTable(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTableField> getTableFields(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBIndex> getIndexes(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBView> getViews(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBView getView(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Map<String, DBTrigger> getTriggers(DBSchema schema) {
		// TODO Auto-generated method stub
				return null;
	}
	
	public DBTrigger getTrigger(DBSchema schema, String name) {
		// TODO Auto-generated method stub
				return null;
	}

	@Override
	public Map<String, DBPackage> getPackages(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBPackage getPackage(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBProcedure getProcedure(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBFunction> getFunctions(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBFunction getFunction(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableData getTableData(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableRow getTableRow(DBTable tbl, Object id) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBUser> getUsers() {
		Map<String, DBUser> listUser = new HashMap<String, DBUser>();
		try {
			String query = "select *from pg_user";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBUser user = new DBUser(name);
				listUser.put(name, user);
			}
			System.out.println("Collection users:");
			for(DBUser user:listUser.values())
				System.out.println(user.getName());
		}catch(Exception e) {
			e.printStackTrace();
		}
		//connect.cre
		//select *from pg_catalog.pg_namespace;
		return listUser;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		Map<String, DBRole> listRole = new HashMap<String, DBRole>();
		try {
			String query = "select *from pg_roles";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBRole role = new DBRole(name);
				listRole.put(name, role);
			}
			System.out.println("Collection roles:");
			for(DBRole role:listRole.values())
				System.out.println(role.getName());
		}catch(Exception e) {
			e.printStackTrace();
		}
		// TODO Auto-generated method stub
		return listRole;
	}
	
	public static void main(String[] args) {
		System.out.println("start");
		try {
		DBAdapterPostgres dbAdapter = (DBAdapterPostgres) AdapterFactory.createAdapter();
		dbAdapter.getRoles();
		}catch(Exception e) {
			e.printStackTrace();
		}
	}
}
