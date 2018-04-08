package ru.fusionsoft.dbgit.postgres;


import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
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
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;

public class DBAdapterPostgres extends DBAdapter {

	private Logger logger = LoggerUtil.getLogger(this.getClass());
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
			String query = "select *from pg_namespace where nspname!='pg_toast' and nspname!='pg_temp_1'"+
					"and nspname!='pg_toast_temp_1' and nspname!='pg_catalog'"+
					"and nspname!='information_schema' and nspname!='pgagent'"+
					"and nspname!='pg_temp_3' and nspname!='pg_toast_temp_3'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("nspname");
				DBSchema scheme = new DBSchema(name);
				listScheme.put(name, scheme);
				System.out.println(name);
			}
			System.out.println("Collection schemes:");
			for(DBSchema schema:listScheme.values())
				System.out.println(schema.getName());
		}catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		//connect.cre
		//select *from pg_catalog.pg_namespace;
		return listScheme;
	}
	
	@Override
	public Map<String, DBTableSpace> getTableSpaces() {
		Map<String, DBTableSpace> listTableSpace = new HashMap<String, DBTableSpace>();
		try {
			String query = "Select * from pg_tablespace where spcname!='pg_default' and spcname!='pg_global'";
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
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listTableSpace;
	}

	@Override
	public Map<String, DBSequence> getSequences(DBSchema schema) {
		Map<String, DBSequence> listSequence = new HashMap<String, DBSequence>();
		try {
			String query = "select * from " + schema.getName()+".sequences";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(3);
				DBSequence sequence = new DBSequence(name);
				listSequence.put(name, sequence);
			}
			System.out.println("Collection TableSpaces:");
			for(DBSequence sequence:listSequence.values())
				System.out.println(sequence.getName());
		}catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listSequence;
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
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		//connect.cre
		//select *from pg_catalog.pg_namespace;
		return listUser;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		Map<String, DBRole> listRole = new HashMap<String, DBRole>();
		try {
			String query = "select *from pg_roles where rolname not like 'pg_%'";
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
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		// TODO Auto-generated method stub
		return listRole;
	}
	
	public static void main(String[] args) {
		System.out.println("start");
		try {
			DBAdapterPostgres dbAdapter = (DBAdapterPostgres) AdapterFactory.createAdapter();
			dbAdapter.getSequences(new DBSchema("information_schema"));
		}catch(ExceptionDBGitRunTime e) {
			e.printStackTrace();
		}catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
