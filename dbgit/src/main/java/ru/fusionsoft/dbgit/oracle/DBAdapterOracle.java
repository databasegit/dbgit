package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.data_table.FactoryCellData;
import ru.fusionsoft.dbgit.data_table.LongData;
import ru.fusionsoft.dbgit.data_table.StringData;
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
import ru.fusionsoft.dbgit.postgres.FactoryDBAdapterRestorePostgres;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

public class DBAdapterOracle extends DBAdapter {
	
	private Logger logger = LoggerUtil.getLogger(this.getClass());

	@Override
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		// TODO Auto-generated method stub
		return null;
	}
	
	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes("string", StringData.class);
		FactoryCellData.regMappingTypes("integer", LongData.class);
		//FactoryCellData.regMappingTypes("blob", BlobData.class);
	}

	@Override
	public void startUpdateDB() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public void restoreDataBase(IMapMetaObject updateObjs) {
		
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
			String query = "SELECT DISTINCT OWNER\n" + 
					"FROM DBA_OBJECTS WHERE OWNER != 'PUBLIC' AND OWNER != 'SYSTEM'\n" + 
					"AND OWNER != 'SYS' AND OWNER != 'APPQOSSYS' AND OWNER != 'OUTLN' \n" + 
					"AND OWNER != 'DIP' AND OWNER != 'DBSNMP' AND OWNER != 'ORACLE_OCM'\n" + 
					"ORDER BY OWNER";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("OWNER");
				DBSchema scheme = new DBSchema(name);
				rowToProperties(rs, scheme.getOptions());
				listScheme.put(name, scheme);	
			}	
			stmt.close();
		}catch(Exception e) {
			logger.error("Error load schemes!", e);
			throw new ExceptionDBGitRunTime("Error load schemes!", e);
		} 

		return listScheme;
	}
	
	@Override
	public Map<String, DBTableSpace> getTableSpaces() {
		Map<String, DBTableSpace> listTableSpace = new HashMap<String, DBTableSpace>();
		try {
			String query = "SELECT owner,\n" + 
					"       segment_name,\n" + 
					"       partition_name,\n" + 
					"       segment_type,\n" + 
					"       bytes \n" + 
					"  FROM dba_segments \n" + 
					" WHERE OWNER != 'PUBLIC' AND OWNER != 'SYSTEM'\n" + 
					"AND OWNER != 'SYS' AND OWNER != 'APPQOSSYS' AND OWNER != 'OUTLN' \n" + 
					"AND OWNER != 'DIP' AND OWNER != 'DBSNMP' AND OWNER != 'ORACLE_OCM'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("segment_name");
				DBTableSpace dbTableSpace = new DBTableSpace(name);
				rowToProperties(rs, dbTableSpace.getOptions());
				listTableSpace.put(name, dbTableSpace);
			}	
			stmt.close();
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listTableSpace;
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTable> getTables(String schema) {
		Map<String, DBTable> listTable = new HashMap<String, DBTable>();
		try {
			String query = "SELECT * FROM DBA_TABLES WHERE OWNER=:schema";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameTable = rs.getString("TABLE_NAME");
				DBTable table = new DBTable(nameTable);
				table.setSchema(schema);
				rowToProperties(rs, table.getOptions());
				listTable.put(nameTable, table);
			}
			stmt.close();
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
		return listTable;
	}

	@Override
	public DBTable getTable(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBView getView(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}
	
	public Map<String, DBTrigger> getTriggers(String schema) {
		// TODO Auto-generated method stub
				return null;
	}
	
	public DBTrigger getTrigger(String schema, String name) {
		// TODO Auto-generated method stub
				return null;
	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableData getTableData(String schema, String nameTable, int paramFetch) {
		// TODO Auto-generated method stub
		return null;
	}
/*
	@Override
	public DBTableRow getTableRow(String schema, String nameTable, Object id) {
		// TODO Auto-generated method stub
		return null;
	}
*/
	@Override
	public Map<String, DBUser> getUsers() {
		Map<String, DBUser> listUser = new HashMap<String, DBUser>();
		try {
			String query = "SELECT USERNAME FROM DBA_USERS WHERE USERNAME != 'PUBLIC' AND USERNAME != 'SYSTEM'\n" + 
					"AND USERNAME != 'SYS' AND USERNAME != 'APPQOSSYS' AND USERNAME != 'OUTLN' \n" + 
					"AND USERNAME != 'DIP' AND USERNAME != 'DBSNMP' AND USERNAME != 'ORACLE_OCM' ORDER BY USERNAME";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBUser user = new DBUser(name);
				listUser.put(name, user);
			}
			stmt.close();
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listUser;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		Map<String, DBRole> listRole = new HashMap<String, DBRole>();
		try {
			String query = "SELECT * FROM DBA_ROLE_PRIVS R WHERE GRANTEE = (SELECT USERNAME FROM DBA_USERS WHERE USERNAME = R.GRANTEE AND\n" + 
					"USERNAME != 'PUBLIC' AND USERNAME != 'SYSTEM'\n" + 
					"AND USERNAME != 'SYS' AND USERNAME != 'APPQOSSYS' AND USERNAME != 'OUTLN' \n" + 
					"AND USERNAME != 'DIP' AND USERNAME != 'DBSNMP' AND USERNAME != 'ORACLE_OCM') ORDER BY GRANTEE";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			String prevName = "";
			DBRole r = new DBRole();
			while(rs.next()){
					String name = rs.getString("GRANTEE");
					if(prevName.isEmpty()) {
						DBRole role = new DBRole(name);
						//DBRole role = new DBRole(name);
						rowToProperties(rs, role.getOptions());
						r = role;
						//listRole.put(name, role);
					}
					else if (name.equals(prevName)) {
						rowToProperties(rs, r.getOptions());
					} else if(rs.last() || !name.equals(prevName)) {
						listRole.put(prevName, r);
						r = null;
						DBRole role = new DBRole(name);
						rowToProperties(rs, role.getOptions());
						r = role;
					}
					prevName = name;
			}
			stmt.close();
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listRole;
	}

}
