package ru.fusionsoft.dbgit.mysql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.data_table.DateData;
import ru.fusionsoft.dbgit.data_table.FactoryCellData;
import ru.fusionsoft.dbgit.data_table.LongData;
import ru.fusionsoft.dbgit.data_table.MapFileData;
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
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.dbobjects.DBUser;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

public class DBAdapterMySql extends DBAdapter {

	private Logger logger = LoggerUtil.getLogger(this.getClass());
	
	public static final String DEFAULT_MAPPING_TYPE = "string";
	
	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(DEFAULT_MAPPING_TYPE, StringData.class);		
		FactoryCellData.regMappingTypes("string", StringData.class);
		FactoryCellData.regMappingTypes("number", LongData.class);
		FactoryCellData.regMappingTypes("binary", MapFileData.class);
		FactoryCellData.regMappingTypes("date", DateData.class);
		FactoryCellData.regMappingTypes("native", StringData.class);

	}

	@Override
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		// TODO Auto-generated method stub
		return null;
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
			String query = "select schema_name\r\n" + 
					"from information_schema.schemata";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("schema_name");
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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
		Map<String, DBSequence> sequences = new HashMap<String, DBSequence>();
		return sequences;
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
			String query = "SELECT T.TABLE_NAME, T.TABLE_SCHEMA " + 
					"FROM information_schema.tables T WHERE TABLE_SCHEMA = '" + schema + "'";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while(rs.next()){
				String nameTable = rs.getString("TABLE_NAME");
				DBTable table = new DBTable(nameTable);
				table.setSchema(schema);
				rowToProperties(rs, table.getOptions());
				
				Statement stmtDdl = connect.createStatement();
				ResultSet rsDdl = stmtDdl.executeQuery("show create table " + schema + "." + nameTable);
				rsDdl.next();
				
				table.getOptions().addChild("ddl", rsDdl.getString(2));
				
				listTable.put(nameTable, table);
				
				stmtDdl.close();
				rsDdl.close();
			}
			rs.close();
			stmt.close();			
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
		return listTable;
	}

	@Override
	public DBTable getTable(String schema, String name) {
		try {
			String query = "SELECT T.TABLE_NAME, T.TABLE_SCHEMA " + 
					"FROM information_schema.tables T WHERE TABLE_SCHEMA = '" + schema + "' AND T.TABLE_NAME = '" + name + "'";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			DBTable table = null;
			
			while(rs.next()) {
				String nameTable = rs.getString("TABLE_NAME");
				table = new DBTable(nameTable);
				table.setSchema(schema);
				
				Statement stmtDdl = connect.createStatement();
				ResultSet rsDdl = stmtDdl.executeQuery("show create table " + schema + "." + nameTable);
				rsDdl.next();
				
				table.getOptions().addChild("ddl", rsDdl.getString("Create Table"));
				
				rowToProperties(rs, table.getOptions());
				
				stmtDdl.close();
				rsDdl.close();
			}
			rs.close();
			stmt.close();
			
			return table;
		
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		try {
			Map<String, DBTableField> listField = new HashMap<String, DBTableField>();
			
			String query = 
					"SELECT distinct col.column_name,col.is_nullable,col.data_type,col.character_maximum_length, tc.constraint_name, " +
					"case\r\n" + 
					"	when lower(data_type) in ('tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'float', 'double', 'decimal') then 'number' \r\n" + 
					"	when lower(data_type) in ('tinytext', 'text', 'char', 'mediumtext', 'longtext', 'blob', 'mediumblob', 'longblob', 'varchar') then 'string'\r\n" + 
					"	when lower(data_type) in ('datetime', 'timestamp', 'date') then 'date'\r\n" + 
					"	when lower(data_type) in ('boolean') then 'boolean'\r\n" + 
					"   when lower(data_type) in ('bytea') then 'binary'" +
					"	else 'native'\r\n" + 
					"	end tp, " +
					"    case when lower(data_type) in ('char', 'character') then true else false end fixed, " +
					"col.*  FROM " + 
					"information_schema.columns col  " + 
					"left join information_schema.key_column_usage kc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and col.column_name=kc.column_name " + 
					"left join information_schema.table_constraints tc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and kc.constraint_name = tc.constraint_name and tc.constraint_type = 'PRIMARY KEY' " + 
					"where col.table_schema = :schema and col.table_name = :table " + 
					"order by col.column_name ";
			Connection connect = getConnection();			
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			stmt.setString("table", nameTable);

			ResultSet rs = stmt.executeQuery();
			while(rs.next()){				
				DBTableField field = new DBTableField();
				field.setName(rs.getString("column_name").toLowerCase());  
				if (rs.getString("constraint_name") != null) { 
					field.setIsPrimaryKey(true);
				}
				field.setTypeSQL(getFieldType(rs));
				field.setTypeMapping(getTypeMapping(rs));
				field.setTypeUniversal(rs.getString("tp"));
				field.setFixed(false);
				field.setLength(rs.getString("character_maximum_length"));
				field.setPrecision(rs.getInt("numeric_precision"));
				field.setScale(rs.getInt("numeric_scale"));
				field.setFixed(rs.getBoolean("fixed"));
				listField.put(field.getName(), field);
			}
			stmt.close();
			
			return listField;
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}		
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<String, DBIndex>();
		return indexes;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		Map<String, DBConstraint> constraints = new HashMap<String, DBConstraint>();
		return constraints;
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		Map<String, DBView> views = new HashMap<String, DBView>();
		return views;
	}

	@Override
	public DBView getView(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		Map<String, DBPackage> packages = new HashMap<String, DBPackage>();
		return packages;
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		Map<String, DBProcedure> procedures = new HashMap<String, DBProcedure>();
		return procedures;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		Map<String, DBFunction> functions = new HashMap<String, DBFunction>();
		return functions;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> triggers = new HashMap<String, DBTrigger>();
		return triggers;
	}

	@Override
	public DBTrigger getTrigger(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBUser> getUsers() {
		Map<String, DBUser> users = new HashMap<String, DBUser>();
		return users;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		Map<String, DBRole> roles = new HashMap<String, DBRole>();
		return roles;
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() {
		return true;
	}

	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDbType() {
		return "mysql";
	}

	@Override
	public String getDbVersion() {
		try {
			PreparedStatement stmt = getConnection().prepareStatement("SELECT version()");
			ResultSet resultSet = stmt.executeQuery();			
			resultSet.next();
			
			String result = resultSet.getString(1);
			resultSet.close();
			stmt.close();
			
			return result;
		} catch (SQLException e) {
			return "";
		}
	}

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		// TODO Auto-generated method stub

	}

	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		// TODO Auto-generated method stub

	}

	protected String getTypeMapping(ResultSet rs) throws SQLException {
		//String tp = rs.getString("data_type");
		String tp = rs.getString("tp");
		if (FactoryCellData.contains(tp) ) 
			return tp;
		
		return DEFAULT_MAPPING_TYPE;
	}
	
	protected String getFieldType(ResultSet rs) {
		try {
			StringBuilder type = new StringBuilder(); 
			type.append(rs.getString("data_type"));
			
			BigDecimal max_length = rs.getBigDecimal("character_maximum_length");
			if (!rs.wasNull()) {
				type.append("("+max_length + ")");
			}
			if (rs.getString("is_nullable").equals("NO")){
				type.append(" NOT NULL");
			}
			
			
			return type.toString();
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}	
	}
}
