package ru.fusionsoft.dbgit.mysql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.core.db.FieldType;
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
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBAdapterMySql extends DBAdapter {
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	
	private FactoryDBRestoreAdapterMySql restoreFactory = new FactoryDBRestoreAdapterMySql();
	private FactoryDBConvertAdapterMySql convertFactory = new FactoryDBConvertAdapterMySql();
	private FactoryDBBackupAdapterMySql backupFactory = new FactoryDBBackupAdapterMySql();

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
			logger.error(lang.getValue("errors", "adapter", "schemes").toString(), e);
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "schemes").toString(), e);
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
					"FROM information_schema.tables T WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_TYPE = 'BASE TABLE'";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while(rs.next()){
				String nameTable = rs.getString("TABLE_NAME");
				DBTable table = new DBTable(nameTable);
				table.setSchema(schema);
				rowToProperties(rs, table.getOptions());
				
				Statement stmtDdl = connect.createStatement();
				ResultSet rsDdl = stmtDdl.executeQuery("show create table " + schema + ".`" + nameTable + "`");
				rsDdl.next();
				
				table.getOptions().addChild("ddl", cleanString(rsDdl.getString(2)));
				
				listTable.put(nameTable, table);
				
				stmtDdl.close();
				rsDdl.close();
			}
			rs.close();
			stmt.close();			
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
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
				ResultSet rsDdl = stmtDdl.executeQuery("show create table " + schema + ".`" + nameTable + "`");
				rsDdl.next();
				table.getOptions().addChild("ddl", cleanString(rsDdl.getString(2)));
				rowToProperties(rs, table.getOptions());
				stmtDdl.close();
				rsDdl.close();
			}
			rs.close();
			stmt.close();
			
			return table;
		
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
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
					"	when lower(data_type) in ('tinytext', 'text', 'char', 'mediumtext', 'longtext', 'varchar') then 'string'\r\n" + 
					"	when lower(data_type) in ('datetime', 'timestamp', 'date') then 'date'\r\n" + 
					"	when lower(data_type) in ('boolean') then 'boolean'\r\n" + 
					"   when lower(data_type) in ('blob', 'mediumblob', 'longblob', 'binary', 'varbinary') then 'binary'" +
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
                String typeSQL = getFieldType(rs);
				field.setTypeSQL(typeSQL);
                field.setIsNullable( !typeSQL.toLowerCase().contains("not null"));
				field.setTypeUniversal(FieldType.fromString(rs.getString("tp")));
				field.setFixed(false);
				field.setLength(rs.getInt("character_maximum_length"));
				field.setPrecision(rs.getInt("numeric_precision"));
				field.setScale(rs.getInt("numeric_scale"));
				field.setFixed(rs.getBoolean("fixed"));
				listField.put(field.getName(), field);
			}
			stmt.close();
			
			return listField;
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}		
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		try {
			Map<String, DBIndex> indexes = new HashMap<>();
			String query = "select TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE, GROUP_CONCAT(COLUMN_NAME separator '`, `') as FIELDS "
					+ "from INFORMATION_SCHEMA.STATISTICS where TABLE_SCHEMA = '" + schema + "' and INDEX_NAME != 'PRIMARY' "
					+ "group by TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE order by TABLE_NAME, INDEX_NAME;";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()) {
				DBIndex index = new DBIndex(rs.getString("INDEX_NAME"));
				index.setSchema(schema);
				index.setOwner(schema);
				rowToProperties(rs, index.getOptions());

				String ddl = "create " + (rs.getInt("NON_UNIQUE") == 1 ? "" : "unique ")
						+ "index `" + rs.getString("INDEX_NAME")
						+ "` using " + rs.getString("INDEX_TYPE")
						+ " on " + schema + ".`" + rs.getString("TABLE_NAME") + "`"
						+ "(`" + rs.getString("FIELDS") + "`)";

				index.getOptions().addChild("ddl", cleanString(ddl));
				indexes.put(rs.getString("INDEX_NAME"), index);
			}
			stmt.close();
			return indexes;
		} catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		Map<String, DBConstraint> constraints = new HashMap<>();
		return constraints;
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		Map<String, DBView> listView = new HashMap<String, DBView>();
		try {
			String query = "show full tables in " + schema + " where TABLE_TYPE like 'VIEW'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBView view = new DBView(rs.getString(1));
				view.setSchema(schema);
				view.setOwner(schema);
				rowToProperties(rs, view.getOptions());
				
				Statement stmtDdl = connect.createStatement();
				ResultSet rsDdl = stmtDdl.executeQuery("show create view " + schema + "." + rs.getString(1));
				rsDdl.next();
				
				view.getOptions().addChild("ddl", cleanString(rsDdl.getString(2)));
				listView.put(rs.getString(1), view);
				
				stmtDdl.close();
				rsDdl.close();
			}
			stmt.close();
			return listView;
		} catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}

	@Override
	public DBView getView(String schema, String name) {
		DBView view = new DBView(name);
		view.setSchema(schema);
		view.setOwner(schema);
		try {
			Statement stmtDdl = connect.createStatement();
			ResultSet rsDdl = stmtDdl.executeQuery("show create view " + schema + ".`" + name + "`");
			if(rsDdl.next())
				view.getOptions().addChild("ddl", cleanString(rsDdl.getString(2)));
			stmtDdl.close();
			rsDdl.close();
			return view;
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
			
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
		Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		try {
			String query = "SELECT R.routine_schema as \"schema\", R.definer as \"rolname\", R.specific_name as \"name\"," +
					"group_concat(concat(P.parameter_name, \" \", P.data_type)) as \"arguments\", R.routine_definition as \"ddl\"\r\n" +
					"FROM information_schema.routines as R, information_schema.parameters as P\r\n" + 
					"WHERE P.parameter_mode='IN' and P.routine_type=R.routine_type and P.specific_schema=R.routine_schema and\r\n" +
					"P.specific_name=R.specific_name and R.routine_type='FUNCTION' and R.routine_schema='" + schema + "' GROUP BY R.specific_name,1,2,5,P.ordinal_position ORDER BY P.ordinal_position";
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()) {
				String name = rs.getString("name");
				String owner = rs.getString("rolname");
				String args = rs.getString("arguments");
				DBFunction func = new DBFunction(name);
				func.setSchema(schema);
				func.setOwner(owner);
				rowToProperties(rs, func.getOptions());
				//func.setArguments(args);
                listFunction.put(name, func);
			}
			stmt.close();
		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
		}
		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
        DBFunction function = new DBFunction(name);
        try {
            String query = "SELECT R.routine_schema as \"schema\", R.definer as \"rolname\", R.specific_name as \"name\"," +
                    "group_concat(concat(P.parameter_name, \" \", P.data_type)) as \"arguments\", R.routine_definition as \"ddl\"\r\n" +
                    "FROM information_schema.routines as R, information_schema.parameters as P\r\n" +
                    "WHERE P.parameter_mode='IN' and P.routine_type=R.routine_type and P.specific_schema=R.routine_schema and\r\n" +
                    "P.specific_name=R.specific_name and R.routine_type='FUNCTION' and R.routine_schema='" + schema + "' and R.specific_name='" + name + "'\r\n" +
                    "GROUP BY R.specific_name,1,2,5,P.ordinal_position ORDER BY P.ordinal_position";
            Statement stmt = getConnection().createStatement();
            ResultSet rs = stmt.executeQuery(query);
            if(rs.next()) {
				String owner = rs.getString("rolname");
				String args = rs.getString("arguments");
				function.setSchema(schema);
				function.setOwner(owner);
				rowToProperties(rs, function.getOptions());
				//function.setArguments(args);
			}
            stmt.close();
        } catch(Exception e) {
            throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
        }
        return function;
	}

	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		try {
			String query = "show triggers in " + schema;
			
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSchema(schema);
				trigger.setOwner(schema);				
				
				Statement stmtDdl = connect.createStatement();
				ResultSet rsDdl = stmtDdl.executeQuery("show create trigger " + schema + "." + rs.getString(1));
				rsDdl.next();
				
				trigger.getOptions().addChild("ddl", cleanString(rsDdl.getString(3)));
				
				stmtDdl.close();
				rsDdl.close();				
				
				rowToProperties(rs, trigger.getOptions());
				listTrigger.put(name, trigger);
			}
			stmt.close();
			return listTrigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);	
		}
	}

	@Override
	public DBTrigger getTrigger(String schema, String name) {
		DBTrigger trigger = new DBTrigger(name);
		try {
			trigger.setSchema(schema);
			trigger.setOwner(schema);
			
			Statement stmtDdl = connect.createStatement();
			ResultSet rsDdl = stmtDdl.executeQuery("show create trigger " + schema + "." + name);
			rsDdl.next();
			
			trigger.getOptions().addChild("ddl", cleanString(rsDdl.getString(3)));
			
			stmtDdl.close();
			rsDdl.close();
			
			return trigger;
			
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);	
		}
	}

	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		String tableName = schema + "." + nameTable;
		try {
			DBTableData data = new DBTableData();
			
			int maxRowsCount = DBGitConfig.getInstance().getInteger("core", "MAX_ROW_COUNT_FETCH", DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH));
			
			if (DBGitConfig.getInstance().getBoolean("core", "LIMIT_FETCH", DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true))) {
				Statement st = getConnection().createStatement();
				String query = "select COALESCE(count(*), 0) kolvo from ( select 1 from "+
						tableName + " limit " + (maxRowsCount + 1) + " ) tbl";
				ResultSet rs = st.executeQuery(query);
				rs.next();
				if (rs.getInt("kolvo") > maxRowsCount) {
					data.setErrorFlag(DBTableData.ERROR_LIMIT_ROWS);
					return data;
				}
				
				rs = st.executeQuery("select * from "+tableName);
				data.setResultSet(rs);
				return data;
			}
			
			return data;
		} catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tableData").toString(), e);
			try {
				getConnection().rollback(); 
			} catch (Exception e2) {
				logger.error(lang.getValue("errors", "adapter", "rollback").toString(), e2);
			}
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}

	@Override
	public Map<String, DBUser> getUsers() {
		Map<String, DBUser> users = new HashMap<String, DBUser>();
		try {
			String query = "select User, authentication_string from mysql.user";
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()) {
				String name = rs.getString(1);
				String password = rs.getString(2);
				StringProperties options = new StringProperties();
				options.addChild("password", password);
				DBUser user = new DBUser(name, options);
				users.put(name, user);
			}
			stmt.close();
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
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
		return backupFactory;
	}

	@Override
	public DbType getDbType() {
		return DbType.MYSQL;
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
		try {
			Statement st = connect.createStatement();
			ResultSet rs = st.executeQuery("select count(*) cnt from information_schema.schemata where upper(schema_name) = '" +
					schemaName.toUpperCase() + "'");

			rs.next();
			if (rs.getInt("cnt") == 0) {
				StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());
				stLog.execute("create schema " + schemaName);

				stLog.close();
			}

			rs.close();
			st.close();
		} catch (SQLException e) {
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "createSchema") + ": " + e.getLocalizedMessage());
		}
	}

	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		// TODO Auto-generated method stub

	}

	protected String getFieldType(ResultSet rs) {
		try {
			StringBuilder type = new StringBuilder(); 
			type.append(rs.getString("data_type"));
			
			BigDecimal max_length = rs.getBigDecimal("character_maximum_length");
			if (!rs.wasNull()) {
				type.append("(" + max_length + ")");
			}
			if (rs.getString("is_nullable").equals("NO")){
				type.append(" NOT NULL");
			}
			
			return type.toString();
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}	
	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		try {
			return getConnection().getCatalog();
		} catch (SQLException e) {
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "getSchema") + ": " + e.getLocalizedMessage());
		}
	}

	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return convertFactory;
	}

	@Override
	public boolean isReservedWord(String word) {
		Set<String> reservedWords = new HashSet<>();
		reservedWords.add("ACCESSIBLE");
		reservedWords.add("ACCOUNT");
		reservedWords.add("ACTION");
		reservedWords.add("ACTIVE");
		reservedWords.add("ADD");
		reservedWords.add("ADMIN");
		reservedWords.add("AFTER");
		reservedWords.add("AGAINST");
		reservedWords.add("AGGREGATE");
		reservedWords.add("ALGORITHM");
		reservedWords.add("ALL");
		reservedWords.add("ALTER");
		reservedWords.add("ALWAYS");
		reservedWords.add("ANALYSE");
		reservedWords.add("ANALYZE");
		reservedWords.add("AND");
		reservedWords.add("ANY");
		reservedWords.add("ARRAY");
		reservedWords.add("AS");
		reservedWords.add("ASC");
		reservedWords.add("ASCII");
		reservedWords.add("ASENSITIVE");
		reservedWords.add("AT");
		reservedWords.add("AUTOEXTEND_SIZE");
		reservedWords.add("AUTO_INCREMENT");
		reservedWords.add("AVG");
		reservedWords.add("AVG_ROW_LENGTH");
		reservedWords.add("BACKUP");
		reservedWords.add("BEFORE");
		reservedWords.add("BEGIN");
		reservedWords.add("BETWEEN");
		reservedWords.add("BIGINT");
		reservedWords.add("BINARY");
		reservedWords.add("BINLOG");
		reservedWords.add("BIT");
		reservedWords.add("BLOB");
		reservedWords.add("BLOCK");
		reservedWords.add("BOOL");
		reservedWords.add("BOOLEAN");
		reservedWords.add("BOTH");
		reservedWords.add("BTREE");
		reservedWords.add("BUCKETS");
		reservedWords.add("BY");
		reservedWords.add("BYTE");
		reservedWords.add("CACHE");
		reservedWords.add("CALL");
		reservedWords.add("CASCADE");
		reservedWords.add("CASCADED");
		reservedWords.add("CASE");
		reservedWords.add("CATALOG_NAME");
		reservedWords.add("CHAIN");
		reservedWords.add("CHANGE");
		reservedWords.add("CHANGED");
		reservedWords.add("CHANNEL");
		reservedWords.add("CHAR");
		reservedWords.add("CHARACTER");
		reservedWords.add("CHARSET");
		reservedWords.add("CHECK");
		reservedWords.add("CHECKSUM");
		reservedWords.add("CIPHER");
		reservedWords.add("CLASS_ORIGIN");
		reservedWords.add("CLIENT");
		reservedWords.add("CLONE");
		reservedWords.add("CLOSE");
		reservedWords.add("COALESCE");
		reservedWords.add("CODE");
		reservedWords.add("COLLATE");
		reservedWords.add("COLLATION");
		reservedWords.add("COLUMN");
		reservedWords.add("COLUMNS");
		reservedWords.add("COLUMN_FORMAT");
		reservedWords.add("COLUMN_NAME");
		reservedWords.add("COMMENT");
		reservedWords.add("COMMIT");
		reservedWords.add("COMMITTED");
		reservedWords.add("COMPACT");
		reservedWords.add("COMPLETION");
		reservedWords.add("COMPONENT");
		reservedWords.add("COMPRESSED");
		reservedWords.add("COMPRESSION");
		reservedWords.add("CONCURRENT");
		reservedWords.add("CONDITION");
		reservedWords.add("CONNECTION");
		reservedWords.add("CONSISTENT");
		reservedWords.add("CONSTRAINT");
		reservedWords.add("CONSTRAINT_CATALOG");
		reservedWords.add("CONSTRAINT_NAME");
		reservedWords.add("CONSTRAINT_SCHEMA");
		reservedWords.add("CONTAINS");
		reservedWords.add("CONTEXT");
		reservedWords.add("CONTINUE");
		reservedWords.add("CONVERT");
		reservedWords.add("CPU");
		reservedWords.add("CREATE");
		reservedWords.add("CROSS");
		reservedWords.add("CUBE");
		reservedWords.add("CUME_DIST");
		reservedWords.add("CURRENT");
		reservedWords.add("CURRENT_DATE");
		reservedWords.add("CURRENT_TIME");
		reservedWords.add("CURRENT_TIMESTAMP");
		reservedWords.add("CURRENT_USER");
		reservedWords.add("CURSOR");
		reservedWords.add("CURSOR_NAME");
		reservedWords.add("DATA");
		reservedWords.add("DATABASE");
		reservedWords.add("DATABASES");
		reservedWords.add("DATAFILE");
		reservedWords.add("DATE");
		reservedWords.add("DATETIME");
		reservedWords.add("DAY");
		reservedWords.add("DAY_HOUR");
		reservedWords.add("DAY_MICROSECOND");
		reservedWords.add("DAY_MINUTE");
		reservedWords.add("DAY_SECOND");
		reservedWords.add("DEALLOCATE");
		reservedWords.add("DEC");
		reservedWords.add("DECIMAL");
		reservedWords.add("DECLARE");
		reservedWords.add("DEFAULT");
		reservedWords.add("DEFAULT_AUTH");
		reservedWords.add("DEFINER");
		reservedWords.add("DEFINITION");
		reservedWords.add("DELAYED");
		reservedWords.add("DELAY_KEY_WRITE");
		reservedWords.add("DELETE");
		reservedWords.add("DENSE_RANK");
		reservedWords.add("DESC");
		reservedWords.add("DESCRIBE");
		reservedWords.add("DESCRIPTION");
		reservedWords.add("DES_KEY_FILE");
		reservedWords.add("DETERMINISTIC");
		reservedWords.add("DIAGNOSTICS");
		reservedWords.add("DIRECTORY");
		reservedWords.add("DISABLE");
		reservedWords.add("DISCARD");
		reservedWords.add("DISK");
		reservedWords.add("DISTINCT");
		reservedWords.add("DISTINCTROW");
		reservedWords.add("DIV");
		reservedWords.add("DO");
		reservedWords.add("DOUBLE");
		reservedWords.add("DROP");
		reservedWords.add("DUAL");
		reservedWords.add("DUMPFILE");
		reservedWords.add("DUPLICATE");
		reservedWords.add("DYNAMIC");
		reservedWords.add("EACH");
		reservedWords.add("ELSE");
		reservedWords.add("ELSEIF");
		reservedWords.add("EMPTY");
		reservedWords.add("ENABLE");
		reservedWords.add("ENCLOSED");
		reservedWords.add("ENCRYPTION");
		reservedWords.add("END");
		reservedWords.add("ENDS");
		reservedWords.add("ENFORCED");
		reservedWords.add("ENGINE");
		reservedWords.add("ENGINES");
		reservedWords.add("ENUM");
		reservedWords.add("ERROR");
		reservedWords.add("ERRORS");
		reservedWords.add("ESCAPE");
		reservedWords.add("ESCAPED");
		reservedWords.add("EVENT");
		reservedWords.add("EVENTS");
		reservedWords.add("EVERY");
		reservedWords.add("EXCEPT");
		reservedWords.add("EXCHANGE");
		reservedWords.add("EXCLUDE");
		reservedWords.add("EXECUTE");
		reservedWords.add("EXISTS");
		reservedWords.add("EXIT");
		reservedWords.add("EXPANSION");
		reservedWords.add("EXPIRE");
		reservedWords.add("EXPLAIN");
		reservedWords.add("EXPORT");
		reservedWords.add("EXTENDED");
		reservedWords.add("EXTENT_SIZE");
		reservedWords.add("FAILED_LOGIN_ATTEMPTS");
		reservedWords.add("FALSE");
		reservedWords.add("FAST");
		reservedWords.add("FAULTS");
		reservedWords.add("FETCH");
		reservedWords.add("FIELDS");
		reservedWords.add("FILE");
		reservedWords.add("FILE_BLOCK_SIZE");
		reservedWords.add("FILTER");
		reservedWords.add("FIRST");
		reservedWords.add("FIRST_VALUE");
		reservedWords.add("FIXED");
		reservedWords.add("FLOAT");
		reservedWords.add("FLOAT4");
		reservedWords.add("FLOAT8");
		reservedWords.add("FLUSH");
		reservedWords.add("FOLLOWING");
		reservedWords.add("FOLLOWS");
		reservedWords.add("FOR");
		reservedWords.add("FORCE");
		reservedWords.add("FOREIGN");
		reservedWords.add("FORMAT");
		reservedWords.add("FOUND");
		reservedWords.add("FROM");
		reservedWords.add("FULL");
		reservedWords.add("FULLTEXT");
		reservedWords.add("FUNCTION");
		reservedWords.add("GENERAL");
		reservedWords.add("GENERATED");
		reservedWords.add("GEOMCOLLECTION");
		reservedWords.add("GEOMETRY");
		reservedWords.add("GEOMETRYCOLLECTION");
		reservedWords.add("GET");
		reservedWords.add("GET_FORMAT");
		reservedWords.add("GET_MASTER_PUBLIC_KEY");
		reservedWords.add("GLOBAL");
		reservedWords.add("GRANT");
		reservedWords.add("GRANTS");
		reservedWords.add("GROUP");
		reservedWords.add("GROUPING");
		reservedWords.add("GROUPS");
		reservedWords.add("GROUP_REPLICATION");
		reservedWords.add("HANDLER");
		reservedWords.add("HASH");
		reservedWords.add("HAVING");
		reservedWords.add("HELP");
		reservedWords.add("HIGH_PRIORITY");
		reservedWords.add("HISTOGRAM");
		reservedWords.add("HISTORY");
		reservedWords.add("HOST");
		reservedWords.add("HOSTS");
		reservedWords.add("HOUR");
		reservedWords.add("HOUR_MICROSECOND");
		reservedWords.add("HOUR_MINUTE");
		reservedWords.add("HOUR_SECOND");
		reservedWords.add("IDENTIFIED");
		reservedWords.add("IF");
		reservedWords.add("IGNORE");
		reservedWords.add("IGNORE_SERVER_IDS");
		reservedWords.add("IMPORT");
		reservedWords.add("IN");
		reservedWords.add("INACTIVE");
		reservedWords.add("INDEX");
		reservedWords.add("INDEXES");
		reservedWords.add("INFILE");
		reservedWords.add("INITIAL_SIZE");
		reservedWords.add("INNER");
		reservedWords.add("INOUT");
		reservedWords.add("INSENSITIVE");
		reservedWords.add("INSERT");
		reservedWords.add("INSERT_METHOD");
		reservedWords.add("INSTALL");
		reservedWords.add("INSTANCE");
		reservedWords.add("INT");
		reservedWords.add("INT1");
		reservedWords.add("INT2");
		reservedWords.add("INT3");
		reservedWords.add("INT4");
		reservedWords.add("INT8");
		reservedWords.add("INTEGER");
		reservedWords.add("INTERVAL");
		reservedWords.add("INTO");
		reservedWords.add("INVISIBLE");
		reservedWords.add("INVOKER");
		reservedWords.add("IO");
		reservedWords.add("IO_AFTER_GTIDS");
		reservedWords.add("IO_BEFORE_GTIDS");
		reservedWords.add("IO_THREAD");
		reservedWords.add("IPC");
		reservedWords.add("IS");
		reservedWords.add("ISOLATION");
		reservedWords.add("ISSUER");
		reservedWords.add("ITERATE");
		reservedWords.add("JOIN");
		reservedWords.add("JSON");
		reservedWords.add("JSON_TABLE");
		reservedWords.add("KEY");
		reservedWords.add("KEYS");
		reservedWords.add("KEY_BLOCK_SIZE");
		reservedWords.add("KILL");
		reservedWords.add("LAG");
		reservedWords.add("LANGUAGE");
		reservedWords.add("LAST");
		reservedWords.add("LAST_VALUE");
		reservedWords.add("LATERAL");
		reservedWords.add("LEAD");
		reservedWords.add("LEADING");
		reservedWords.add("LEAVE");
		reservedWords.add("LEAVES");
		reservedWords.add("LEFT");
		reservedWords.add("LESS");
		reservedWords.add("LEVEL");
		reservedWords.add("LIKE");
		reservedWords.add("LIMIT");
		reservedWords.add("LINEAR");
		reservedWords.add("LINES");
		reservedWords.add("LINESTRING");
		reservedWords.add("LIST");
		reservedWords.add("LOAD");
		reservedWords.add("LOCAL");
		reservedWords.add("LOCALTIME");
		reservedWords.add("LOCALTIMESTAMP");
		reservedWords.add("LOCK");
		reservedWords.add("LOCKED");
		reservedWords.add("LOCKS");
		reservedWords.add("LOGFILE");
		reservedWords.add("LOGS");
		reservedWords.add("LONG");
		reservedWords.add("LONGBLOB");
		reservedWords.add("LONGTEXT");
		reservedWords.add("LOOP");
		reservedWords.add("LOW_PRIORITY");
		reservedWords.add("MASTER");
		reservedWords.add("MASTER_AUTO_POSITION");
		reservedWords.add("MASTER_BIND");
		reservedWords.add("MASTER_COMPRESSION_ALGORITHMS");
		reservedWords.add("MASTER_CONNECT_RETRY");
		reservedWords.add("MASTER_DELAY");
		reservedWords.add("MASTER_HEARTBEAT_PERIOD");
		reservedWords.add("MASTER_HOST");
		reservedWords.add("MASTER_LOG_FILE");
		reservedWords.add("MASTER_LOG_POS");
		reservedWords.add("MASTER_PASSWORD");
		reservedWords.add("MASTER_PORT");
		reservedWords.add("MASTER_PUBLIC_KEY_PATH");
		reservedWords.add("MASTER_RETRY_COUNT");
		reservedWords.add("MASTER_SERVER_ID");
		reservedWords.add("MASTER_SSL");
		reservedWords.add("MASTER_SSL_CA");
		reservedWords.add("MASTER_SSL_CAPATH");
		reservedWords.add("MASTER_SSL_CERT");
		reservedWords.add("MASTER_SSL_CIPHER");
		reservedWords.add("MASTER_SSL_CRL");
		reservedWords.add("MASTER_SSL_CRLPATH");
		reservedWords.add("MASTER_SSL_KEY");
		reservedWords.add("MASTER_SSL_VERIFY_SERVER_CERT");
		reservedWords.add("MASTER_TLS_CIPHERSUITES");
		reservedWords.add("MASTER_TLS_VERSION");
		reservedWords.add("MASTER_USER");
		reservedWords.add("MASTER_ZSTD_COMPRESSION_LEVEL");
		reservedWords.add("MATCH");
		reservedWords.add("MAXVALUE");
		reservedWords.add("MAX_CONNECTIONS_PER_HOUR");
		reservedWords.add("MAX_QUERIES_PER_HOUR");
		reservedWords.add("MAX_ROWS");
		reservedWords.add("MAX_SIZE");
		reservedWords.add("MAX_UPDATES_PER_HOUR");
		reservedWords.add("MAX_USER_CONNECTIONS");
		reservedWords.add("MEDIUM");
		reservedWords.add("MEDIUMBLOB");
		reservedWords.add("MEDIUMINT");
		reservedWords.add("MEDIUMTEXT");
		reservedWords.add("MEMBER");
		reservedWords.add("MEMORY");
		reservedWords.add("MERGE");
		reservedWords.add("MESSAGE_TEXT");
		reservedWords.add("MICROSECOND");
		reservedWords.add("MIDDLEINT");
		reservedWords.add("MIGRATE");
		reservedWords.add("MINUTE");
		reservedWords.add("MINUTE_MICROSECOND");
		reservedWords.add("MINUTE_SECOND");
		reservedWords.add("MIN_ROWS");
		reservedWords.add("MOD");
		reservedWords.add("MODE");
		reservedWords.add("MODIFIES");
		reservedWords.add("MODIFY");
		reservedWords.add("MONTH");
		reservedWords.add("MULTILINESTRING");
		reservedWords.add("MULTIPOINT");
		reservedWords.add("MULTIPOLYGON");
		reservedWords.add("MUTEX");
		reservedWords.add("MYSQL_ERRNO");
		reservedWords.add("NAME");
		reservedWords.add("NAMES");
		reservedWords.add("NATIONAL");
		reservedWords.add("NATURAL");
		reservedWords.add("NCHAR");
		reservedWords.add("NDB");
		reservedWords.add("NDBCLUSTER");
		reservedWords.add("NESTED");
		reservedWords.add("NETWORK_NAMESPACE");
		reservedWords.add("NEVER");
		reservedWords.add("NEW");
		reservedWords.add("NEXT");
		reservedWords.add("NO");
		reservedWords.add("NODEGROUP");
		reservedWords.add("NONE");
		reservedWords.add("NOT");
		reservedWords.add("NOWAIT");
		reservedWords.add("NO_WAIT");
		reservedWords.add("NO_WRITE_TO_BINLOG");
		reservedWords.add("NTH_VALUE");
		reservedWords.add("NTILE");
		reservedWords.add("NULL");
		reservedWords.add("NULLS");
		reservedWords.add("NUMBER");
		reservedWords.add("NUMERIC");
		reservedWords.add("NVARCHAR");
		reservedWords.add("OF");
		reservedWords.add("OFFSET");
		reservedWords.add("OJ");
		reservedWords.add("OLD");
		reservedWords.add("ON");
		reservedWords.add("ONE");
		reservedWords.add("ONLY");
		reservedWords.add("OPEN");
		reservedWords.add("OPTIMIZE");
		reservedWords.add("OPTIMIZER_COSTS");
		reservedWords.add("OPTION");
		reservedWords.add("OPTIONAL");
		reservedWords.add("OPTIONALLY");
		reservedWords.add("OPTIONS");
		reservedWords.add("OR");
		reservedWords.add("ORDER");
		reservedWords.add("ORDINALITY");
		reservedWords.add("ORGANIZATION");
		reservedWords.add("OTHERS");
		reservedWords.add("OUT");
		reservedWords.add("OUTER");
		reservedWords.add("OUTFILE");
		reservedWords.add("OVER");
		reservedWords.add("OWNER");
		reservedWords.add("PACK_KEYS");
		reservedWords.add("PAGE");
		reservedWords.add("PARSER");
		reservedWords.add("PARTIAL");
		reservedWords.add("PARTITION");
		reservedWords.add("PARTITIONING");
		reservedWords.add("PARTITIONS");
		reservedWords.add("PASSWORD");
		reservedWords.add("PASSWORD_LOCK_TIME");
		reservedWords.add("PATH");
		reservedWords.add("PERCENT_RANK");
		reservedWords.add("PERSIST");
		reservedWords.add("PERSIST_ONLY");
		reservedWords.add("PHASE");
		reservedWords.add("PLUGIN");
		reservedWords.add("PLUGINS");
		reservedWords.add("PLUGIN_DIR");
		reservedWords.add("POINT");
		reservedWords.add("POLYGON");
		reservedWords.add("PORT");
		reservedWords.add("PRECEDES");
		reservedWords.add("PRECEDING");
		reservedWords.add("PRECISION");
		reservedWords.add("PREPARE");
		reservedWords.add("PRESERVE");
		reservedWords.add("PREV");
		reservedWords.add("PRIMARY");
		reservedWords.add("PRIVILEGES");
		reservedWords.add("PRIVILEGE_CHECKS_USER");
		reservedWords.add("PROCEDURE");
		reservedWords.add("PROCESS");
		reservedWords.add("PROCESSLIST");
		reservedWords.add("PROFILE");
		reservedWords.add("PROFILES");
		reservedWords.add("PROXY");
		reservedWords.add("PURGE");
		reservedWords.add("QUARTER");
		reservedWords.add("QUERY");
		reservedWords.add("QUICK");
		reservedWords.add("RANDOM");
		reservedWords.add("RANGE");
		reservedWords.add("RANK");
		reservedWords.add("READ");
		reservedWords.add("READS");
		reservedWords.add("READ_ONLY");
		reservedWords.add("READ_WRITE");
		reservedWords.add("REAL");
		reservedWords.add("REBUILD");
		reservedWords.add("RECOVER");
		reservedWords.add("RECURSIVE");
		reservedWords.add("REDOFILE");
		reservedWords.add("REDO_BUFFER_SIZE");
		reservedWords.add("REDUNDANT");
		reservedWords.add("REFERENCE");
		reservedWords.add("REFERENCES");
		reservedWords.add("REGEXP");
		reservedWords.add("RELAY");
		reservedWords.add("RELAYLOG");
		reservedWords.add("RELAY_LOG_FILE");
		reservedWords.add("RELAY_LOG_POS");
		reservedWords.add("RELAY_THREAD");
		reservedWords.add("RELEASE");
		reservedWords.add("RELOAD");
		reservedWords.add("REMOTE");
		reservedWords.add("REMOVE");
		reservedWords.add("RENAME");
		reservedWords.add("REORGANIZE");
		reservedWords.add("REPAIR");
		reservedWords.add("REPEAT");
		reservedWords.add("REPEATABLE");
		reservedWords.add("REPLACE");
		reservedWords.add("REPLICATE_DO_DB");
		reservedWords.add("REPLICATE_DO_TABLE");
		reservedWords.add("REPLICATE_IGNORE_DB");
		reservedWords.add("REPLICATE_IGNORE_TABLE");
		reservedWords.add("REPLICATE_REWRITE_DB");
		reservedWords.add("REPLICATE_WILD_DO_TABLE");
		reservedWords.add("REPLICATE_WILD_IGNORE_TABLE");
		reservedWords.add("REPLICATION");
		reservedWords.add("REQUIRE");
		reservedWords.add("REQUIRE_ROW_FORMAT");
		reservedWords.add("RESET");
		reservedWords.add("RESIGNAL");
		reservedWords.add("RESOURCE");
		reservedWords.add("RESPECT");
		reservedWords.add("RESTART");
		reservedWords.add("RESTORE");
		reservedWords.add("RESTRICT");
		reservedWords.add("RESUME");
		reservedWords.add("RETAIN");
		reservedWords.add("RETURN");
		reservedWords.add("RETURNED_SQLSTATE");
		reservedWords.add("RETURNS");
		reservedWords.add("REUSE");
		reservedWords.add("REVERSE");
		reservedWords.add("REVOKE");
		reservedWords.add("RIGHT");
		reservedWords.add("RLIKE");
		reservedWords.add("ROLE");
		reservedWords.add("ROLLBACK");
		reservedWords.add("ROLLUP");
		reservedWords.add("ROTATE");
		reservedWords.add("ROUTINE");
		reservedWords.add("ROW");
		reservedWords.add("ROWS");
		reservedWords.add("ROW_COUNT");
		reservedWords.add("ROW_FORMAT");
		reservedWords.add("ROW_NUMBER");
		reservedWords.add("RTREE");
		reservedWords.add("SAVEPOINT");
		reservedWords.add("SCHEDULE");
		reservedWords.add("SCHEMA");
		reservedWords.add("SCHEMAS");
		reservedWords.add("SCHEMA_NAME");
		reservedWords.add("SECOND");
		reservedWords.add("SECONDARY");
		reservedWords.add("SECONDARY_ENGINE");
		reservedWords.add("SECONDARY_LOAD");
		reservedWords.add("SECONDARY_UNLOAD");
		reservedWords.add("SECOND_MICROSECOND");
		reservedWords.add("SECURITY");
		reservedWords.add("SELECT");
		reservedWords.add("SENSITIVE");
		reservedWords.add("SEPARATOR");
		reservedWords.add("SERIAL");
		reservedWords.add("SERIALIZABLE");
		reservedWords.add("SERVER");
		reservedWords.add("SESSION");
		reservedWords.add("SET");
		reservedWords.add("SHARE");
		reservedWords.add("SHOW");
		reservedWords.add("SHUTDOWN");
		reservedWords.add("SIGNAL");
		reservedWords.add("SIGNED");
		reservedWords.add("SIMPLE");
		reservedWords.add("SKIP");
		reservedWords.add("SLAVE");
		reservedWords.add("SLOW");
		reservedWords.add("SMALLINT");
		reservedWords.add("SNAPSHOT");
		reservedWords.add("SOCKET");
		reservedWords.add("SOME");
		reservedWords.add("SONAME");
		reservedWords.add("SOUNDS");
		reservedWords.add("SOURCE");
		reservedWords.add("SPATIAL");
		reservedWords.add("SPECIFIC");
		reservedWords.add("SQL");
		reservedWords.add("SQLEXCEPTION");
		reservedWords.add("SQLSTATE");
		reservedWords.add("SQLWARNING");
		reservedWords.add("SQL_AFTER_GTIDS");
		reservedWords.add("SQL_AFTER_MTS_GAPS");
		reservedWords.add("SQL_BEFORE_GTIDS");
		reservedWords.add("SQL_BIG_RESULT");
		reservedWords.add("SQL_BUFFER_RESULT");
		reservedWords.add("SQL_CACHE");
		reservedWords.add("SQL_CALC_FOUND_ROWS");
		reservedWords.add("SQL_NO_CACHE");
		reservedWords.add("SQL_SMALL_RESULT");
		reservedWords.add("SQL_THREAD");
		reservedWords.add("SQL_TSI_DAY");
		reservedWords.add("SQL_TSI_HOUR");
		reservedWords.add("SQL_TSI_MINUTE");
		reservedWords.add("SQL_TSI_MONTH");
		reservedWords.add("SQL_TSI_QUARTER");
		reservedWords.add("SQL_TSI_SECOND");
		reservedWords.add("SQL_TSI_WEEK");
		reservedWords.add("SQL_TSI_YEAR");
		reservedWords.add("SRID");
		reservedWords.add("SSL");
		reservedWords.add("STACKED");
		reservedWords.add("START");
		reservedWords.add("STARTING");
		reservedWords.add("STARTS");
		reservedWords.add("STATS_AUTO_RECALC");
		reservedWords.add("STATS_PERSISTENT");
		reservedWords.add("STATS_SAMPLE_PAGES");
		reservedWords.add("STATUS");
		reservedWords.add("STOP");
		reservedWords.add("STORAGE");
		reservedWords.add("STORED");
		reservedWords.add("STRAIGHT_JOIN");
		reservedWords.add("STRING");
		reservedWords.add("SUBCLASS_ORIGIN");
		reservedWords.add("SUBJECT");
		reservedWords.add("SUBPARTITION");
		reservedWords.add("SUBPARTITIONS");
		reservedWords.add("SUPER");
		reservedWords.add("SUSPEND");
		reservedWords.add("SWAPS");
		reservedWords.add("SWITCHES");
		reservedWords.add("SYSTEM");
		reservedWords.add("TABLE");
		reservedWords.add("TABLES");
		reservedWords.add("TABLESPACE");
		reservedWords.add("TABLE_CHECKSUM");
		reservedWords.add("TABLE_NAME");
		reservedWords.add("TEMPORARY");
		reservedWords.add("TEMPTABLE");
		reservedWords.add("TERMINATED");
		reservedWords.add("TEXT");
		reservedWords.add("THAN");
		reservedWords.add("THEN");
		reservedWords.add("THREAD_PRIORITY");
		reservedWords.add("TIES");
		reservedWords.add("TIME");
		reservedWords.add("TIMESTAMP");
		reservedWords.add("TIMESTAMPADD");
		reservedWords.add("TIMESTAMPDIFF");
		reservedWords.add("TINYBLOB");
		reservedWords.add("TINYINT");
		reservedWords.add("TINYTEXT");
		reservedWords.add("TO");
		reservedWords.add("TRAILING");
		reservedWords.add("TRANSACTION");
		reservedWords.add("TRIGGER");
		reservedWords.add("TRIGGERS");
		reservedWords.add("TRUE");
		reservedWords.add("TRUNCATE");
		reservedWords.add("TYPE");
		reservedWords.add("TYPES");
		reservedWords.add("UNBOUNDED");
		reservedWords.add("UNCOMMITTED");
		reservedWords.add("UNDEFINED");
		reservedWords.add("UNDO");
		reservedWords.add("UNDOFILE");
		reservedWords.add("UNDO_BUFFER_SIZE");
		reservedWords.add("UNICODE");
		reservedWords.add("UNINSTALL");
		reservedWords.add("UNION");
		reservedWords.add("UNIQUE");
		reservedWords.add("UNKNOWN");
		reservedWords.add("UNLOCK");
		reservedWords.add("UNSIGNED");
		reservedWords.add("UNTIL");
		reservedWords.add("UPDATE");
		reservedWords.add("UPGRADE");
		reservedWords.add("USAGE");
		reservedWords.add("USE");
		reservedWords.add("USER");
		reservedWords.add("USER_RESOURCES");
		reservedWords.add("USE_FRM");
		reservedWords.add("USING");
		reservedWords.add("UTC_DATE");
		reservedWords.add("UTC_TIME");
		reservedWords.add("UTC_TIMESTAMP");
		reservedWords.add("VALIDATION");
		reservedWords.add("VALUE");
		reservedWords.add("VALUES");
		reservedWords.add("VARBINARY");
		reservedWords.add("VARCHAR");
		reservedWords.add("VARCHARACTER");
		reservedWords.add("VARIABLES");
		reservedWords.add("VARYING");
		reservedWords.add("VCPU");
		reservedWords.add("VIEW");
		reservedWords.add("VIRTUAL");
		reservedWords.add("VISIBLE");
		reservedWords.add("WAIT");
		reservedWords.add("WARNINGS");
		reservedWords.add("WEEK");
		reservedWords.add("WEIGHT_STRING");
		reservedWords.add("WHEN");
		reservedWords.add("WHERE");
		reservedWords.add("WHILE");
		reservedWords.add("WINDOW");
		reservedWords.add("WITH");
		reservedWords.add("WITHOUT");
		reservedWords.add("WORK");
		reservedWords.add("WRAPPER");
		reservedWords.add("WRITE");
		reservedWords.add("X509");
		reservedWords.add("XA");
		reservedWords.add("XID");
		reservedWords.add("XML");
		reservedWords.add("XOR");
		reservedWords.add("YEAR");
		reservedWords.add("YEAR_MONTH");
		reservedWords.add("ZEROFILL");
		reservedWords.add("ACTIVE");
		reservedWords.add("ADMIN");
		reservedWords.add("ARRAY");
		reservedWords.add("BUCKETS");
		reservedWords.add("CLONE");
		reservedWords.add("COMPONENT");
		reservedWords.add("CUME_DIST");
		reservedWords.add("DEFINITION");
		reservedWords.add("DENSE_RANK");
		reservedWords.add("DESCRIPTION");
		reservedWords.add("EMPTY");
		reservedWords.add("ENFORCED");
		reservedWords.add("EXCEPT");
		reservedWords.add("EXCLUDE");
		reservedWords.add("FAILED_LOGIN_ATTEMPTS");
		reservedWords.add("FIRST_VALUE");
		reservedWords.add("FOLLOWING");
		reservedWords.add("GEOMCOLLECTION");
		reservedWords.add("GET_MASTER_PUBLIC_KEY");
		reservedWords.add("GROUPING");
		reservedWords.add("GROUPS");
		reservedWords.add("HISTOGRAM");
		reservedWords.add("HISTORY");
		reservedWords.add("INACTIVE");
		reservedWords.add("INVISIBLE");
		reservedWords.add("JSON_TABLE");
		reservedWords.add("LAG");
		reservedWords.add("LAST_VALUE");
		reservedWords.add("LATERAL");
		reservedWords.add("LEAD");
		reservedWords.add("LOCKED");
		reservedWords.add("MASTER_COMPRESSION_ALGORITHMS");
		reservedWords.add("MASTER_PUBLIC_KEY_PATH");
		reservedWords.add("MASTER_TLS_CIPHERSUITES");
		reservedWords.add("MASTER_ZSTD_COMPRESSION_LEVEL");
		reservedWords.add("MEMBER");
		reservedWords.add("NESTED");
		reservedWords.add("NETWORK_NAMESPACE");
		reservedWords.add("NOWAIT");
		reservedWords.add("NTH_VALUE");
		reservedWords.add("NTILE");
		reservedWords.add("NULLS");
		reservedWords.add("OF");
		reservedWords.add("OJ");
		reservedWords.add("OLD");
		reservedWords.add("OPTIONAL");
		reservedWords.add("ORDINALITY");
		reservedWords.add("ORGANIZATION");
		reservedWords.add("OTHERS");
		reservedWords.add("OVER");
		reservedWords.add("PASSWORD_LOCK_TIME");
		reservedWords.add("PATH");
		reservedWords.add("PERCENT_RANK");
		reservedWords.add("PERSIST");
		reservedWords.add("PERSIST_ONLY");
		reservedWords.add("PRECEDING");
		reservedWords.add("PRIVILEGE_CHECKS_USER");
		reservedWords.add("PROCESS");
		reservedWords.add("RANDOM");
		reservedWords.add("RANK");
		reservedWords.add("RECURSIVE");
		reservedWords.add("REFERENCE");
		reservedWords.add("REQUIRE_ROW_FORMAT");
		reservedWords.add("RESOURCE");
		reservedWords.add("RESPECT");
		reservedWords.add("RESTART");
		reservedWords.add("RETAIN");
		reservedWords.add("REUSE");
		reservedWords.add("ROLE");
		reservedWords.add("ROW_NUMBER");
		reservedWords.add("SECONDARY");
		reservedWords.add("SECONDARY_ENGINE");
		reservedWords.add("SECONDARY_LOAD");
		reservedWords.add("SECONDARY_UNLOAD");
		reservedWords.add("SKIP");
		reservedWords.add("SRID");
		reservedWords.add("SYSTEM");
		reservedWords.add("THREAD_PRIORITY");
		reservedWords.add("TIES");
		reservedWords.add("UNBOUNDED");
		reservedWords.add("VCPU");
		reservedWords.add("VISIBLE");
		reservedWords.add("WINDOW");
		return reservedWords.contains(word.toUpperCase());
	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		DBTableData data = new DBTableData();
		try {
			int portionSize = DBGitConfig.getInstance().getInteger("core", "PORTION_SIZE", DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000));

			int begin = 1 + portionSize * portionIndex;
			int end = portionSize + portionSize * portionIndex;

			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery("SELECT * FROM \r\n" +
					"(SELECT f.*, ROW_NUMBER() OVER (ORDER BY (select group_concat(column_name separator ', ') from information_schema.columns where \r\n" +
					"table_schema='" + schema + "' and table_name='" + nameTable + "' and upper(column_key)='PRI')) DBGIT_ROW_NUM FROM " + schema + "." + nameTable + " f) s \r\n" +
					"WHERE DBGIT_ROW_NUM BETWEEN " + begin + " and " + end);
			data.setResultSet(rs);
			return data;
		} catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tableData").toString(), e);

			try {
				if (tryNumber <= DBGitConfig.getInstance().getInteger("core", "TRY_COUNT", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000))) {
					try {
						TimeUnit.SECONDS.sleep(DBGitConfig.getInstance().getInteger("core", "TRY_DELAY", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000)));
					} catch (InterruptedException e1) {
						throw new ExceptionDBGitRunTime(e1.getMessage());
					}
					ConsoleWriter.println("Error while getting portion of data, try " + tryNumber);
					getTableDataPortion(schema, nameTable, portionIndex, tryNumber++);
				}
			} catch (Exception e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}

			try {
				getConnection().rollback();
			} catch (Exception e2) {
				logger.error(lang.getValue("errors", "adapter", "rollback").toString(), e2);
			}
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}
}
