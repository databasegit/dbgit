package ru.fusionsoft.dbgit.mysql;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBDomain;
import ru.fusionsoft.dbgit.dbobjects.DBEnum;
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
import ru.fusionsoft.dbgit.dbobjects.DBUserDefinedType;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import ru.fusionsoft.dbgit.utils.StringProperties;

public class DBAdapterMySql extends DBAdapter {
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	
	private FactoryDBRestoreAdapterMySql restoreFactory = new FactoryDBRestoreAdapterMySql();
	private FactoryDBConvertAdapterMySql convertFactory = new FactoryDBConvertAdapterMySql();
	private FactoryDBBackupAdapterMySql backupFactory = new FactoryDBBackupAdapterMySql();
	public final static Set<String> reservedWords = new HashSet<>();

	public String escapeNameIfNeeded(String name)  {
		boolean shouldBeEscaped = false;
		//TODO Permitted characters in unquoted identifiers:
		//ASCII: [0-9,a-z,A-Z$_] (basic Latin letters, digits 0-9, dollar, underscore)
		//Extended: U+0080 .. U+FFFF
		//Permitted characters in quoted identifiers include the full Unicode Basic Multilingual Plane (BMP), except U+0000:
		//ASCII: U+0001 .. U+007F
		//Extended: U+0080 .. U+FFFF
		if(reservedWords.contains(name.toUpperCase())) shouldBeEscaped = true;
		if(name.charAt(0) == '`' || name.charAt(name.length()-1) == '`') shouldBeEscaped = false;
		if(shouldBeEscaped){
			return MessageFormat.format("`{0}`", name);
		}
		return name;
	}

	@Override
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		return restoreFactory;
	}
	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return convertFactory;
	}
	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		return backupFactory;
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() {
		return true;
	}

	@Override
	public DbType getDbType() {
		return DbType.MYSQL;
	}
	@Override
	public String getDbVersion() {
		try (
			PreparedStatement stmt = getConnection().prepareStatement("SELECT version()");
			ResultSet resultSet = stmt.executeQuery();
		) {
			if(!resultSet.next()) throw new ExceptionDBGitRunTime("failed to get db version resultset");
			final String result = resultSet.getString(1);

			return result;
		} catch (SQLException e) {
			throw new ExceptionDBGitRunTime("failed to get db version resultset");
		}
	}
	@Override
	public String getDefaultScheme() {
		try {
			return getConnection().getCatalog();
		} catch (SQLException e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "getSchema");
			throw new ExceptionDBGitRunTime(msg);
		}
	}

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		final String query =
			"select count(*) cnt " +
			"from information_schema.schemata " +
			"where upper(schema_name) = '" + schemaName.toUpperCase() + "'";

		try (
			Statement st = connect.createStatement();
			ResultSet rs = st.executeQuery(query);
		) {

			if(!rs.next()) throw new ExceptionDBGitRunTime("failed to get schemas count resultset");

			if (rs.getInt("cnt") == 0){
				try(StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());) {
					stLog.execute("create schema " + schemaName);
				}
			}

		} catch (SQLException e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "createSchema");
			throw new ExceptionDBGit(msg);
		}
	}
	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean isReservedWord(String word) {
		return reservedWords.contains(word.toUpperCase());
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
		return new TreeMapMetaObject(Collections.emptyList());
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		final Map<String, DBSchema> listScheme = new HashMap<String, DBSchema>();
		final String query =
			"select schema_name\r\n" +
			"from information_schema.schemata";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		){

			while(rs.next()){
				final String name = rs.getString("schema_name");
				final DBSchema scheme = new DBSchema(name, new StringProperties(rs));
				listScheme.put(name, scheme);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "schemes").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		} 

		return listScheme;
	}

	@Override
	public Map<String, DBTableSpace> getTableSpaces() {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
		final Map<String, DBSequence> sequences = new HashMap<>();
		final String query =
			" select column_name, table_name, column_type, extra " +
			" from information_schema.columns" +
			" where extra like '%auto_increment%' and table_schema='" + schema + "'";

		try(
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()) {

				final String valueQuery =
					" select coalesce(max(" + rs.getString("column_name") + "), 0) as nextval" +
					" from " + schema + ".`" + rs.getString("table_name") + "`";

				try(Statement stmtValue = getConnection().createStatement(); ResultSet rsValue = stmtValue.executeQuery(valueQuery)){
					if(rsValue.next()){
						final String name = rs.getString("column_name");
						final String owner = "";
						final Long value = rsValue.getLong("nextval");
						final DBSequence seq = new DBSequence(name, new StringProperties(rs), schema, owner, Collections.emptySet(), value);
						sequences.put(name, seq);
					}
				}

			}
		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "seq").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
		return sequences;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {
		final String query =
			"select column_name, table_name, column_type, extra from information_schema.columns" +
			" where extra like '%auto_increment%' and table_schema='" + schema + "' and column_name='" + name + "'";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		){

			if(rs.next()) {
				final String valueQuery =
					"select coalesce(max(" + rs.getString("column_name") + "), 0) as nextval" +
					" from " + schema + ".`" + rs.getString("table_name") + "`";

				try(
					Statement stmtValue = connect.createStatement();
					ResultSet rsValue = stmtValue.executeQuery(valueQuery)
				){
					if(!rsValue.next()) throw new ExceptionDBGitRunTime("failed to get seq value resultset");

					final String nameSeq = rs.getString("column_name");
					final String ownerSeq = "";
					final Long valueSeq = rsValue.getLong("nextval");

					return new DBSequence(nameSeq, new StringProperties(rs), schema, ownerSeq, Collections.emptySet(), valueSeq);
				}
			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch (Exception e) {
			final String msg = lang.getValue("errors", "adapter", "seq").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBTable> getTables(String schema) {
		final Map<String, DBTable> listTable = new HashMap<>();
		final String query =
			"SELECT T.TABLE_NAME, T.TABLE_SCHEMA " +
			"FROM information_schema.tables T WHERE TABLE_SCHEMA = '" + schema + "' and TABLE_TYPE = 'BASE TABLE'";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		){

			while(rs.next()){
				//TODO retrieve table comment
				//TODO retrieve table owner

				final String nameTable = rs.getString("TABLE_NAME");
				final String ownerTable = "";
				final String commentTable = "";
				final String ddlQuery = "show create table " + schema + ".`" + nameTable + "`";
				final StringProperties options = new StringProperties(rs);
				final Set<String> dependencies = rs.getArray("dependencies") != null
					? new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()))
					: Collections.emptySet();

				try(
					Statement stmtDdl = getConnection().createStatement();
					ResultSet rsDdl = stmtDdl.executeQuery(ddlQuery);
				){
					if(!rsDdl.next()) throw new ExceptionDBGitRunTime("failed to get table ddl resultset");
					options.addChild("ddl", cleanString(rsDdl.getString(2)));
				}

				final DBTable table = new DBTable(nameTable, options, schema, ownerTable, dependencies, commentTable);
				listTable.put(nameTable, table);

			}
		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tables").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listTable;
	}

	@Override
	public DBTable getTable(String schema, String name) {
		final String query =
			"SELECT T.TABLE_NAME, T.TABLE_SCHEMA FROM information_schema.tables T" +
			" WHERE TABLE_SCHEMA = '" + schema + "'" +
			" AND T.TABLE_NAME = '" + name + "'";

		try (Statement stmt = connect.createStatement(); ResultSet rs = stmt.executeQuery(query);){

			if (rs.next()){
				//TODO retrieve table comment
				//TODO retrieve table owner
				final String nameTable = rs.getString("TABLE_NAME");
				final String ownerTable = "";
				final String commentTable = "";
				final StringProperties options = new StringProperties(rs);
				final Set<String> dependencies = rs.getArray("dependencies") != null
						? new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()))
						: Collections.emptySet();

				final String ddlQuery = "show create table " + schema + ".`" + nameTable + "`";
				try(Statement stmtDdl = getConnection().createStatement(); ResultSet rsDdl = stmtDdl.executeQuery(ddlQuery);){
					rsDdl.next();
					options.addChild("ddl", cleanString(rsDdl.getString(2)));
				}
				return new DBTable(nameTable, options, schema, ownerTable, dependencies, commentTable);

			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "tables").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		final Map<String, DBTableField> listField = new HashMap<String, DBTableField>();
		final String query =
			"SELECT DISTINCT " +
			"	col.column_name, col.is_nullable, col.data_type, col.character_maximum_length, tc.constraint_name, " +
			"	case\r\n" +
			"		when lower(data_type) in ('tinyint', 'smallint', 'mediumint', 'int', 'bigint', 'float', 'double', 'decimal') then 'number' \r\n" +
			"		when lower(data_type) in ('tinytext', 'text', 'char', 'mediumtext', 'longtext', 'varchar') then 'string'\r\n" +
			"		when lower(data_type) in ('datetime', 'timestamp', 'date') then 'date'\r\n" +
			"		when lower(data_type) in ('boolean') then 'boolean'\r\n" +
			"   	when lower(data_type) in ('blob', 'mediumblob', 'longblob', 'binary', 'varbinary') then 'binary'" +
			"		else 'native'\r\n" +
			"	end tp, " +
			"   case " +
			"		when lower(data_type) in ('char', 'character') " +
			"		then true else false " +
			"	end fixed, " +
			"	col.* " +
			"FROM information_schema.columns col  " +
			"LEFT JOIN information_schema.key_column_usage kc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and col.column_name=kc.column_name " +
			"LEFT JOIN information_schema.table_constraints tc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and kc.constraint_name = tc.constraint_name and tc.constraint_type = 'PRIMARY KEY' " +
			"WHERE col.table_schema = :schema and col.table_name = :table " +
			"ORDER BY col.column_name ";

		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema, "table", nameTable));
			ResultSet rs = stmt.executeQuery();
		) {

			while(rs.next()){

				//TODO make restore defaultValue, description (comment) and order in MySQL
				final String columnName    = rs.getString("column_name").toLowerCase();
				final String typeSQL       = getFieldType(rs);
				final FieldType typeUniversal = FieldType.fromString(rs.getString("tp"));
				final String defaultValue  = rs.getString("column_name") != null    ? rs.getString("column_name").toLowerCase()    : "";
				final String description   = rs.getString("column_comment") != null ? rs.getString("column_comment").toLowerCase() : "";
				final int order 		   = rs.getInt("ordinal_position");
				final boolean isPrimaryKey = rs.getString("constraint_name") != null;
				final boolean isFixed      = rs.getBoolean("isFixed");
				final boolean isNullable   = !typeSQL.toLowerCase().contains("not null");
				final int length           = rs.getInt("character_maximum_length");
				final int precision        = rs.getInt("numeric_precision");
				final int scale            = rs.getInt("numeric_scale");

				final DBTableField field = new DBTableField(
					columnName, description, isPrimaryKey, isNullable,
					typeSQL, typeUniversal, order, defaultValue,
					length, scale, precision, isFixed
				);

				listField.put(field.getName(), field);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listField;
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		final Map<String, DBIndex> indexes = new HashMap<>();
		final String query =
			"select TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE, GROUP_CONCAT(COLUMN_NAME separator '`, `') as FIELDS "
			+ "from INFORMATION_SCHEMA.STATISTICS where TABLE_SCHEMA = '" + schema + "' and INDEX_NAME != 'PRIMARY' "
			+ "group by TABLE_NAME, INDEX_NAME, INDEX_TYPE, NON_UNIQUE order by TABLE_NAME, INDEX_NAME;";

		try(
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()) {

				//TODO find real owner
				final String indexName = rs.getString("INDEX_NAME");
				final String sql = cleanString(
					"create " + (rs.getInt("NON_UNIQUE") == 1 ? "" : "unique ")
					+ "index `" + indexName
					+ "` using " + rs.getString("INDEX_TYPE")
					+ " on " + schema + ".`" + rs.getString("TABLE_NAME") + "`"
					+ "(`" + rs.getString("FIELDS") + "`)"
				);

				final DBIndex index = new DBIndex(indexName, new StringProperties(rs), schema, schema, Collections.emptySet(), sql);
				indexes.put(indexName, index);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "indexes").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return indexes;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		final Map<String, DBView> listView = new HashMap<String, DBView>();
		final String query = "show full tables in " + schema + " where TABLE_TYPE like 'VIEW'";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()){

				//TODO try find real owner
				final String name = rs.getString(1);
				final String ddlQuery = "show create view " + schema + "." + name;
				String sql = "";

				try(
					Statement stmtDdl = getConnection().createStatement();
					ResultSet rsDdl = stmtDdl.executeQuery(ddlQuery);
				){
					if(rsDdl.next()){
						sql = cleanString(rsDdl.getString(2));
					} else {
						String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
						throw new ExceptionDBGitObjectNotFound(msg);
					}
				}

				final DBView view = new DBView(name, new StringProperties(rs), schema, schema, Collections.emptySet(), sql);
				listView.put(name, view);
			}

		} catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "views");
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listView;

	}

	@Override
	public DBView getView(String schema, String name) {
		//TODO find real owner
		final String query = "show create view " + schema + ".`" + name + "`";

		try (
			Statement stmtDdl = getConnection().createStatement();
			ResultSet rsDdl = stmtDdl.executeQuery(query);
		) {

			if(rsDdl.next()) {
				final String sql = cleanString(rsDdl.getString(2));
				return new DBView(name, new StringProperties(rsDdl), schema, schema, Collections.emptySet(), sql);
			} else {
				String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "views");
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		return Collections.emptyMap();
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		throw new ExceptionDBGitRunTime(new ExceptionDBGitObjectNotFound("cannot get packages on mysql"));
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		return Collections.emptyMap();
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		throw new ExceptionDBGitRunTime(new ExceptionDBGitObjectNotFound("cannot get procedure on mysql"));
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		final Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		final String query =
			"SELECT R.routine_schema as \"schema\", R.definer as \"rolname\", R.specific_name as \"name\"," +
			"group_concat(concat(P.parameter_name, \" \", P.data_type)) as \"arguments\", R.routine_definition as \"ddl\"\r\n" +
			"FROM information_schema.routines as R, information_schema.parameters as P\r\n" +
			"WHERE P.parameter_mode='IN' and P.routine_type=R.routine_type and P.specific_schema=R.routine_schema and\r\n" +
			"P.specific_name=R.specific_name and R.routine_type='FUNCTION' and R.routine_schema='" + schema + "'" +
			" GROUP BY R.specific_name,1,2,5,P.ordinal_position ORDER BY P.ordinal_position";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()) {
				final String name = rs.getString("name");
				final String owner = rs.getString("rolname");
				final String sql = rs.getString("ddl");

				DBFunction func = new DBFunction(name, new StringProperties(rs), schema, owner, Collections.emptySet(), sql);
				//String args = rs.getString("arguments");
				//func.setArguments(args);
                listFunction.put(name, func);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
		}

		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		final DBFunction function = null;

		final String query =
			"SELECT R.routine_schema as \"schema\", R.definer as \"rolname\", R.specific_name as \"name\"," +
			"group_concat(concat(P.parameter_name, \" \", P.data_type)) as \"arguments\", R.routine_definition as \"ddl\"\r\n" +
			"FROM information_schema.routines as R, information_schema.parameters as P\r\n" +
			"WHERE P.parameter_mode='IN' and P.routine_type=R.routine_type and P.specific_schema=R.routine_schema and\r\n" +
			"P.specific_name=R.specific_name and R.routine_type='FUNCTION' and R.routine_schema='" + schema + "'" +
			" and R.specific_name='" + name + "'\r\n" +
			"GROUP BY R.specific_name,1,2,5,P.ordinal_position ORDER BY P.ordinal_position";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

            if(rs.next()) {
				final String owner = rs.getString("rolname");
				final String sql = rs.getString("ddl");

				//String args = rs.getString("arguments");
				//function.setArguments(args);
				return new DBFunction(name, new StringProperties(rs), schema, owner, Collections.emptySet(), sql);
			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

        } catch (Exception e) {
			final String msg = lang.getValue("errors", "adapter", "fnc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
        }

	}

	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {

		final Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		final String query = "show triggers in " + schema;

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {
			while(rs.next()){
				//TODO find real owner
				final String name = rs.getString(1);
				final String ddlQuery = "show create trigger " + schema + "." + name;

				try(
					Statement stmtDdl = getConnection().createStatement();
					ResultSet rsDdl = stmtDdl.executeQuery(ddlQuery);
				){
					if(!rsDdl.next()) throw new ExceptionDBGitRunTime("failed to get ddl resultset");

					final String sql = rsDdl.getString(3);
					final StringProperties options = new StringProperties(rs);
					final DBTrigger trigger = new DBTrigger(name, options, schema, schema, Collections.emptySet(), sql);

					listTrigger.put(name, trigger);
				}

			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "triggers").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listTrigger;
	}

	@Override
	public DBTrigger getTrigger(String schema, String name) {
		final String query = "show create trigger " + schema + "." + name;

		try (
			Statement stmtDdl = getConnection().createStatement();
			ResultSet rsDdl = stmtDdl.executeQuery(query);
		) {

			if(rsDdl.next()){

				final StringProperties options = new StringProperties(rsDdl);
				final String sql = rsDdl.getString("SQL Original Statement");
				return new DBTrigger(name, options, schema, schema, Collections.emptySet(), sql);

			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}

	}

	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		try {

			final int maxRowsCount = DBGitConfig.getInstance().getInteger(
				"core", "MAX_ROW_COUNT_FETCH",
				DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH)
			);
			final boolean toLimitFetch = DBGitConfig.getInstance().getBoolean(
				"core", "LIMIT_FETCH",
				DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true)
			);

			final String tableName = schema + ".`" + nameTable + "`";
			final String dataQuery = "select * from " + tableName;
			final String rowsCountQuery =
				"select COALESCE(count(*), 0) kolvo " +
				"from ( " +
				"	select 1 " +
				"	from " +  tableName + " " +
				"	limit " + (maxRowsCount + 1) + " " +
				") tbl";

			if (toLimitFetch) {

				try (
					Statement st = getConnection().createStatement();
					ResultSet rs = st.executeQuery(rowsCountQuery);
				){
					if(!rs.next()) throw new ExceptionDBGitRunTime("Could not execute rows count query");
					if (rs.getInt("kolvo") > maxRowsCount) {
						return new DBTableData(DBTableData.ERROR_LIMIT_ROWS);
					}
				}

			}
			return new DBTableData(getConnection(), dataQuery);

		} catch(Exception e) {
			final String msg = DBGitLang.getInstance().getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		final int portionSize = DBGitConfig.getInstance().getInteger("core", "PORTION_SIZE", DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000));
		final int beginRowNum = 1 + portionSize * portionIndex;
		final int endRowNum = portionSize + portionSize * portionIndex;

		final String dataQuery =
			"SELECT * " + "\n" +
			"FROM (" + "\n" +
			"	SELECT " + "\n" +
			"		f.*, " + "\n" +
			"		ROW_NUMBER() OVER (" + "\n" +
			"			ORDER BY (" + "\n" +
			"				select group_concat(column_name separator ', ') " + "\n" +
			"				from information_schema.columns " + "\n" +
			"				where table_schema='" + schema + "' " + "\n" +
			"				and table_name='" + nameTable + "' " + "\n" +
			"				and upper(column_key)='PRI'" + "\n" +
			"			)" + "\n" +
			"		) DBGIT_ROW_NUM " + "\n" +
			"	FROM " + schema + "." + nameTable + " f" + "\n" +
			") s \n" +
			"WHERE DBGIT_ROW_NUM BETWEEN " + beginRowNum + " AND " + endRowNum;

		try {

			return new DBTableData(getConnection(), dataQuery);

		} catch(Exception e) {

			final int maxTriesCount = DBGitConfig.getInstance().getInteger("core", "TRY_COUNT", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000));
			final int tryDelay = DBGitConfig.getInstance().getInteger("core", "TRY_DELAY", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000));

			ConsoleWriter.println(e.getLocalizedMessage(), messageLevel);
			ConsoleWriter.detailsPrintln(ExceptionUtils.getStackTrace(e), messageLevel);
			logger.error(lang.getValue("errors", "adapter", "tableData").toString(), e);

			if (tryNumber <= maxTriesCount) {

				final String waitMessage = DBGitLang.getInstance()
					.getValue("errors", "dataTable", "wait")
					.withParams(String.valueOf(tryDelay));

				final String tryAgainMessage = DBGitLang.getInstance()
					.getValue("errors", "dataTable", "tryAgain")
					.withParams(String.valueOf(tryNumber));

				ConsoleWriter.println(waitMessage, messageLevel);
				try { TimeUnit.SECONDS.sleep(tryDelay); } catch (InterruptedException e1) {
					throw new ExceptionDBGitRunTime(e1.getMessage());
				}

				ConsoleWriter.println(tryAgainMessage, messageLevel);
				return getTableDataPortion(schema, nameTable, portionIndex, tryNumber++);

			} else {
				final String msg = DBGitLang.getInstance().getValue("errors", "adapter", "tableData").toString();
				throw new ExceptionDBGitRunTime(msg, e);
			}

		}
	}

	@Override
	public Map<String, DBUser> getUsers() {
		final Map<String, DBUser> users = new HashMap<String, DBUser>();
		final String query = "select User, authentication_string from mysql.user";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()) {
				final String name = rs.getString(1);
				final String password = rs.getString(2);
				final StringProperties options = new StringProperties();
				options.addChild("password", password);

				final DBUser user = new DBUser(name, options);
				users.put(name, user);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}

		return users;
	}

	@Override
	public Map<String, DBRole> getRoles() { return Collections.emptyMap(); }

	@Override
	public Map<String, DBUserDefinedType> getUDTs(String schema) {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, DBDomain> getDomains(String schema) {
		return Collections.emptyMap();
	}

	@Override
	public Map<String, DBEnum> getEnums(String schema) {
		return Collections.emptyMap();
	}

	@Override
	public DBUserDefinedType getUDT(String schema, String name) {
		final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
		throw new ExceptionDBGitObjectNotFound(msg);
	}

	@Override
	public DBDomain getDomain(String schema, String name) {
		final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
		throw new ExceptionDBGitObjectNotFound(msg);
	}

	@Override
	public DBEnum getEnum(String schema, String name) {
		final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
		throw new ExceptionDBGitObjectNotFound(msg);
	}

	protected String getFieldType(ResultSet rs) {
		try {
			final StringBuilder type = new StringBuilder();

			if (!rs.wasNull()) {
				final String typePart = rs.getString("data_type");
				final String lengthPart = "(" + rs.getBigDecimal("character_maximum_length") + ")";
				final String nullablePart = rs.getString("is_nullable").equals("NO") ? " NOT NULL" : "";
				type.append( typePart );
				type.append( lengthPart );
				type.append( nullablePart );
			}

			return type.toString();
		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tables").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}
	private NamedParameterPreparedStatement getParamStatement(String query) throws SQLException {
		return NamedParameterPreparedStatement.createNamedParameterPreparedStatement(getConnection(), query);
	}

	static {
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
	}
}
