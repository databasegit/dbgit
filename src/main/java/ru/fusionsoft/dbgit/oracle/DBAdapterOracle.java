package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.exception.ExceptionUtils;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.data_table.BooleanData;
import ru.fusionsoft.dbgit.data_table.DateData;
import ru.fusionsoft.dbgit.data_table.FactoryCellData;
import ru.fusionsoft.dbgit.data_table.LongData;
import ru.fusionsoft.dbgit.data_table.MapFileData;
import ru.fusionsoft.dbgit.data_table.StringData;
import ru.fusionsoft.dbgit.data_table.TextFileData;
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
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.oracle.FactoryDBAdapterRestoreOracle;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;
import ru.fusionsoft.dbgit.utils.StringProperties;


public class DBAdapterOracle extends DBAdapter {
	final private Logger logger = LoggerUtil.getLogger(this.getClass());
	final private FactoryDBAdapterRestoreOracle restoreFactory = new FactoryDBAdapterRestoreOracle();
	final private FactoryDbConvertAdapterOracle convertFactory = new FactoryDbConvertAdapterOracle();
	final private FactoryDBBackupAdapterOracle backupFactory = new FactoryDBBackupAdapterOracle();
	final private static Set<String> reservedWords = new HashSet<>();


	@Override
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		return restoreFactory;
	}
	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() { return backupFactory; }
	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return convertFactory;
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
	public boolean userHasRightsToGetDdlOfOtherUsers() {
		try {
			String userName = getConnection().getSchema();

			if (userName.equalsIgnoreCase("SYS"))
				return true;

			PreparedStatement stmt = getConnection().prepareStatement
					("SELECT count(1) cnt FROM DBA_ROLE_PRIVS WHERE GRANTEE = ? and GRANTED_ROLE = 'SELECT_CATALOG_ROLE'");
			stmt.setString(1, userName);
			ResultSet resultSet = stmt.executeQuery();
			resultSet.next();

			if (resultSet.getInt(1) == 0) {
				return false;
			} else {
				return true;
			}
		} catch (SQLException e) {
			logger.error(e.getMessage());
			return false;
		}
	}

	@Override
	public DbType getDbType() {
		return DbType.ORACLE;
	}
	@Override
	public String getDbVersion() {
		final String query = "SELECT version FROM V$INSTANCE";
		try (
			PreparedStatement stmt = getConnection().prepareStatement(query);
			ResultSet resultSet = stmt.executeQuery();
		) {

			if(!resultSet.next()) throw new ExceptionDBGitRunTime("get db version resultset is empty");
			final String result = resultSet.getString("version");

			return result;

		} catch (SQLException e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}
	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		try {
			return getConnection().getSchema();
		} catch (SQLException e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "getSchema");
			throw new ExceptionDBGit(msg, e);
		}
	}

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		final String userCountQuery = "select count(*) cnt from all_users where USERNAME = '" + schemaName.toUpperCase() + "'";
		try (
			Statement st = 	connect.createStatement();
			ResultSet rs = st.executeQuery(userCountQuery);
		) {

			if(!rs.next()) throw new ExceptionDBGitRunTime("get schema count empty resultset");
			if (rs.getInt("cnt") == 0) {
				try(StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());){

					final String configureUserQuery = "ALTER USER \"" + schemaName.toUpperCase() + "\" QUOTA UNLIMITED ON SYSTEM";
					final String createUserQuery =
						"create USER \"" + schemaName.toUpperCase() + "\"\r\n" +
						"IDENTIFIED BY \"" + schemaName.toUpperCase() + "\"\r\n" +
						"DEFAULT TABLESPACE \"SYSTEM\"\r\n" +
						"TEMPORARY TABLESPACE \"TEMP\"\r\n" +
						"ACCOUNT UNLOCK";

					stLog.execute(createUserQuery);
					stLog.execute(configureUserQuery);
				}
			}

		} catch (SQLException e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "createSchema");
			throw new ExceptionDBGit(msg, e);
		}

	}
	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		//TODO implement
	}

	@Override
	public boolean isReservedWord(String word) {
		return reservedWords.contains(word.toUpperCase());
	}

	@Override
	public IMapMetaObject loadCustomMetaObjects() {
		return new TreeMapMetaObject(Collections.emptyList());
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		final Map<String, DBSchema> listScheme = new HashMap<String, DBSchema>();
		final String query =
			"SELECT DISTINCT OWNER\n" +
			"FROM DBA_OBJECTS WHERE OWNER != 'PUBLIC' AND OWNER != 'SYSTEM'\n" +
			"AND OWNER != 'SYS' AND OWNER != 'APPQOSSYS' AND OWNER != 'OUTLN' \n" +
			"AND OWNER != 'DIP' AND OWNER != 'DBSNMP' AND OWNER != 'ORACLE_OCM'\n" +
			"ORDER BY OWNER";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("OWNER");
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
		final Map<String, DBTableSpace> listTableSpace = new HashMap<String, DBTableSpace>();
		final String query =
			"SELECT owner,\n" +
			"       segment_name,\n" +
			"       partition_name,\n" +
			"       segment_type,\n" +
			"       bytes \n" +
			"  FROM dba_segments \n" +
			" WHERE OWNER != 'PUBLIC' AND OWNER != 'SYSTEM'\n" +
			"AND OWNER != 'SYS' AND OWNER != 'APPQOSSYS' AND OWNER != 'OUTLN' \n" +
			"AND OWNER != 'DIP' AND OWNER != 'DBSNMP' AND OWNER != 'ORACLE_OCM' and segment_name not like 'SYS%'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("segment_name");
				final DBTableSpace dbTableSpace = new DBTableSpace(name, new StringProperties(rs));
				listTableSpace.put(name, dbTableSpace);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tablespace").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listTableSpace;
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
		final Map<String, DBSequence> listSequence = new HashMap<String, DBSequence>();

		//variant 1 from DBA_OBJECTS
		/*String query =
			"SELECT ROWNUM AS NUM, OWNER, OBJECT_NAME, SUBOBJECT_NAME, OBJECT_TYPE, STATUS,\n" +
			"(select dbms_metadata.get_ddl('SEQUENCE', O.OBJECT_NAME) AS DDL from dual) AS DDL\n" +
			"FROM DBA_OBJECTS O WHERE OBJECT_TYPE = 'SEQUENCE' AND OWNER = :schema";*/

		//variant 2 from DBA_SEQUENCES
		final String query =
			"SELECT S.SEQUENCE_NAME, (SELECT dbms_metadata.get_ddl('SEQUENCE', S.SEQUENCE_NAME, S.SEQUENCE_OWNER) from dual) AS DDL,\n" +
			"order_flag, increment_by, last_number, min_value, max_value, cache_size \n" +
			"FROM DBA_SEQUENCES S WHERE S.SEQUENCE_OWNER = '" + schema + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
						
			while(rs.next()){
				//TODO find real sequence value
				final long valueSeq = 0L;
				final String nameSeq = rs.getString("SEQUENCE_NAME");
				final String ownerSeq = "";
				final DBSequence sequence = new DBSequence(nameSeq, new StringProperties(rs), schema, ownerSeq, Collections.emptySet(), valueSeq);
				listSequence.put(nameSeq, sequence);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "seq").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listSequence;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {
		final String query =
			"SELECT S.SEQUENCE_NAME, (SELECT dbms_metadata.get_ddl('SEQUENCE', S.SEQUENCE_NAME, S.SEQUENCE_OWNER) from dual) AS DDL, \n" +
			"order_flag, increment_by, last_number, min_value, max_value, cache_size \n" +
			"FROM DBA_SEQUENCES S WHERE S.SEQUENCE_OWNER = '" + schema + "' AND S.SEQUENCE_NAME = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
			if (rs.next()) {
				//TODO find real sequence value
				final long valueSeq = 0L;
				final String nameSeq = rs.getString("SEQUENCE_NAME");
				final String ownerSeq = "";
				return new DBSequence(nameSeq, new StringProperties(rs), schema, ownerSeq, Collections.emptySet(), valueSeq);
			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "sequence").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBTable> getTables(String schema) {
		final Map<String, DBTable> listTable = new HashMap<String, DBTable>();
		final String query =
			"SELECT T.TABLE_NAME, T.OWNER, (SELECT dbms_metadata.get_ddl('TABLE', T.TABLE_NAME, T.OWNER) from dual) AS DDL\n" +
			"FROM DBA_TABLES T WHERE upper(OWNER) = upper('" + schema + "') and nested = 'NO' and (iot_type <> 'IOT_OVERFLOW' or iot_type is null)";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
			while(rs.next()){
				//TODO retrieve table comment
				//TODO retrieve table owner
				final String nameTable = rs.getString("TABLE_NAME");
				final String ownerTable = "";
				final String commentTable = "";
				final StringProperties options = new StringProperties(rs);
				final Set<String> dependencies = rs.getArray("dependencies") != null
					? new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()))
					: Collections.emptySet();

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
			"SELECT T.TABLE_NAME, T.OWNER, (SELECT dbms_metadata.get_ddl('TABLE', T.TABLE_NAME, T.OWNER) from dual) AS DDL\n" +
			"FROM DBA_TABLES T WHERE upper(T.OWNER) = upper('" + schema + "') AND upper(T.TABLE_NAME) = upper('" + name + "')";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			if (rs.next()) {
				//TODO retrieve table comment
				//TODO retrieve table owner
				final String nameTable = rs.getString("TABLE_NAME");
				final String ownerTable = "";
				final String commentTable = "";
				final StringProperties options = new StringProperties(rs);
				final Set<String> dependencies = rs.getArray("dependencies") != null
					? new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()))
					: Collections.emptySet();

				return new DBTable(nameTable, options, schema, ownerTable, dependencies, commentTable);
			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tables").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		final Map<String, DBTableField> listField = new HashMap<String, DBTableField>();

		final String pkNameQuery =
			"SELECT column_name FROM all_constraints cons, all_cons_columns cols\n"+
			"WHERE upper(cols.table_name) = upper('" + nameTable + "')\n"+
			"AND cons.constraint_type = 'P'\n" +
			"AND cons.constraint_name = cols.constraint_name\n" +
			"AND cons.owner = cols.owner";

		final String query =
			"SELECT " +
			"	case \r\n" +
			"		when lower(data_type) in ('number', 'numeric', 'dec', 'decimal', 'pls_integer') then 'number'\r\n" +
			"		when lower(data_type) in ('varchar2', 'varchar', 'char', 'nchar', 'nvarchar2') then 'string'\r\n" +
			"		when substr(lower(data_type), 1, instr(data_type, '(') - 1) in ('date', 'timestamp') then 'date'\r\n" +
			"		when lower(data_type) in ('date', 'timestamp') then 'date'\r\n" +
			"		when lower(data_type) in ('clob') then 'text'\r\n" +
			"		when lower(data_type) in ('blob') then 'binary'" +
			"    	else 'native'\r\n" +
			"   end type, " +
			"   case " +
			"		when lower(data_type) in ('char', 'nchar') then 1 else 0 " +
			"	end fixed, " +
			"	ROWNUM AS NUM, " +
			"	TC.* \n" +
			"	DTC.COMMENTS \n" +
			"FROM DBA_TAB_COLS TC \n" +
			"LEFT OUTER JOIN dba_tables T " +
            "   ON T.TABLE_NAME = TC.TABLE_NAME " +
            "   AND T.COLUMN_NAME = TC.COLUMN_NAME \n" +
            "   AND T.OWNER = TC.OWNER \n" +
			//So I checked the documentation it turns out Oracle 10g added a column called DROPPED to the USER_/ALL_/DBA_TABLES views.
			( (getDbVersionNumber() >= 10) ?
			"   AND T.DROPPED = 'NO' \n" : "" ) +
            "LEFT OUTER JOIN dba_tab_comment TCOM " +
            "   ON TCOM.OWNER = T.OWNER " +
            "   AND TCOM.TABLE_NAME = T.TABLE_NAME " +
            "   AND TCOM.COLUMN_NAME = TC.COLUMN_NAME " +
			"WHERE lower(table_name) = lower('" + nameTable + "') AND lower(OWNER) = lower('" + schema + "') ORDER BY column_id";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet pkRs = stmt.executeQuery(pkNameQuery);
			ResultSet fieldsRs = stmt.executeQuery(query);
		){

			final Set<String> pkColumnNames = new HashSet<>();
			while (pkRs.next()) { pkColumnNames.add(pkRs.getString("COLUMN_NAME").toLowerCase()); }
			while(fieldsRs.next()){
			//TODO make restore 'description', 'column default'
				final String columnName       = fieldsRs.getString("COLUMN_NAME").toLowerCase();
				final String columnDesc       = fieldsRs.getString("COMMENTS");
				final Long columnDefault      = fieldsRs.getLong("DATA_DEFAULT");
				final String typeSQL          = getFieldType(fieldsRs);
				final FieldType typeUniversal = FieldType.fromString(fieldsRs.getString("TYPE").toUpperCase());
				final int order               = fieldsRs.getInt("column_id");
				final boolean isPrimaryKey    = pkColumnNames.contains(columnName);
				final boolean isNullable      = !typeSQL.toLowerCase().contains("not null");
				final boolean fixed           = fieldsRs.getBoolean("fixed");
				final int dataLength          = fieldsRs.getInt("DATA_LENGTH");
				final int dataScale           = fieldsRs.getInt("DATA_SCALE");
				final int dataPrecision       = fieldsRs.getInt("DATA_PRECISION");


				final DBTableField field = new DBTableField(
					columnName,
					columnDesc != null ? columnDesc : "",
					isPrimaryKey,
					isNullable,
					typeSQL, typeUniversal, order,
					columnDefault != null ? String.valueOf(columnDefault) : "",
					dataLength, dataScale, dataPrecision, fixed
				);


				listField.put(field.getName(), field);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listField;
	}

	protected String getFieldType(ResultSet rs) throws SQLException {
		final StringBuilder type = new StringBuilder();
		final Integer max_length = rs.getInt("CHAR_LENGTH");
		final String data_type = rs.getString("DATA_TYPE");

		type.append(data_type);

		if (!rs.wasNull() && !data_type.contains("(")) {
			type.append("("+max_length.toString()+")");
		}

		if (rs.getString("NULLABLE").equals("N")){
			type.append(" NOT NULL");
		}

		return type.toString();
	}
	
	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		final Map<String, DBIndex> indexes = new HashMap<>();
		final String query =
			"SELECT  ind.index_name, (select dbms_metadata.get_ddl('INDEX', ind.INDEX_NAME, owner) AS DDL from dual) AS DDL\n" +
			"FROM all_indexes ind\n" +
			"WHERE upper(table_name) = upper('" + nameTable + "') AND upper(owner) = upper('" + schema + "')";

		try (Statement stmt = connect.createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				//TODO find real owner
				final String name = rs.getString("INDEX_NAME");
				final String sql = rs.getString("DDL");
				final DBIndex index = new DBIndex(name, new StringProperties(rs), schema, schema, Collections.emptySet(), sql);
				indexes.put(index.getName(), index);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "indexes").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return indexes;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		final Map<String, DBConstraint> constraints = new HashMap<>();

		final String query =
			"SELECT cons.constraint_type, cons.CONSTRAINT_NAME, (select dbms_metadata.get_ddl('CONSTRAINT', cons.constraint_name, owner) AS DDL from dual) AS DDL\n" +
			"FROM all_constraints cons\n" +
			"WHERE upper(owner) = upper('" + schema + "') and upper(table_name) = upper('" + nameTable + "') and constraint_name not like 'SYS%' and cons.constraint_type = 'P'";

		try (Statement stmt = connect.createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				//TODO find real owner
				final String name = rs.getString("CONSTRAINT_NAME");
				final String sql = rs.getString("DDL");
				final String type = rs.getString("CONSTRAINT_TYPE");
				final String owner = schema;
				final StringProperties options = new StringProperties(rs);

				final DBConstraint con = new DBConstraint(name, options, schema, owner, Collections.emptySet(), sql, type);
				constraints.put(con.getName(), con);
			}

		}
		catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "constraints").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return constraints;
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		final Map<String, DBView> listView = new HashMap<String, DBView>();
		final String query =
			"SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('VIEW', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f " +
			"WHERE f.owner = '" + schema + "' and f.object_type = 'VIEW'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				final String name = rs.getString("OBJECT_NAME");
				final String owner = rs.getString("OWNER");
				final String sql = rs.getString("DDL");
				final StringProperties options = new StringProperties(rs);
				final Set<String> dependencies = Collections.emptySet();

				final DBView view = new DBView(name, options, schema, owner, dependencies, sql);
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

		final String query =
			"SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('VIEW', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'VIEW' and f.object_name = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query); ){

			if (!rs.next()) throw new ExceptionDBGitObjectNotFound("view is not found in db");

			final String owner = rs.getString("owner");
			final String sql = rs.getString("DDL");
			final StringProperties options = new StringProperties(rs);
			return new DBView(name, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "views").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}
	
	public Map<String, DBTrigger> getTriggers(String schema) {

		final Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		final String query =
			"SELECT  tr.owner, tr.trigger_name, tr.trigger_type, tr.table_name," +
			" (select dbms_metadata.get_ddl('TRIGGER', tr.trigger_name, tr.owner) AS DDL from dual) AS DDL\n" +
			"FROM all_triggers tr\n" +
			"WHERE owner = '"+ schema +"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				//TODO find real owner
				//what means owner? oracle/postgres or owner like database user/schema
				final String owner = rs.getString("owner");
				final String name = rs.getString("TRIGGER_NAME");
				final String sql = rs.getString("DDL");
				final StringProperties options = new StringProperties(rs);

				final DBTrigger trigger = new DBTrigger(name, options, schema, owner, Collections.emptySet(), sql);
				listTrigger.put(name, trigger);
			}


		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "triggers").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listTrigger;
	}
	
	public DBTrigger getTrigger(String schema, String name) {

		final String query =
			"SELECT  tr.owner, tr.trigger_name, tr.trigger_type, tr.table_name, (select dbms_metadata.get_ddl('TRIGGER', tr.trigger_name, tr.owner) AS DDL from dual) AS DDL\n" +
			"FROM    all_triggers tr\n" +
			"WHERE   owner = '" + schema + "' and trigger_name = '" + name + "'";

		try (Statement stmt = connect.createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			if(!rs.next()) throw new ExceptionDBGitObjectNotFound("trigger is not found in database");

			final String owner = rs.getString("owner");
			final String sql = rs.getString("DDL");
			final StringProperties options = new StringProperties(rs);

			return new DBTrigger(name, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "triggers").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		final Map<String, DBPackage> listPackage = new HashMap<String, DBPackage>();
		final String query =
			"SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PACKAGE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PACKAGE'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query); ){

			while(rs.next()){
				final String name = rs.getString("OBJECT_NAME");
				final String owner = rs.getString("OWNER");
				final String sql = rs.getString("DDL");
				final StringProperties options = new StringProperties(rs);

				//String args = rs.getString("arguments");
				//pack.setArguments(args);

				final DBPackage pack = new DBPackage(name, options, schema, owner, Collections.emptySet(), sql);
				listPackage.put(name, pack);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "pkg").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listPackage;
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		final String query =
			"SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PACKAGE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PACKAGE' and f.object_name = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			if (!rs.next()) throw new ExceptionDBGitObjectNotFound("package is not found in db");

			final String owner = rs.getString("OWNER");
			final String sql = rs.getString("DDL");
			final StringProperties options = new StringProperties(rs);

			//String args = rs.getString("arguments");
			//pack.setArguments(args);
			return new DBPackage(name, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "views").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		final Map<String, DBProcedure> listProcedure = new HashMap<String, DBProcedure>();
		final String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS " +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('PROCEDURE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' " +
			"AND f.object_type = 'PROCEDURE'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				final String name = rs.getString("OBJECT_NAME");
				final String owner = rs.getString("OWNER");
				final String sql = rs.getString("DDL");
				final StringProperties options = new StringProperties(rs);

				//String args = rs.getString("arguments");
				//proc.setArguments(args);
				final DBProcedure proc = new DBProcedure(name, options, schema, owner, Collections.emptySet(), sql);
				listProcedure.put(name, proc);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "prc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listProcedure;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {

		final String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS " +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('PROCEDURE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PROCEDURE' and f.object_name = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			if (!rs.next()) throw new ExceptionDBGitObjectNotFound("procedure is not found in database");

			final String objName = rs.getString("OBJECT_NAME");
			final String owner = rs.getString("OWNER");
			final String sql = rs.getString("DDL");
			final StringProperties options = new StringProperties(rs);

			return new DBProcedure(name, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "prc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		final Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		final String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS \r\n" +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('FUNCTION', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'FUNCTION'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
			while(rs.next()){
				final String name = rs.getString("OBJECT_NAME");
				final String sql = rs.getString("DDL");
				final String owner = rs.getString("OWNER");
				final StringProperties options = new StringProperties(rs);

				//String args = rs.getString("arguments");
				//func.setArguments(args);
				final DBFunction func = new DBFunction(name, options, schema, owner, Collections.emptySet(), sql);
				listFunction.put(name, func);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "fnc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {

		final String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS " +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('FUNCTION', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema +"' and " +
			"f.object_type = 'FUNCTION' and f.object_name = '" + name +"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			if (!rs.next()) throw new ExceptionDBGitObjectNotFound("function is not found in database");

			final String objName = rs.getString("OBJECT_NAME");
			final String owner = rs.getString("OWNER");
			final String sql = rs.getString("DDL");
			final StringProperties options = new StringProperties(rs);


			//String args = rs.getString("arguments");
			//func.setArguments(args);
			return new DBFunction(objName, options, schema, owner, Collections.emptySet(), sql);


		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "fnc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {

		final int portionSize = DBGitConfig.getInstance().getInteger("core", "PORTION_SIZE", DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000));
		final int begin = 1 + portionSize*portionIndex;
		final int end = portionSize + portionSize*portionIndex;

		final String dataQuery =
			"SELECT * FROM (" +
			"	SELECT f.*, ROW_NUMBER() OVER (ORDER BY rowid) DBGIT_ROW_NUM " +
			" 	FROM " + schema + "." + nameTable + " f" +
			") " +
			"WHERE DBGIT_ROW_NUM BETWEEN " + begin  + " and " + end;

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
	public DBTableData getTableData(String schema, String nameTable) {
		final String tableName = schema + "." + nameTable;
		final Boolean isFetchLimited = DBGitConfig.getInstance().getBoolean("core", "LIMIT_FETCH", DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true));
		final int maxRowsCount = DBGitConfig.getInstance().getInteger("core", "MAX_ROW_COUNT_FETCH", DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH));
		final String dataQuery = "select * from " + tableName;
		try {

			if (isFetchLimited) {
				final String rowsCountQuery =
					"select COALESCE(count(*), 0) row_count " +
					"from ( " +
					"	select 1 " +
					"	from " + tableName+ " " +
					"	where ROWNUM <= " + (maxRowsCount + 1) + " " +
					") tbl";
				try(Statement st = getConnection().createStatement(); ResultSet rs = st.executeQuery(rowsCountQuery);){

					if(!rs.next()) throw new ExceptionDBGitRunTime("empty rows count resultset");

					if (rs.getInt("row_count") > maxRowsCount) {
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
	public Map<String, DBUser> getUsers() {
		final Map<String, DBUser> listUser = new HashMap<String, DBUser>();
		final String query =
			"SELECT USERNAME FROM DBA_USERS WHERE USERNAME != 'PUBLIC' AND USERNAME != 'SYSTEM'\n" +
			"AND USERNAME != 'SYS' AND USERNAME != 'APPQOSSYS' AND USERNAME != 'OUTLN' \n" +
			"AND USERNAME != 'DIP' AND USERNAME != 'DBSNMP' AND USERNAME != 'ORACLE_OCM' ORDER BY USERNAME";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		){

			while(rs.next()){
				final String name = rs.getString(1);
				final StringProperties options = new StringProperties(rs);

				final DBUser user = new DBUser(name, options);
				listUser.put(name, user);
			}

		} catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "users");
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listUser;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		final Map<String, DBRole> listRole = new HashMap<String, DBRole>();
		final String query =
			"SELECT R.GRANTEE, \n" +
			"R.GRANTED_ROLE, R.ADMIN_OPTION, R.DEFAULT_ROLE FROM DBA_ROLE_PRIVS R \n" +
			"WHERE R.GRANTEE = (SELECT USERNAME FROM DBA_USERS WHERE USERNAME = R.GRANTEE AND\n" +
			"USERNAME != 'PUBLIC' AND USERNAME != 'SYSTEM'\n" +
			"AND USERNAME != 'SYS' AND USERNAME != 'APPQOSSYS' AND USERNAME != 'OUTLN' \n" +
			"AND USERNAME != 'DIP' AND USERNAME != 'DBSNMP' AND USERNAME != 'ORACLE_OCM')";
		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()){
				final String name = rs.getString("GRANTED_ROLE");
				final StringProperties options = new StringProperties(rs);

				final DBRole role = new DBRole(name, options);
				listRole.put(name, role);
			}

		} catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "roles");
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listRole;
	}

	
	static {
		reservedWords.add("ACCESS");
		reservedWords.add("ADD");
		reservedWords.add("ALL");
		reservedWords.add("ALTER");
		reservedWords.add("AND");
		reservedWords.add("ANY");
		reservedWords.add("AS");
		reservedWords.add("ASC");
		reservedWords.add("AUDIT");
		reservedWords.add("BETWEEN");
		reservedWords.add("BY");
		reservedWords.add("CHAR");
		reservedWords.add("CHECK");
		reservedWords.add("CLUSTER");
		reservedWords.add("COLUMN");
		reservedWords.add("COMMENT");
		reservedWords.add("COMPRESS");
		reservedWords.add("CONNECT");
		reservedWords.add("CREATE");
		reservedWords.add("CURRENT");
		reservedWords.add("DATE");
		reservedWords.add("DECIMAL");
		reservedWords.add("DEFAULT");
		reservedWords.add("DELETE");
		reservedWords.add("DESC");
		reservedWords.add("DISTINCT");
		reservedWords.add("DROP");
		reservedWords.add("ELSE");
		reservedWords.add("EXCLUSIVE");
		reservedWords.add("EXISTS");
		reservedWords.add("FILE");
		reservedWords.add("FLOAT");
		reservedWords.add("FOR");
		reservedWords.add("FROM");
		reservedWords.add("GRANT");
		reservedWords.add("GROUP");
		reservedWords.add("HAVING");
		reservedWords.add("IDENTIFIED");
		reservedWords.add("IMMEDIATE");
		reservedWords.add("IN");
		reservedWords.add("INCREMENT");
		reservedWords.add("INDEX");
		reservedWords.add("INITIAL");
		reservedWords.add("INSERT");
		reservedWords.add("INTEGER");
		reservedWords.add("INTERSECT");
		reservedWords.add("INTO");
		reservedWords.add("IS");
		reservedWords.add("LEVEL");
		reservedWords.add("LIKE");
		reservedWords.add("LOCK");
		reservedWords.add("LONG");
		reservedWords.add("MAXEXTENTS");
		reservedWords.add("MINUS");
		reservedWords.add("MLSLABEL");
		reservedWords.add("MODE");
		reservedWords.add("MODIFY");
		reservedWords.add("NOAUDIT");
		reservedWords.add("NOCOMPRESS");
		reservedWords.add("NOT");
		reservedWords.add("NOWAIT");
		reservedWords.add("NULL");
		reservedWords.add("NUMBER");
		reservedWords.add("OF");
		reservedWords.add("OFFLINE");
		reservedWords.add("ON");
		reservedWords.add("ONLINE");
		reservedWords.add("OPTION");
		reservedWords.add("OR");
		reservedWords.add("ORDER");
		reservedWords.add("PCTFREE");
		reservedWords.add("PRIOR");
		reservedWords.add("PRIVILEGES");
		reservedWords.add("PUBLIC");
		reservedWords.add("RAW");
		reservedWords.add("RENAME");
		reservedWords.add("RESOURCE");
		reservedWords.add("REVOKE");
		reservedWords.add("ROW");
		reservedWords.add("ROWID");
		reservedWords.add("ROWNUM");
		reservedWords.add("ROWS");
		reservedWords.add("SELECT");
		reservedWords.add("SESSION");
		reservedWords.add("SET");
		reservedWords.add("SHARE");
		reservedWords.add("SIZE");
		reservedWords.add("SMALLINT");
		reservedWords.add("START");
		reservedWords.add("SUCCESSFUL");
		reservedWords.add("SYNONYM");
		reservedWords.add("SYSDATE");
		reservedWords.add("TABLE");
		reservedWords.add("THEN");
		reservedWords.add("TO");
		reservedWords.add("TRIGGER");
		reservedWords.add("UID");
		reservedWords.add("UNION");
		reservedWords.add("UNIQUE");
		reservedWords.add("UPDATE");
		reservedWords.add("USER");
		reservedWords.add("VALIDATE");
		reservedWords.add("VALUES");
		reservedWords.add("VARCHAR");
		reservedWords.add("VARCHAR2");
		reservedWords.add("VIEW");
		reservedWords.add("WHENEVER");
		reservedWords.add("WHERE");
		reservedWords.add("WITH");
	}
}
