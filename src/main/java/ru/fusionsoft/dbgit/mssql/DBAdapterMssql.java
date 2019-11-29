package ru.fusionsoft.dbgit.mssql;

import org.slf4j.Logger;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.data_table.*;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;


public class DBAdapterMssql extends DBAdapter {
	public static final String DEFAULT_MAPPING_TYPE = "VARCHAR2";
	private static final HashSet<String> systemSchemas = new HashSet<>(Arrays.asList(
			"db_denydatawriter",
			"db_datawriter",
			"db_accessadmin",
			"db_ddladmin",
			"db_securityadmin",
			"db_denydatareader",
			"db_backupoperator",
			"db_datareader",
			"db_owner",
			"sys",
			"INFORMATION_SCHEMA"
		));

	//Stubs for MSSQL adapter, marked as "TODO Auto-generated method stub"
	//And some unfinished implementations marked as "TODO MSSQL *"

	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestoreMssql restoreFactory = new FactoryDBAdapterRestoreMssql();
	private FactoryDbConvertAdapterMssql convertFactory = new FactoryDbConvertAdapterMssql();
	private FactoryDBBackupAdapterMssql backupFactory = new FactoryDBBackupAdapterMssql();

	@Override
    @SuppressWarnings("Duplicates")
	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(DEFAULT_MAPPING_TYPE, StringData.class);
		FactoryCellData.regMappingTypes("number", LongData.class);
		FactoryCellData.regMappingTypes("date", DateData.class);
		FactoryCellData.regMappingTypes("string", StringData.class);
		FactoryCellData.regMappingTypes("binary", MapFileData.class);
		FactoryCellData.regMappingTypes("text", TextFileData.class);
		FactoryCellData.regMappingTypes("native", StringData.class);
		FactoryCellData.regMappingTypes("boolean", BooleanData.class);
	}

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
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		Map<String, DBSchema> listScheme = new HashMap<String, DBSchema>();
		try {

			Connection connect = getConnection();
			DatabaseMetaData meta = connect.getMetaData();
			ResultSet rs = meta.getSchemas();
			// made without query
			// Statement stmt = connect.createStatement();
			// ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				// May also get catalog names that belong to scheme as "TABLE_CATALOG"
				String name = rs.getString("TABLE_SCHEM");
				if(!systemSchemas.contains(name)) {
					DBSchema scheme = new DBSchema(name);
					rowToProperties(rs, scheme.getOptions());
					listScheme.put(name, scheme);
				}
			}
			//stmt.close();
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
		// TODO Auto-generated method stub
		return null;
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
	public Map<String, DBTrigger> getTriggers(String schema) {
		// TODO Auto-generated method stub
		return null;
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
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBUser> getUsers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDbType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDbVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		// TODO Auto-generated method stub
	}

	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		// TODO Auto-generated method stub
	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	@SuppressWarnings("SpellCheckingInspection")
	public boolean isReservedWord(String word) {
		Set<String> reservedWords = new HashSet<>();

		reservedWords.add("DD");
		reservedWords.add("EXTERNAL");
		reservedWords.add("PROCEDURE");
		reservedWords.add("ALL");
		reservedWords.add("FETCH");
		reservedWords.add("PUBLIC");
		reservedWords.add("ALTER");
		reservedWords.add("FILE");
		reservedWords.add("RAISERROR");
		reservedWords.add("AND");
		reservedWords.add("FILLFACTOR");
		reservedWords.add("READ");
		reservedWords.add("ANY");
		reservedWords.add("FOR");
		reservedWords.add("READTEXT");
		reservedWords.add("AS");
		reservedWords.add("FOREIGN");
		reservedWords.add("RECONFIGURE");
		reservedWords.add("ASC");
		reservedWords.add("FREETEXT");
		reservedWords.add("REFERENCES");
		reservedWords.add("AUTHORIZATION");
		reservedWords.add("FREETEXTTABLE");
		reservedWords.add("REPLICATION");
		reservedWords.add("BACKUP");
		reservedWords.add("FROM");
		reservedWords.add("RESTORE");
		reservedWords.add("BEGIN");
		reservedWords.add("FULL");
		reservedWords.add("RESTRICT");
		reservedWords.add("BETWEEN");
		reservedWords.add("FUNCTION");
		reservedWords.add("RETURN");
		reservedWords.add("BREAK");
		reservedWords.add("GOTO");
		reservedWords.add("REVERT");
		reservedWords.add("BROWSE");
		reservedWords.add("GRANT");
		reservedWords.add("REVOKE");
		reservedWords.add("BULK");
		reservedWords.add("GROUP");
		reservedWords.add("RIGHT");
		reservedWords.add("BY");
		reservedWords.add("HAVING");
		reservedWords.add("ROLLBACK");
		reservedWords.add("CASCADE");
		reservedWords.add("HOLDLOCK");
		reservedWords.add("ROWCOUNT");
		reservedWords.add("CASE");
		reservedWords.add("IDENTITY");
		reservedWords.add("ROWGUIDCOL");
		reservedWords.add("CHECK");
		reservedWords.add("IDENTITY_INSERT");
		reservedWords.add("RULE");
		reservedWords.add("CHECKPOINT");
		reservedWords.add("IDENTITYCOL");
		reservedWords.add("SAVE");
		reservedWords.add("CLOSE");
		reservedWords.add("IF");
		reservedWords.add("SCHEMA");
		reservedWords.add("CLUSTERED");
		reservedWords.add("IN");
		reservedWords.add("SECURITYAUDIT");
		reservedWords.add("COALESCE");
		reservedWords.add("INDEX");
		reservedWords.add("SELECT");
		reservedWords.add("COLLATE");
		reservedWords.add("INNER");
		reservedWords.add("SEMANTICKEYPHRASETABLE");
		reservedWords.add("COLUMN");
		reservedWords.add("INSERT");
		reservedWords.add("SEMANTICSIMILARITYDETAILSTABLE");
		reservedWords.add("COMMIT");
		reservedWords.add("INTERSECT");
		reservedWords.add("SEMANTICSIMILARITYTABLE");
		reservedWords.add("COMPUTE");
		reservedWords.add("INTO");
		reservedWords.add("SESSION_USER");
		reservedWords.add("CONSTRAINT");
		reservedWords.add("IS");
		reservedWords.add("SET");
		reservedWords.add("CONTAINS");
		reservedWords.add("JOIN");
		reservedWords.add("SETUSER");
		reservedWords.add("CONTAINSTABLE");
		reservedWords.add("KEY");
		reservedWords.add("SHUTDOWN");
		reservedWords.add("CONTINUE");
		reservedWords.add("KILL");
		reservedWords.add("SOME");
		reservedWords.add("CONVERT");
		reservedWords.add("LEFT");
		reservedWords.add("STATISTICS");
		reservedWords.add("CREATE");
		reservedWords.add("LIKE");
		reservedWords.add("SYSTEM_USER");
		reservedWords.add("CROSS");
		reservedWords.add("LINENO");
		reservedWords.add("TABLE");
		reservedWords.add("CURRENT");
		reservedWords.add("LOAD");
		reservedWords.add("TABLESAMPLE");
		reservedWords.add("CURRENT_DATE");
		reservedWords.add("MERGE");
		reservedWords.add("TEXTSIZE");
		reservedWords.add("CURRENT_TIME");
		reservedWords.add("NATIONAL");
		reservedWords.add("THEN");
		reservedWords.add("CURRENT_TIMESTAMP");
		reservedWords.add("NOCHECK");
		reservedWords.add("TO");
		reservedWords.add("CURRENT_USER");
		reservedWords.add("NONCLUSTERED");
		reservedWords.add("В начало");
		reservedWords.add("CURSOR");
		reservedWords.add("NOT");
		reservedWords.add("TRAN");
		reservedWords.add("DATABASE");
		reservedWords.add("NULL");
		reservedWords.add("TRANSACTION");
		reservedWords.add("DBCC");
		reservedWords.add("NULLIF");
		reservedWords.add("TRIGGER");
		reservedWords.add("DEALLOCATE");
		reservedWords.add("OF");
		reservedWords.add("TRUNCATE");
		reservedWords.add("DECLARE");
		reservedWords.add("OFF");
		reservedWords.add("TRY_CONVERT");
		reservedWords.add("DEFAULT");
		reservedWords.add("OFFSETS");
		reservedWords.add("TSEQUAL");
		reservedWords.add("DELETE");
		reservedWords.add("ON");
		reservedWords.add("UNION");
		reservedWords.add("DENY");
		reservedWords.add("OPEN");
		reservedWords.add("UNIQUE");
		reservedWords.add("DESC");
		reservedWords.add("OPENDATASOURCE");
		reservedWords.add("UNPIVOT");
		reservedWords.add("DISK");;
		reservedWords.add("OPENQUERY");
		reservedWords.add("UPDATE");
		reservedWords.add("DISTINCT");
		reservedWords.add("OPENROWSET");
		reservedWords.add("UPDATETEXT");
		reservedWords.add("DISTRIBUTED");
		reservedWords.add("OPENXML");
		reservedWords.add("USE");
		reservedWords.add("DOUBLE");
		reservedWords.add("OPTION");
		reservedWords.add("Пользователь");
		reservedWords.add("DROP");
		reservedWords.add("OR");
		reservedWords.add("VALUES");
		reservedWords.add("DUMP");;
		reservedWords.add("OVER");
		reservedWords.add("WAITFOR");
		reservedWords.add("ERRLVL");
		reservedWords.add("PERCENT");
		reservedWords.add("PIVOT");
		reservedWords.add("PLAN");
		reservedWords.add("WHILE");
		reservedWords.add("на");
		reservedWords.add("PRINT");
		reservedWords.add("WRITETEXT");
		reservedWords.add("EXIT");
		reservedWords.add("PROC");
		reservedWords.add("OVERLAPS");
		reservedWords.add("ADA");
		reservedWords.add("ADD");
		reservedWords.add("EXTERNAL");
		reservedWords.add("PASCAL");
		reservedWords.add("ALL");
		reservedWords.add("EXTRACT");
		reservedWords.add("POSITION");
		reservedWords.add("PRECISION");
		reservedWords.add("ALTER");
		reservedWords.add("FETCH");
		reservedWords.add("AND");
		reservedWords.add("ANY");
		reservedWords.add("PRIMARY");
		reservedWords.add("FOR");
		reservedWords.add("Службы");
		reservedWords.add("Analysis");
		reservedWords.add("Services");
		reservedWords.add("FOREIGN");
		reservedWords.add("ASC");
		reservedWords.add("FORTRAN");
		reservedWords.add("PROCEDURE");
		reservedWords.add("PUBLIC");
		reservedWords.add("FROM");
		reservedWords.add("READ");
		reservedWords.add("AUTHORIZATION");
		reservedWords.add("ПОЛНОЕ");
		reservedWords.add("REAL");
		reservedWords.add("AVG");
		reservedWords.add("REFERENCES");
		reservedWords.add("BEGIN");
		reservedWords.add("BETWEEN");
		reservedWords.add("RESTRICT");
		reservedWords.add("GOTO");
		reservedWords.add("REVOKE");
		reservedWords.add("BIT_LENGTH");
		reservedWords.add("GRANT");
		reservedWords.add("RIGHT");
		reservedWords.add("GROUP");
		reservedWords.add("ROLLBACK");
		reservedWords.add("BY");
		reservedWords.add("HAVING");
		reservedWords.add("CASCADE");
		reservedWords.add("SCHEMA");
		reservedWords.add("IDENTITY");
		reservedWords.add("CASE");
		reservedWords.add("IN");
		reservedWords.add("INCLUDE");
		reservedWords.add("SELECT");
		reservedWords.add("INDEX");
		reservedWords.add("CHAR_LENGTH");
		reservedWords.add("SESSION_USER");
		reservedWords.add("SET");
		reservedWords.add("CHARACTER_LENGTH");
		reservedWords.add("INNER");
		reservedWords.add("CHECK");
		reservedWords.add("CLOSE");
		reservedWords.add("INSENSITIVE");
		reservedWords.add("SOME");
		reservedWords.add("COALESCE");
		reservedWords.add("INSERT");
		reservedWords.add("COLLATE");
		reservedWords.add("SQLCA");
		reservedWords.add("COLUMN");
		reservedWords.add("INTERSECT");
		reservedWords.add("SQLCODE");
		reservedWords.add("COMMIT");
		reservedWords.add("SQLERROR");
		reservedWords.add("INTO");
		reservedWords.add("IS");
		reservedWords.add("CONSTRAINT");
		reservedWords.add("SUBSTRING");
		reservedWords.add("JOIN");
		reservedWords.add("SUM");
		reservedWords.add("CONTINUE");
		reservedWords.add("KEY");
		reservedWords.add("SYSTEM_USER");
		reservedWords.add("CONVERT");
		reservedWords.add("TABLE");
		reservedWords.add("COUNT");
		reservedWords.add("THEN");
		reservedWords.add("CREATE");
		reservedWords.add("LEFT");
		reservedWords.add("CROSS");
		reservedWords.add("TIMESTAMP");
		reservedWords.add("CURRENT");
		reservedWords.add("LIKE");
		reservedWords.add("CURRENT_DATE");
		reservedWords.add("CURRENT_TIME");
		reservedWords.add("LOWER");
		reservedWords.add("Кому");
		reservedWords.add("CURRENT_TIMESTAMP");
		reservedWords.add("CURRENT_USER");
		reservedWords.add("MAX");
		reservedWords.add("TRANSACTION");
		reservedWords.add("CURSOR");
		reservedWords.add("MIN");
		reservedWords.add("TRANSLATE");
		reservedWords.add("TRIM");
		reservedWords.add("DEALLOCATE");
		reservedWords.add("UNION");
		reservedWords.add("NATIONAL");
		reservedWords.add("UNIQUE");
		reservedWords.add("DECLARE");
		reservedWords.add("DEFAULT");
		reservedWords.add("UPDATE");
		reservedWords.add("UPPER");
		reservedWords.add("DELETE");
		reservedWords.add("NONE");
		reservedWords.add("USER");
		reservedWords.add("DESC");
		reservedWords.add("NOT");
		reservedWords.add("NULL");
		reservedWords.add("VALUE");
		reservedWords.add("NULLIF");
		reservedWords.add("VALUES");
		reservedWords.add("OCTET_LENGTH");
		reservedWords.add("VARYING");
		reservedWords.add("DISTINCT");
		reservedWords.add("OF");
		reservedWords.add("VIEW");
		reservedWords.add("ON");
		reservedWords.add("WHEN");
		reservedWords.add("DOUBLE");
		reservedWords.add("DROP");
		reservedWords.add("OPEN");
		reservedWords.add("WHERE");
		reservedWords.add("ELSE");
		reservedWords.add("OPTION");
		reservedWords.add("WITH");
		reservedWords.add("END");
		reservedWords.add("OR");
		reservedWords.add("ORDER");
		reservedWords.add("ESCAPE");
		reservedWords.add("OUTER");
		reservedWords.add("EXCEPT");
		reservedWords.add("ABSOLUTE");
		reservedWords.add("HOST");
		reservedWords.add("RELATIVE");
		reservedWords.add("ACTION");
		reservedWords.add("HOUR");
		reservedWords.add("RELEASE");
		reservedWords.add("ADMIN");
		reservedWords.add("IGNORE");
		reservedWords.add("RESULT");
		reservedWords.add("AFTER");
		reservedWords.add("IMMEDIATE");
		reservedWords.add("RETURNS");
		reservedWords.add("AGGREGATE");
		reservedWords.add("INDICATOR");
		reservedWords.add("ROLE");
		reservedWords.add("ALIAS");
		reservedWords.add("INITIALIZE");
		reservedWords.add("ROLLUP");
		reservedWords.add("ALLOCATE");
		reservedWords.add("INITIALLY");
		reservedWords.add("ROUTINE");
		reservedWords.add("ARE");
		reservedWords.add("INOUT");
		reservedWords.add("ROW");
		reservedWords.add("ARRAY");
		reservedWords.add("INPUT");
		reservedWords.add("ROWS");
		reservedWords.add("ASENSITIVE");
		reservedWords.add("INT");
		reservedWords.add("SAVEPOINT");
		reservedWords.add("ASSERTION");
		reservedWords.add("INTEGER");
		reservedWords.add("SCROLL");
		reservedWords.add("ASYMMETRIC");
		reservedWords.add("INTERSECTION");
		reservedWords.add("SCOPE");
		reservedWords.add("AT");
		reservedWords.add("INTERVAL");
		reservedWords.add("SEARCH");
		reservedWords.add("ATOMIC");
		reservedWords.add("ISOLATION");
		reservedWords.add("SECOND");
		reservedWords.add("BEFORE");
		reservedWords.add("ITERATE");
		reservedWords.add("SECTION");
		reservedWords.add("BINARY");
		reservedWords.add("LANGUAGE");
		reservedWords.add("SENSITIVE");
		reservedWords.add("BIT");
		reservedWords.add("LARGE");
		reservedWords.add("SEQUENCE");
		reservedWords.add("BLOB");
		reservedWords.add("LAST");
		reservedWords.add("SESSION");
		reservedWords.add("BOOLEAN");
		reservedWords.add("LATERAL");
		reservedWords.add("SETS");
		reservedWords.add("BOTH");
		reservedWords.add("LEADING");
		reservedWords.add("SIMILAR");
		reservedWords.add("BREADTH");
		reservedWords.add("LESS");
		reservedWords.add("SIZE");
		reservedWords.add("CALL");
		reservedWords.add("LEVEL");
		reservedWords.add("SMALLINT");
		reservedWords.add("CALLED");
		reservedWords.add("LIKE_REGEX");
		reservedWords.add("SPACE");
		reservedWords.add("CARDINALITY");
		reservedWords.add("LIMIT");
		reservedWords.add("SPECIFIC");
		reservedWords.add("CASCADED");
		reservedWords.add("LN");
		reservedWords.add("SPECIFICTYPE");
		reservedWords.add("CAST");
		reservedWords.add("LOCAL");
		reservedWords.add("SQL");
		reservedWords.add("CATALOG");
		reservedWords.add("LOCALTIME");
		reservedWords.add("SQLEXCEPTION");
		reservedWords.add("CHAR");
		reservedWords.add("LOCALTIMESTAMP");
		reservedWords.add("SQLSTATE");
		reservedWords.add("CHARACTER");
		reservedWords.add("LOCATOR");
		reservedWords.add("SQLWARNING");
		reservedWords.add("CLASS");
		reservedWords.add("MAP");
		reservedWords.add("START");
		reservedWords.add("CLOB");
		reservedWords.add("MATCH");
		reservedWords.add("STATE");
		reservedWords.add("COLLATION");
		reservedWords.add("MEMBER");
		reservedWords.add("STATEMENT");
		reservedWords.add("COLLECT");
		reservedWords.add("METHOD");
		reservedWords.add("STATIC");
		reservedWords.add("COMPLETION");
		reservedWords.add("MINUTE");
		reservedWords.add("STDDEV_POP");
		reservedWords.add("CONDITION");
		reservedWords.add("MOD");
		reservedWords.add("STDDEV_SAMP");
		reservedWords.add("CONNECT");
		reservedWords.add("MODIFIES");
		reservedWords.add("STRUCTURE");
		reservedWords.add("CONNECTION");
		reservedWords.add("MODIFY");
		reservedWords.add("SUBMULTISET");
		reservedWords.add("CONSTRAINTS");
		reservedWords.add("MODULE");
		reservedWords.add("SUBSTRING_REGEX");
		reservedWords.add("CONSTRUCTOR");
		reservedWords.add("MONTH");
		reservedWords.add("SYMMETRIC");
		reservedWords.add("CORR");
		reservedWords.add("MULTISET");
		reservedWords.add("SYSTEM");
		reservedWords.add("CORRESPONDING");
		reservedWords.add("NAMES");
		reservedWords.add("TEMPORARY");
		reservedWords.add("COVAR_POP");
		reservedWords.add("NATURAL");
		reservedWords.add("TERMINATE");
		reservedWords.add("COVAR_SAMP");
		reservedWords.add("NCHAR");
		reservedWords.add("THAN");
		reservedWords.add("CUBE");
		reservedWords.add("NCLOB");
		reservedWords.add("TIME");
		reservedWords.add("CUME_DIST");
		reservedWords.add("NEW");
		reservedWords.add("timestamp");
		reservedWords.add("CURRENT_CATALOG");
		reservedWords.add("NEXT");
		reservedWords.add("TIMEZONE_HOUR");
		reservedWords.add("CURRENT_DEFAULT_TRANSFORM_GROUP");
		reservedWords.add("NO");
		reservedWords.add("TIMEZONE_MINUTE");
		reservedWords.add("CURRENT_PATH");
		reservedWords.add("None");
		reservedWords.add("TRAILING");
		reservedWords.add("CURRENT_ROLE");
		reservedWords.add("NORMALIZE");
		reservedWords.add("TRANSLATE_REGEX");
		reservedWords.add("CURRENT_SCHEMA");
		reservedWords.add("NUMERIC");
		reservedWords.add("TRANSLATION");
		reservedWords.add("CURRENT_TRANSFORM_GROUP_FOR_TYPE");
		reservedWords.add("OBJECT");
		reservedWords.add("TREAT");
		reservedWords.add("CYCLE");
		reservedWords.add("OCCURRENCES_REGEX");
		reservedWords.add("TRUE");
		reservedWords.add("DATA");
		reservedWords.add("OLD");
		reservedWords.add("UESCAPE");
		reservedWords.add("DATE");
		reservedWords.add("ONLY");
		reservedWords.add("UNDER");
		reservedWords.add("DAY");
		reservedWords.add("OPERATION");
		reservedWords.add("UNKNOWN");
		reservedWords.add("DEC");
		reservedWords.add("ORDINALITY");
		reservedWords.add("UNNEST");
		reservedWords.add("DECIMAL");
		reservedWords.add("OUT");
		reservedWords.add("USAGE");
		reservedWords.add("DEFERRABLE");
		reservedWords.add("OVERLAY");
		reservedWords.add("USING");
		reservedWords.add("DEFERRED");
		reservedWords.add("OUTPUT");
		reservedWords.add("Value");
		reservedWords.add("DEPTH");
		reservedWords.add("PAD");
		reservedWords.add("VAR_POP");
		reservedWords.add("DEREF");
		reservedWords.add("Параметр");
		reservedWords.add("VAR_SAMP");
		reservedWords.add("DESCRIBE");
		reservedWords.add("PARAMETERS");
		reservedWords.add("VARCHAR");
		reservedWords.add("DESCRIPTOR");
		reservedWords.add("PARTIAL");
		reservedWords.add("VARIABLE");
		reservedWords.add("DESTROY");
		reservedWords.add("PARTITION");
		reservedWords.add("WHENEVER");
		reservedWords.add("DESTRUCTOR");
		reservedWords.add("PATH");
		reservedWords.add("WIDTH_BUCKET");
		reservedWords.add("DETERMINISTIC");
		reservedWords.add("POSTFIX");
		reservedWords.add("WITHOUT");
		reservedWords.add("DICTIONARY");
		reservedWords.add("PREFIX");
		reservedWords.add("WINDOW");
		reservedWords.add("DIAGNOSTICS");
		reservedWords.add("PREORDER");
		reservedWords.add("WITHIN");
		reservedWords.add("DISCONNECT");
		reservedWords.add("PREPARE");
		reservedWords.add("WORK");
		reservedWords.add("DOMAIN");
		reservedWords.add("PERCENT_RANK");
		reservedWords.add("WRITE");
		reservedWords.add("DYNAMIC");
		reservedWords.add("PERCENTILE_CONT");
		reservedWords.add("XMLAGG");
		reservedWords.add("EACH");
		reservedWords.add("PERCENTILE_DISC");
		reservedWords.add("XMLATTRIBUTES");
		reservedWords.add("ELEMENT");
		reservedWords.add("POSITION_REGEX");
		reservedWords.add("XMLBINARY");
		reservedWords.add("END-EXEC");
		reservedWords.add("PRESERVE");
		reservedWords.add("XMLCAST");
		reservedWords.add("EQUALS");
		reservedWords.add("PRIOR");
		reservedWords.add("XMLCOMMENT");
		reservedWords.add("EVERY");
		reservedWords.add("PRIVILEGES");
		reservedWords.add("XMLCONCAT");
		reservedWords.add("EXCEPTION");
		reservedWords.add("RANGE");
		reservedWords.add("XMLDOCUMENT");
		reservedWords.add("FALSE");
		reservedWords.add("READS");
		reservedWords.add("XMLELEMENT");
		reservedWords.add("FILTER");
		reservedWords.add("real");
		reservedWords.add("XMLEXISTS");
		reservedWords.add("FIRST");
		reservedWords.add("RECURSIVE");
		reservedWords.add("XMLFOREST");
		reservedWords.add("FLOAT");
		reservedWords.add("REF");
		reservedWords.add("XMLITERATE");
		reservedWords.add("FOUND");
		reservedWords.add("REFERENCING");
		reservedWords.add("XMLNAMESPACES");
		reservedWords.add("FREE");
		reservedWords.add("REGR_AVGX");
		reservedWords.add("XMLPARSE");
		reservedWords.add("FULLTEXTTABLE");
		reservedWords.add("REGR_AVGY");
		reservedWords.add("XMLPI");
		reservedWords.add("FUSION");
		reservedWords.add("REGR_COUNT");
		reservedWords.add("XMLQUERY");
		reservedWords.add("GENERAL");
		reservedWords.add("REGR_INTERCEPT");
		reservedWords.add("XMLSERIALIZE");
		reservedWords.add("GET");
		reservedWords.add("REGR_R2");
		reservedWords.add("XMLTABLE");
		reservedWords.add("GLOBAL");
		reservedWords.add("REGR_SLOPE");
		reservedWords.add("XMLTEXT");
		reservedWords.add("GO");
		reservedWords.add("REGR_SXX");
		reservedWords.add("XMLVALIDATE");
		reservedWords.add("GROUPING");
		reservedWords.add("REGR_SXY");
		reservedWords.add("YEAR");
		reservedWords.add("HOLD");
		reservedWords.add("REGR_SYY");
		reservedWords.add("ZONE");


		return reservedWords.contains(word.toUpperCase());
	}
}
