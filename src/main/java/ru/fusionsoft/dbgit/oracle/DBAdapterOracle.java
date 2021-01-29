package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Set;

import org.apache.commons.lang3.exception.ExceptionUtils;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
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
import ru.fusionsoft.dbgit.oracle.FactoryDBAdapterRestoreOracle;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;


public class DBAdapterOracle extends DBAdapter {
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestoreOracle restoreFactory = new FactoryDBAdapterRestoreOracle();	
	private FactoryDbConvertAdapterOracle convertFactory = new FactoryDbConvertAdapterOracle();
	private FactoryDBBackupAdapterOracle backupFactory = new FactoryDBBackupAdapterOracle();

	private String s;

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
			logger.error(lang.getValue("errors", "adapter", "schemes").toString(), e);
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "schemes").toString(), e);
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
					"AND OWNER != 'DIP' AND OWNER != 'DBSNMP' AND OWNER != 'ORACLE_OCM' and segment_name not like 'SYS%'";
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
			throw new ExceptionDBGitRunTime(e);
		}
		return listTableSpace;
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
		Map<String, DBSequence> listSequence = new HashMap<String, DBSequence>();
		try {
			Connection connect = getConnection();
			//variant 1 from DBA_OBJECTS
			/*String query = 
					"SELECT ROWNUM AS NUM, OWNER, OBJECT_NAME, SUBOBJECT_NAME, OBJECT_TYPE, STATUS,\n" + 
					"(select dbms_metadata.get_ddl('SEQUENCE', O.OBJECT_NAME) AS DDL from dual) AS DDL\n" + 
					"FROM DBA_OBJECTS O WHERE OBJECT_TYPE = 'SEQUENCE' AND OWNER = :schema";*/
			
			//variant 2 from DBA_SEQUENCES
			String query = 
					"SELECT S.SEQUENCE_NAME, (SELECT dbms_metadata.get_ddl('SEQUENCE', S.SEQUENCE_NAME, S.SEQUENCE_OWNER) from dual) AS DDL,\n" + 
					"order_flag, increment_by, last_number, min_value, max_value, cache_size \n" +
					"FROM DBA_SEQUENCES S WHERE S.SEQUENCE_OWNER = '" + schema + "'";
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
						
			while(rs.next()){
				String nameSeq = rs.getString("SEQUENCE_NAME");
				DBSequence sequence = new DBSequence();
				sequence.setName(nameSeq);
				sequence.setSchema(schema);
				sequence.setValue(0L);
				rowToProperties(rs, sequence.getOptions());
				listSequence.put(nameSeq, sequence);
			}
			stmt.close();
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "sequences").toString(), e);
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "sequences").toString(), e);
		}
		return listSequence;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {
		try {
			Connection connect = getConnection();
			String query = 
					"SELECT S.SEQUENCE_NAME, (SELECT dbms_metadata.get_ddl('SEQUENCE', S.SEQUENCE_NAME, S.SEQUENCE_OWNER) from dual) AS DDL, \n" +
					"order_flag, increment_by, last_number, min_value, max_value, cache_size \n" +
					"FROM DBA_SEQUENCES S WHERE S.SEQUENCE_OWNER = '" + schema + "' AND S.SEQUENCE_NAME = '" + name + "'";
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
					
			DBSequence sequence = null;
			while (rs.next()) {
				String nameSeq = rs.getString("SEQUENCE_NAME");
				sequence = new DBSequence();
				sequence.setName(nameSeq);
				sequence.setSchema(schema);
				sequence.setValue(0L);
				rowToProperties(rs, sequence.getOptions());
			}
			stmt.close();
			return sequence;
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "sequences").toString(), e);
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "sequences").toString(), e);
		}
	}

	@Override
	public Map<String, DBTable> getTables(String schema) {
		Map<String, DBTable> listTable = new HashMap<String, DBTable>();
		try {
			String query = "SELECT T.TABLE_NAME, T.OWNER, (SELECT dbms_metadata.get_ddl('TABLE', T.TABLE_NAME, T.OWNER) from dual) AS DDL\n" + 
					"FROM DBA_TABLES T WHERE upper(OWNER) = upper('" + schema + "') and nested = 'NO' and (iot_type <> 'IOT_OVERFLOW' or iot_type is null)";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while(rs.next()){
				String nameTable = rs.getString("TABLE_NAME");
				DBTable table = new DBTable(nameTable);
				table.setSchema(schema);
				rowToProperties(rs, table.getOptions());
				listTable.put(nameTable, table);
			}
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
			String query = "SELECT T.TABLE_NAME, T.OWNER, (SELECT dbms_metadata.get_ddl('TABLE', T.TABLE_NAME, T.OWNER) from dual) AS DDL\n" + 
							"FROM DBA_TABLES T WHERE upper(T.OWNER) = upper('" + schema + "') AND upper(T.TABLE_NAME) = upper('" + name + "')";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			DBTable table = null;
			
			while(rs.next()) {
				String nameTable = rs.getString("TABLE_NAME");
				table = new DBTable(nameTable);
				table.setSchema(schema);
				rowToProperties(rs, table.getOptions());
			}
			
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
			
			String query1 = 
					"SELECT column_name FROM all_constraints cons, all_cons_columns cols\n"+
					"WHERE upper(cols.table_name) = upper('" + nameTable + "')\n"+
					"AND cons.constraint_type = 'P'\n" + 
					"AND cons.constraint_name = cols.constraint_name\n" +
					"AND cons.owner = cols.owner";			
			Connection connect = getConnection();			
			
			Statement stmt = connect.createStatement();
			ResultSet rs1 = stmt.executeQuery(query1);
			
			String s = "";
			
			while (rs1.next())
				s = rs1.getString("COLUMN_NAME").toLowerCase();
			
			String query = 
					"SELECT case \r\n" + 
					"    when lower(data_type) in ('number', 'numeric', 'dec', 'decimal', 'pls_integer') then 'number'\r\n" + 
					"    when lower(data_type) in ('varchar2', 'varchar', 'char', 'nchar', 'nvarchar2') then 'string'\r\n" + 
					"    when substr(lower(data_type), 1, instr(data_type, '(') - 1) in ('date', 'timestamp') then 'date'\r\n" +
					"    when lower(data_type) in ('date', 'timestamp') then 'date'\r\n" +
					"    when lower(data_type) in ('clob') then 'text'\r\n" +
					"    when lower(data_type) in ('blob') then 'binary'" +
					"    else 'native'\r\n" + 
					"    end type, " +
					"    case when lower(data_type) in ('char', 'nchar') then 1 else 0 end fixed, " +
					"    ROWNUM AS NUM, TC.* FROM DBA_TAB_COLS TC \n" + 
					"WHERE lower(table_name) = lower('" + nameTable + "') AND lower(OWNER) = lower('" + schema + "') ORDER BY column_id";
			
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){				
				DBTableField field = new DBTableField();
				field.setName(rs.getString("COLUMN_NAME").toLowerCase());  
				if (rs.getString("COLUMN_NAME").toLowerCase().equals(s)) { 
					field.setIsPrimaryKey(true);
				}
				String typeSQL = getFieldType(rs);
				field.setTypeSQL(typeSQL);
				field.setIsNullable( !typeSQL.toLowerCase().contains("not null"));
				field.setTypeUniversal(FieldType.fromString(rs.getString("TYPE").toUpperCase()));
				field.setLength(rs.getInt("DATA_LENGTH"));
				field.setScale(rs.getInt("DATA_SCALE"));
				field.setPrecision(rs.getInt("DATA_PRECISION"));
				field.setFixed(rs.getBoolean("fixed"));
				field.setOrder(rs.getInt("column_id"));
				listField.put(field.getName(), field);
			}
			
			stmt.close();
			
			return listField;
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}		
	}

	protected String getFieldType(ResultSet rs) {
		try {
			StringBuilder type = new StringBuilder(); 
			type.append(rs.getString("DATA_TYPE"));
			
			Integer max_length = rs.getInt("CHAR_LENGTH");
			if (!rs.wasNull() && !rs.getString("DATA_TYPE").contains("(")) {
				type.append("("+max_length.toString()+")");
			}
			
			if (rs.getString("NULLABLE").equals("N")){
				type.append(" NOT NULL");
			}			
			
			return type.toString();
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}	
	}
	
	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<>();
		try {
			String query = "SELECT  ind.index_name, (select dbms_metadata.get_ddl('INDEX', ind.INDEX_NAME, owner) AS DDL from dual) AS DDL\n" + 
					"FROM all_indexes ind\n" + 
					"WHERE upper(table_name) = upper('" + nameTable + "') AND upper(owner) = upper('" + schema + "')";
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()){
				DBIndex index = new DBIndex();
				index.setName(rs.getString("INDEX_NAME"));
				index.setSchema(schema);	
				rowToProperties(rs, index.getOptions());
				indexes.put(index.getName(), index);
			}
			stmt.close();
			
			return indexes;
			
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "indexes").toString());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "indexes").toString(), e);
		}
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		Map<String, DBConstraint> constraints = new HashMap<>();
		try {
			String query = "SELECT cons.constraint_type, cons.CONSTRAINT_NAME, (select dbms_metadata.get_ddl('CONSTRAINT', cons.constraint_name, owner) AS DDL from dual) AS DDL\n" + 
					"FROM all_constraints cons\n" + 
					"WHERE upper(owner) = upper('" + schema + "') and upper(table_name) = upper('" + nameTable + "') and constraint_name not like 'SYS%' and cons.constraint_type = 'P'";

			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while(rs.next()){
				DBConstraint con = new DBConstraint();
				con.setName(rs.getString("CONSTRAINT_NAME"));
				//This is DDL?
				con.setConstraintType(rs.getString("CONSTRAINT_TYPE"));
				con.setSchema(schema);
				rowToProperties(rs, con.getOptions());
				constraints.put(con.getName(), con);
			}
			stmt.close();
			
			return constraints;		
			
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "constraints").toString());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "constraints").toString(), e);
		}
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		Map<String, DBView> listView = new HashMap<String, DBView>();
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('VIEW', f.object_name, f.owner) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'VIEW'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBView view = new DBView(rs.getString("OBJECT_NAME"));
				view.setSchema(rs.getString("OWNER"));
				view.setOwner(rs.getString("OWNER"));
				rowToProperties(rs, view.getOptions());
				listView.put(rs.getString("OBJECT_NAME"), view);
			}
			stmt.close();
			return listView;
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views") + ": " + e.getMessage());
		}
	}

	@Override
	public DBView getView(String schema, String name) {
		DBView view = new DBView(name);
		view.setSchema(schema);
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('VIEW', f.object_name, f.owner) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'VIEW' and f.object_name = '" + name + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			view.setOwner(schema);
			
			while (rs.next()) {
				view.setOwner(rs.getString("OWNER"));
				rowToProperties(rs, view.getOptions());
			}
			stmt.close();
			return view;
			
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "views").toString() + ": "+ e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views").toString(), e);
		}
	}
	
	public Map<String, DBTrigger> getTriggers(String schema) {

		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		String query =
			"SELECT  tr.owner, tr.trigger_name, tr.trigger_type, tr.table_name," +
			" (select dbms_metadata.get_ddl('TRIGGER', tr.trigger_name, tr.owner) AS DDL from dual) AS DDL\n" +
			"FROM all_triggers tr\n" +
			"WHERE owner = '"+ schema +"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				String name = rs.getString("TRIGGER_NAME");
				String sql = rs.getString("DDL");
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSchema(schema);
				//what means owner? oracle/postgres or owner like database user/schema
				trigger.setOwner("oracle");
				rowToProperties(rs, trigger.getOptions());
				listTrigger.put(name, trigger);
			}


		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);
		}

		return listTrigger;
	}
	
	public DBTrigger getTrigger(String schema, String name) {
		DBTrigger trigger = null;

		String query =
			"SELECT  tr.owner, tr.trigger_name, tr.trigger_type, tr.table_name, (select dbms_metadata.get_ddl('TRIGGER', tr.trigger_name, tr.owner) AS DDL from dual) AS DDL\n" +
			"FROM    all_triggers tr\n" +
			"WHERE   owner = '" + schema + "' and trigger_name = '" + name + "'";

		try (Statement stmt = connect.createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				trigger = new DBTrigger(name);	
				trigger.setSchema(schema);
				//what means owner? oracle/postgres or owner like database user/schema
				trigger.setOwner("oracle");
				rowToProperties(rs, trigger.getOptions());
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);
		}

		return trigger;
	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		Map<String, DBPackage> listPackage = new HashMap<String, DBPackage>();

		String query =
			"SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PACKAGE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PACKAGE'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query); ){

			while(rs.next()){
				String name = rs.getString("OBJECT_NAME");
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				DBPackage pack = new DBPackage(name);
				pack.setSchema(schema);
				pack.setOwner(owner);
				rowToProperties(rs,pack.getOptions());
				//pack.setArguments(args);
				listPackage.put(name, pack);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "pkg").toString(), e);
		}

		return listPackage;
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		DBPackage pack = null;

		String query =
			"SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PACKAGE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PACKAGE' and f.object_name = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while (rs.next()) {
				pack = new DBPackage(name);
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				pack.setSchema(schema);
				pack.setOwner(owner);
				//pack.setArguments(args);
				rowToProperties(rs,pack.getOptions());
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views").toString(), e);
		}

		return pack;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		Map<String, DBProcedure> listProcedure = new HashMap<String, DBProcedure>();

		String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS " +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('PROCEDURE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PROCEDURE'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				String name = rs.getString("OBJECT_NAME");
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				DBProcedure proc = new DBProcedure(name);
				proc.setSchema(schema);
				proc.setOwner(owner);
				proc.setName(name);
				rowToProperties(rs,proc.getOptions());
				//proc.setArguments(args);
				listProcedure.put(name, proc);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "prc").toString(), e);
		}

		return listProcedure;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		DBProcedure proc = null;

		String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS " +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('PROCEDURE', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PROCEDURE' and f.object_name = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while (rs.next()) {
				proc = new DBProcedure(rs.getString("OBJECT_NAME"));
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				proc.setSchema(schema);
				proc.setOwner(owner);
				//proc.setArguments(args);
				rowToProperties(rs,proc.getOptions());
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "prc").toString(), e);
		}

		return proc;
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS \r\n" +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('FUNCTION', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'FUNCTION'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
			while(rs.next()){
				String name = rs.getString("OBJECT_NAME");
				String sql = rs.getString("DDL");
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				DBFunction func = new DBFunction(name);
				func.setSchema(schema);
				func.setOwner(owner);
				rowToProperties(rs,func.getOptions());
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
		DBFunction func = null;

		String query =
			"SELECT f.owner, f.object_name, (select listagg(DATA_TYPE, ' ' ) within group (order by DATA_TYPE) from ALL_ARGUMENTS " +
			"WHERE object_name = f.OBJECT_NAME AND owner = f.owner) arguments, (select dbms_metadata.get_ddl('FUNCTION', f.object_name, f.owner) AS DDL from dual) AS DDL \n" +
			"FROM all_objects f WHERE f.owner = '" + schema +"' and " +
			"f.object_type = 'FUNCTION' and f.object_name = '" + name +"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while (rs.next()) {
				func = new DBFunction(rs.getString("OBJECT_NAME"));
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				func.setSchema(schema);
				func.setOwner(owner);
				//func.setArguments(args);
				rowToProperties(rs,func.getOptions());
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
		}

		return func;
	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		DBTableData data = new DBTableData();
				
		try {
			int portionSize = DBGitConfig.getInstance().getInteger("core", "PORTION_SIZE", DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000));
			
			int begin = 1 + portionSize*portionIndex;
			int end = portionSize + portionSize*portionIndex;

			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery("    SELECT * FROM \r\n" + 
					"   (SELECT f.*, ROW_NUMBER() OVER (ORDER BY rowid) DBGIT_ROW_NUM FROM " + schema + "." + nameTable + " f)\r\n" + 
					"   WHERE DBGIT_ROW_NUM BETWEEN " + begin  + " and " + end);
			data.setResultSet(rs);	
			return data;
		} catch(Exception e) {

			ConsoleWriter.println(e.getLocalizedMessage(), messageLevel);
			ConsoleWriter.detailsPrintln(ExceptionUtils.getStackTrace(e), messageLevel);
			logger.error(lang.getValue("errors", "adapter", "tableData").toString(), e);

			try {
				if (tryNumber <= DBGitConfig.getInstance().getInteger("core", "TRY_COUNT", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000))) {
					try {
						TimeUnit.SECONDS.sleep(DBGitConfig.getInstance().getInteger("core", "TRY_DELAY", DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000)));
					} catch (InterruptedException e1) {
						throw new ExceptionDBGitRunTime(e1.getMessage());
					}
					ConsoleWriter.println(DBGitLang.getInstance()
					    .getValue("errors", "dataTable", "loadPortionError")
					    .withParams(String.valueOf(tryNumber))
					    , messageLevel
					);
					return getTableDataPortion(schema, nameTable, portionIndex, tryNumber++);
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
	
	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		String tableName = schema + "." + nameTable;
		try {
			DBTableData data = new DBTableData();
			
			int maxRowsCount = DBGitConfig.getInstance().getInteger("core", "MAX_ROW_COUNT_FETCH", DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH));
			
			if (DBGitConfig.getInstance().getBoolean("core", "LIMIT_FETCH", DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true))) {
				Statement st = getConnection().createStatement();
				String query = "select COALESCE(count(*), 0) row_count from ( select 1 from "+
						tableName+" where ROWNUM <= " + (maxRowsCount + 1) + " ) tbl";
				ResultSet rs = st.executeQuery(query);
				rs.next();
				if (rs.getInt("row_count") > maxRowsCount) {
					data.setErrorFlag(DBTableData.ERROR_LIMIT_ROWS);
					return data;
				}
			}	
			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery("select * from "+tableName);
			data.setResultSet(rs);	
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
			logger.error(lang.getValue("errors", "adapter", "users") + ": " +e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "users") + ": " + e.getMessage());
		}
		return listUser;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		Map<String, DBRole> listRole = new HashMap<String, DBRole>();
		try {
			String query = "SELECT R.GRANTEE, \n" + 
					"R.GRANTED_ROLE, R.ADMIN_OPTION, R.DEFAULT_ROLE FROM DBA_ROLE_PRIVS R \n" + 
					"WHERE R.GRANTEE = (SELECT USERNAME FROM DBA_USERS WHERE USERNAME = R.GRANTEE AND\n" + 
					"USERNAME != 'PUBLIC' AND USERNAME != 'SYSTEM'\n" + 
					"AND USERNAME != 'SYS' AND USERNAME != 'APPQOSSYS' AND USERNAME != 'OUTLN' \n" + 
					"AND USERNAME != 'DIP' AND USERNAME != 'DBSNMP' AND USERNAME != 'ORACLE_OCM')";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("GRANTED_ROLE");
				DBRole role = new DBRole(name);
				rowToProperties(rs, role.getOptions());
				listRole.put(name, role);
			}
			stmt.close();
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "roles") + ": " + e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "roles") + ": " + e.getMessage());
		}
		return listRole;
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
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		return backupFactory;
	}

	@Override
	public DbType getDbType() {
		return DbType.ORACLE;
	}
	
	@Override
	public String getDbVersion() {
		try {
			PreparedStatement stmt = getConnection().prepareStatement("SELECT version FROM V$INSTANCE");
			ResultSet resultSet = stmt.executeQuery();			
			resultSet.next();
			
			String result = resultSet.getString("version");
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
			Statement st = 	connect.createStatement();
			ResultSet rs = st.executeQuery("select count(*) cnt from all_users where USERNAME = '" + schemaName.toUpperCase() + "'");
			rs.next();
			if (rs.getInt("cnt") == 0) {
				StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());
				stLog.execute("create USER \"" + schemaName.toUpperCase() + "\"\r\n" + 
						"IDENTIFIED BY \"" + schemaName.toUpperCase() + "\"\r\n" + 
						"DEFAULT TABLESPACE \"SYSTEM\"\r\n" + 
						"TEMPORARY TABLESPACE \"TEMP\"\r\n" + 
						"ACCOUNT UNLOCK");
				
				stLog.execute("ALTER USER \"" + schemaName.toUpperCase() + "\" QUOTA UNLIMITED ON SYSTEM");
				stLog.close();
			}
			
			rs.close();
			st.close();
		} catch (SQLException e) {
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "createSchema") + ": " + e.getLocalizedMessage());
		}
		
	}

	@Override
	public boolean isReservedWord(String word) {
		Set<String> reservedWords = new HashSet<>();

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
		
		return reservedWords.contains(word.toUpperCase());
	}
	
	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return convertFactory;
	}


	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		
	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		try {
			return getConnection().getSchema();
		} catch (SQLException e) {
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "getSchema") + ": " + e.getLocalizedMessage());
		}
	}	
}
