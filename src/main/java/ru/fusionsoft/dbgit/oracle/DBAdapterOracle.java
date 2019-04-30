package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapterRestoreMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
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
import ru.fusionsoft.dbgit.dbobjects.DBTableRow;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.dbobjects.DBUser;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.oracle.FactoryDBAdapterRestoreOracle;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;


public class DBAdapterOracle extends DBAdapter {
	public static final String DEFAULT_MAPPING_TYPE = "VARCHAR2";
	
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestoreOracle restoreFactory = new FactoryDBAdapterRestoreOracle();

	private String s;

	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(DEFAULT_MAPPING_TYPE, StringData.class);
		FactoryCellData.regMappingTypes("VARCHAR2", StringData.class);
		FactoryCellData.regMappingTypes("NUMBER", LongData.class);
		FactoryCellData.regMappingTypes("BLOB", MapFileData.class);
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
			throw new ExceptionDBGitRunTime(e.getMessage());
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
					"SELECT S.*, (SELECT dbms_metadata.get_ddl('SEQUENCE', S.SEQUENCE_NAME) from dual) AS DDL\n" + 
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
			logger.error(e.getMessage(), e);
			throw new ExceptionDBGitRunTime(e.getMessage(), e);
		}
		return listSequence;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {
		try {
			Connection connect = getConnection();
			String query = 
					"SELECT S.*, (SELECT dbms_metadata.get_ddl('SEQUENCE', S.SEQUENCE_NAME) from dual) AS DDL\n" + 
					"FROM DBA_SEQUENCES S WHERE S.SEQUENCE_OWNER = '" + schema + "' AND S.SEQUENCE_NAME = '" + name + "'";
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
						
			rs.next();
			String nameSeq = rs.getString("SEQUENCE_NAME");
			DBSequence sequence = new DBSequence();
			sequence.setName(nameSeq);
			sequence.setSchema(schema);
			sequence.setValue(0L);
			rowToProperties(rs, sequence.getOptions());
				
			stmt.close();
			return sequence;
		}catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new ExceptionDBGitRunTime(e.getMessage(), e);
		}
	}

	@Override
	public Map<String, DBTable> getTables(String schema) {
		Map<String, DBTable> listTable = new HashMap<String, DBTable>();
		try {
			String query = "SELECT T.*, (SELECT dbms_metadata.get_ddl('TABLE', T.TABLE_NAME) from dual) AS DDL\n" + 
					"FROM DBA_TABLES T WHERE OWNER = '" + schema + "'";
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
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
		return listTable;
	}

	@Override
	public DBTable getTable(String schema, String name) {
		try {
			String query = "SELECT T.*, (SELECT dbms_metadata.get_ddl('TABLE', T.TABLE_NAME) from dual) AS DDL\n" + 
							"FROM DBA_TABLES T WHERE T.OWNER = '" + schema + "' AND T.TABLE_NAME = '" + name + "'";
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
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		try {
			Map<String, DBTableField> listField = new HashMap<String, DBTableField>();
			
			String query1 = 
					"SELECT column_name FROM all_constraints cons, all_cons_columns cols\n"+
					"WHERE cols.table_name = '" + nameTable + "'\n"+
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
					"SELECT ROWNUM AS NUM, TC.* FROM DBA_TAB_COLS TC \n" + 
					"WHERE table_name = '" + nameTable + "' AND OWNER = '" + schema + "'";
			
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){				
				DBTableField field = new DBTableField();
				field.setName(rs.getString("COLUMN_NAME").toLowerCase());  
				if (rs.getString("COLUMN_NAME").toLowerCase().equals(s)) { 
					field.setIsPrimaryKey(true);
				}
				field.setTypeSQL(getFieldType(rs));
				field.setTypeMapping(getTypeMapping(rs));
				listField.put(field.getName(), field);
			}
			
			stmt.close();
			
			return listField;
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}		
	}

	protected String getTypeMapping(ResultSet rs) throws SQLException {
		String tp = rs.getString("DATA_TYPE");
		if (FactoryCellData.contains(tp) ) 
			return tp;
		
		return DEFAULT_MAPPING_TYPE;
	}
	
	protected String getFieldType(ResultSet rs) {
		try {
			StringBuilder type = new StringBuilder(); 
			type.append(rs.getString("DATA_TYPE"));
			
			Integer max_length = rs.getInt("CHAR_LENGTH");
			if (!rs.wasNull()) {
				type.append("("+max_length.toString()+")");
			}
			if (rs.getString("NULLABLE").equals("N")){
				type.append(" NOT NULL");
			}
			
			
			return type.toString();
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}	
	}
	
	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<>();
		try {
			String query = "SELECT  ind.*, (select dbms_metadata.get_ddl('INDEX', ind.INDEX_NAME) AS DDL from dual) AS DDL\n" + 
					"FROM all_indexes ind\n" + 
					"WHERE table_name = '" + nameTable + "' AND owner = '" + schema + "'";
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()){
				DBIndex index = new DBIndex();
				index.setName(rs.getString("INDEX_NAME"));
				index.setSchema(schema);
				index.setSql(rs.getString("DDL"));		
				rowToProperties(rs, index.getOptions());
				indexes.put(index.getName(), index);
			}
			stmt.close();
			
			return indexes;
			
		}catch(Exception e) {
			logger.error("Error load Indexes");
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		Map<String, DBConstraint> constraints = new HashMap<>();
		try {
			String query = "SELECT cons.*, (select dbms_metadata.get_ddl('CONSTRAINT', cons.constraint_name) AS DDL from dual) AS DDL\n" + 
					"FROM all_constraints cons\n" + 
					"WHERE owner = '" + schema + "' and table_name = '" + nameTable + "' and constraint_name not like 'SYS%' and cons.constraint_type = 'P'";

			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while(rs.next()){
				DBConstraint con = new DBConstraint();
				con.setName(rs.getString("CONSTRAINT_NAME"));
				//This is DDL?
				con.setConstraintDef(rs.getString("DDL"));
				con.setConstraintType(rs.getString("CONSTRAINT_TYPE"));
				con.setSchema(schema);
				constraints.put(con.getName(), con);
			}
			stmt.close();
			
			return constraints;		
			
		}catch(Exception e) {
			logger.error("Error load Constraints");
			throw new ExceptionDBGitRunTime("Error", e);
		}
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		Map<String, DBView> listView = new HashMap<String, DBView>();
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('VIEW', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'VIEW'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBView view = new DBView(rs.getString("OBJECT_NAME"));
				view.setSql(rs.getString("DDL"));
				view.setSchema(rs.getString("OWNER"));
				view.setOwner(rs.getString("OWNER"));
				listView.put(rs.getString("OBJECT_NAME"), view);
			}
			stmt.close();
			return listView;
		}catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}

	@Override
	public DBView getView(String schema, String name) {
		DBView view = new DBView(name);
		view.setSchema(schema);
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('VIEW', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'VIEW' and f.object_name = '" + name + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			view.setOwner(schema);
			view.setSql("");
			
			while (rs.next()) {
				view.setOwner(rs.getString("OWNER"));
				view.setSql(rs.getString("DDL"));
			}
			stmt.close();
			return view;
			
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}
	
	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		try {
			String query = "SELECT  tr.owner, tr.trigger_name, tr.trigger_type, tr.table_name," + 
					" (select dbms_metadata.get_ddl('TRIGGER', tr.trigger_name) AS DDL from dual) AS DDL\n" + 
					"FROM all_triggers tr\n" + 
					"WHERE owner = '"+ schema +"'";
			
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("TRIGGER_NAME");
				String sql = rs.getString("DDL");
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSql(sql);
				trigger.setSchema(schema);
				//what means owner? oracle/postgres or owner like database user/schema
				trigger.setOwner("oracle");
				rowToProperties(rs, trigger.getOptions());
				listTrigger.put(name, trigger);
			}
			stmt.close();
			return listTrigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error ", e);	
		}
	}
	
	public DBTrigger getTrigger(String schema, String name) {
		DBTrigger trigger = null;
		try {
			String query = "SELECT  tr.owner, tr.trigger_name, tr.trigger_type, tr.table_name, (select dbms_metadata.get_ddl('TRIGGER', tr.trigger_name) AS DDL from dual) AS DDL\n" + 
					"FROM    all_triggers tr\n" + 
					"WHERE   owner = '" + schema + "' and trigger_name = '" + name + "'";
			
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()){
				String sql = rs.getString("DDL");
				trigger = new DBTrigger(name);
				trigger.setSql(sql);			
				trigger.setSchema(schema);
				//what means owner? oracle/postgres or owner like database user/schema
				trigger.setOwner("oracle");
				rowToProperties(rs, trigger.getOptions());
			}
			stmt.close();
			return trigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error ", e);	
		}
	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		Map<String, DBPackage> listPackage = new HashMap<String, DBPackage>();
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PACKAGE', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PACKAGE'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("OBJECT_NAME");
				String sql = rs.getString("DDL");
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				DBPackage pack = new DBPackage(name);
				pack.setSql(sql);
				pack.setSchema(schema);
				pack.setOwner(owner);
				rowToProperties(rs,pack.getOptions());
				//pack.setArguments(args);
				listPackage.put(name, pack);
			}
			stmt.close();
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error load functions from " +schema, e);
		}
		return listPackage;
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PACKAGE', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PACKAGE' and f.object_name = '" + name + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			rs.next();
			DBPackage pack = new DBPackage(name);
			String owner = rs.getString("OWNER");
			//String args = rs.getString("arguments");
			pack.setSchema(schema);
			pack.setSql(rs.getString("DDL"));
			pack.setOwner(owner);
			//pack.setArguments(args);
			rowToProperties(rs,pack.getOptions());
			stmt.close();
			
			return pack;
			
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error load function " +schema+"."+name, e);			
		}
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		Map<String, DBProcedure> listProcedure = new HashMap<String, DBProcedure>();
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PROCEDURE', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PROCEDURE'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("OBJECT_NAME");
				String sql = rs.getString("DDL");
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				DBProcedure proc = new DBProcedure();
				proc.setSql(sql);
				proc.setSchema(schema);
				proc.setOwner(owner);
				proc.setName(name);
				rowToProperties(rs,proc.getOptions());
				//proc.setArguments(args);
				listProcedure.put(name, proc);
			}
			stmt.close();
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error load functions from " +schema, e);
		}
		return listProcedure;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('PROCEDURE', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'PROCEDURE' and f.object_name = '" + name + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			rs.next();
			DBProcedure proc = new DBProcedure();
			String owner = rs.getString("OWNER");
			//String args = rs.getString("arguments");
			proc.setSchema(schema);
			proc.setSql(rs.getString("DDL"));
			proc.setOwner(owner);
			//proc.setArguments(args);
			rowToProperties(rs,proc.getOptions());
			stmt.close();
			
			return proc;
			
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error load function " +schema+"."+name, e);			
		}
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('FUNCTION', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema + "' and f.object_type = 'FUNCTION'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("OBJECT_NAME");
				String sql = rs.getString("DDL");
				String owner = rs.getString("OWNER");
				//String args = rs.getString("arguments");
				DBFunction func = new DBFunction(name);
				func.setSql(sql);
				func.setSchema(schema);
				func.setOwner(owner);
				rowToProperties(rs,func.getOptions());
				//func.setArguments(args);
				listFunction.put(name, func);
			}
			stmt.close();
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error load functions from " +schema, e);
		}
		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		try {
			String query = "SELECT f.owner, f.object_name, (select dbms_metadata.get_ddl('FUNCTION', f.object_name) AS DDL from dual) AS DDL \n" + 
					"FROM all_objects f WHERE f.owner = '" + schema +"' and " + 
					"f.object_type = 'FUNCTION' and f.object_name = '" + name +"'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			rs.next();
			DBFunction func = new DBFunction(rs.getString("OBJECT_NAME"));
			String owner = rs.getString("OWNER");
			//String args = rs.getString("arguments");
			func.setSchema(schema);
			func.setSql(rs.getString("DDL"));
			func.setOwner(owner);
			//func.setArguments(args);
			rowToProperties(rs,func.getOptions());
			stmt.close();
			
			return func;
			
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error load function " +schema+"."+name, e);			
		}
	}

	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		String tableName = schema + "." + nameTable;
		try {
			DBTableData data = new DBTableData();
			
			int maxRowsCount = DBGitConfig.getInstance().getInteger("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH);
			
			if (DBGitConfig.getInstance().getBoolean("core", "LIMIT_FETCH", true)) {
				Statement st = getConnection().createStatement();
				String query = "select COALESCE(count(*), 0) row_count from ( select 1 from "+
						tableName+" where ROWNUM <= " + (maxRowsCount + 1) + " ) tbl";
				ResultSet rs = st.executeQuery(query);
				rs.next();
				if (rs.getInt("row_count") > maxRowsCount) {
					data.setErrorFlag(DBTableData.ERROR_LIMIT_ROWS);
					return data;
				}
				
				rs = st.executeQuery("select * from " + tableName);
				data.setResultSet(rs);
				return data;
			}
			
			//TODO other state
			
			return data;
		} catch(Exception e) {
			logger.error("Error load data from " + tableName, e);
			try {
				getConnection().rollback(); 
			} catch (Exception e2) {
				logger.error("Error rollback  ", e2);
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
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
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
				//String name = rs.getString("GRANTEE") + "_" + rs.getString("GRANTED_ROLE");
				String name = rs.getString("GRANTED_ROLE");
				DBRole role = new DBRole(name);
				rowToProperties(rs, role.getOptions());
				listRole.put(name, role);
			}
			stmt.close();
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
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

}
