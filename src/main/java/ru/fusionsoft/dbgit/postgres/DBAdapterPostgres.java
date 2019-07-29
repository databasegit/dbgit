package ru.fusionsoft.dbgit.postgres;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitObjectNotFound;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.data_table.MapFileData;
import ru.fusionsoft.dbgit.data_table.BooleanData;
import ru.fusionsoft.dbgit.data_table.DateData;
import ru.fusionsoft.dbgit.data_table.FactoryCellData;
import ru.fusionsoft.dbgit.data_table.LongData;
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
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;


public class DBAdapterPostgres extends DBAdapter {
	public static final String DEFAULT_MAPPING_TYPE = "string";

	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestorePostgres restoreFactory = new FactoryDBAdapterRestorePostgres();

	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(DEFAULT_MAPPING_TYPE, StringData.class);		
		FactoryCellData.regMappingTypes("string", StringData.class);
		FactoryCellData.regMappingTypes("number", LongData.class);
		FactoryCellData.regMappingTypes("binary", MapFileData.class);
		FactoryCellData.regMappingTypes("text", TextFileData.class);
		FactoryCellData.regMappingTypes("date", DateData.class);
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
		return null;
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		Map<String, DBSchema> listScheme = new HashMap<String, DBSchema>();
		try {
			String query = "select nspname,usename,nspacl from pg_namespace,pg_user where nspname!='pg_toast' and nspname!='pg_temp_1'"+
					"and nspname!='pg_toast_temp_1' and nspname!='pg_catalog'"+
					"and nspname!='information_schema' and nspname!='pgagent'"+
					"and nspname!='pg_temp_3' and nspname!='pg_toast_temp_3'"+
					"and usesysid = nspowner";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("nspname");
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
			String query = "SELECT tblspaces.spcname,tblspaces.spcacl,tblspaces.spcoptions,users.usename,pg_tablespace_location(tblspacesoid.oid) " + 
					"FROM pg_tablespace as tblspaces,pg_user as users,(Select oid FROM pg_tablespace where spcname!='pg_default' and spcname!='pg_global') as tblspacesoid " + 
					"WHERE users.usesysid=tblspaces.spcowner and spcname!='pg_default' and spcname!='pg_global' and tblspacesoid.oid=tblspaces.oid";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("spcname");
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
			String query = 
					"select s.sequence_name, rol.rolname as owner, s.start_value, s.minimum_value, s.maximum_value, s.increment, s.cycle_option " + 
					"from pg_class cls " + 
					"  join pg_roles rol on rol.oid = cls.relowner  " + 
					"  join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
					"  join information_schema.sequences s on cls.relname = s.sequence_name " + 
					"where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
					"  and nsp.nspname not like 'pg_toast%' " + 
					"  and cls.relkind = 'S' and s.sequence_schema = :schema ";
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);
			stmt.setString("schema", schema);
						
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameSeq = rs.getString("sequence_name");
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
					"select s.sequence_name, rol.rolname as owner, s.start_value, s.minimum_value, s.maximum_value, s.increment, s.cycle_option " + 
					"from pg_class cls " + 
					"  join pg_roles rol on rol.oid = cls.relowner  " + 
					"  join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
					"  join information_schema.sequences s on cls.relname = s.sequence_name " + 
					"where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
					"  and nsp.nspname not like 'pg_toast%' " + 
					"  and cls.relkind = 'S' and s.sequence_schema = :schema and s.sequence_name = :name ";
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);
			stmt.setString("schema", schema);
			stmt.setString("name", name);
						
			ResultSet rs = stmt.executeQuery();
			DBSequence sequence = null;
			while (rs.next()) {
				String nameSeq = rs.getString("sequence_name");
				sequence = new DBSequence();
				sequence.setName(nameSeq);
				sequence.setSchema(schema);
				sequence.setValue(0L);
				rowToProperties(rs, sequence.getOptions());
			}
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
			String query = 
					"select tablename as table_name,tableowner as owner,tablespace,hasindexes,hasrules,hastriggers "
					+ "from pg_tables where schemaname not in ('information_schema', 'pg_catalog') "
					+ "and schemaname not like 'pg_toast%' and upper(schemaname) = upper(:schema) ";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameTable = rs.getString("table_name");
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
			String query = 
					"select tablename as table_name,tableowner as owner,tablespace,hasindexes,hasrules,hastriggers from pg_tables where schemaname not in ('information_schema', 'pg_catalog') "
					+ "and schemaname not like 'pg_toast%' and upper(schemaname) = upper(\'"+schema+"\') and upper(tablename) = upper(\'"+name+"\') ";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			
			ResultSet rs = stmt.executeQuery(query);

			DBTable table = null;
			
			while (rs.next()) {
				String nameTable = rs.getString("table_name");
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
			
			String query = 
					"SELECT distinct col.column_name,col.is_nullable,col.data_type,col.character_maximum_length, tc.constraint_name, " +
					"case\r\n" + 
					"	when lower(data_type) in ('integer', 'numeric', 'smallint', 'double precision', 'bigint') then 'number' \r\n" + 
					"	when lower(data_type) in ('character varying', 'char', 'character', 'varchar') then 'string'\r\n" + 
					"	when lower(data_type) in ('timestamp without time zone', 'timestamp with time zone', 'date') then 'date'\r\n" + 
					"	when lower(data_type) in ('boolean') then 'boolean'\r\n" + 
					"	when lower(data_type) in ('text') then 'text'\r\n" + 
					"   when lower(data_type) in ('bytea') then 'binary'" +
					"	else 'native'\r\n" + 
					"	end tp, " +
					"    case when lower(data_type) in ('char', 'character') then true else false end fixed, " +
					"col.*  FROM " + 
					"information_schema.columns col  " + 
					"left join information_schema.key_column_usage kc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and col.column_name=kc.column_name " + 
					"left join information_schema.table_constraints tc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and kc.constraint_name = tc.constraint_name and tc.constraint_type = 'PRIMARY KEY' " + 
					"where upper(col.table_schema) = upper(:schema) and upper(col.table_name) = upper(:table) " + 
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
				field.setLength(rs.getInt("character_maximum_length"));
				field.setPrecision(rs.getInt("numeric_precision"));
				field.setScale(rs.getInt("numeric_scale"));
				field.setFixed(rs.getBoolean("fixed"));
				field.setOrder(rs.getInt("ordinal_position"));
				listField.put(field.getName(), field);
			}
			stmt.close();
			
			return listField;
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);			
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}		
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
			
			Integer max_length = rs.getInt("character_maximum_length");
			if (!rs.wasNull()) {
				type.append("("+max_length.toString()+")");
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
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<>();
		try {
			String query = "select i.schemaname,\r\n" +
					"i.tablename, \r\n" +
					"i.indexname, \r\n" +
					"i.tablespace, \r\n" +
					"i.indexdef as ddl \r\n" +
					"from \r\n" + 
					"pg_indexes as i JOIN pg_class as cl \r\n" + 
					"	on i.indexname = cl.relname\r\n" + 
					"JOIN pg_index AS idx \r\n" + 
					"	ON cl.oid = idx.indexrelid\r\n" + 
					"where i.tablename not like 'pg%' and i.schemaname = :schema and i.tablename = :table and idx.indisprimary = false and idx.indisunique=false ";
			
			Connection connect = getConnection();
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			stmt.setString("table", nameTable);

			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				DBIndex index = new DBIndex();
				index.setName(rs.getString("indexname"));
				index.setSchema(schema);	
				rowToProperties(rs, index.getOptions());
				indexes.put(index.getName(), index);
			}
			stmt.close();
			
			return indexes;
			
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "indexes").toString());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		Map<String, DBConstraint> constraints = new HashMap<>();
		try {
			String query = "select conname as constraint_name,contype as constraint_type, " + 
					"  pg_catalog.pg_get_constraintdef(r.oid, true) as ddl " + 
					"from " + 
					"    pg_class c " + 
					"    join pg_namespace n on n.oid = c.relnamespace " + 
					"    join pg_catalog.pg_constraint r on r.conrelid = c.relfilenode " + 
					"WHERE    " + 
					"    relname = :table and nspname = :schema and c.relkind = 'r' ";

			Connection connect = getConnection();
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);
			stmt.setString("table", nameTable);
			stmt.setString("schema", schema);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				DBConstraint con = new DBConstraint();
				con.setName(rs.getString("constraint_name"));
				con.setConstraintType(rs.getString("constraint_type"));
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
			String query = "select nsp.nspname as object_schema, " + 
				       "cls.relname as object_name,  rol.rolname as owner, 'create or replace view ' || nsp.nspname || '.' || cls.relname || ' as \n' || pg_get_viewdef(cls.oid) as ddl "+
				       "from pg_class cls " + 
				         " join pg_roles rol on rol.oid = cls.relowner" +
				         " join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
				       " where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
				       " and nsp.nspname not like 'pg_toast%' " +
				       "and cls.relkind = 'v' and nsp.nspname = '" + schema + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBView view = new DBView(rs.getString("object_name"));
				view.setSchema(rs.getString("object_schema"));
				view.setOwner(rs.getString("owner"));
				rowToProperties(rs, view.getOptions());
				listView.put(rs.getString("object_name"), view);
			}
			stmt.close();
			return listView;
		}catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
		}
	}

	@Override
	public DBView getView(String schema, String name) {
		DBView view = new DBView(name);
		view.setSchema(schema);
		try {
			String query = "select nsp.nspname as object_schema, " + 
				       "cls.relname as object_name,  rol.rolname as owner, 'create or replace view ' || nsp.nspname || '.' || cls.relname || ' as \n' || pg_get_viewdef(cls.oid) as ddl "+
				       "from pg_class cls " + 
				         " join pg_roles rol on rol.oid = cls.relowner" +
				         " join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
				       " where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
				       " and nsp.nspname not like 'pg_toast%' " +
				       "and cls.relkind = 'v' and nsp.nspname = '" + schema + "' and cls.relname='"+name+"'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			while (rs.next()) {
				view.setOwner(rs.getString("owner"));
				rowToProperties(rs, view.getOptions());
			}
			stmt.close();
			return view;
			
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
		}		
	}
	
	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		try {
			String query = "SELECT trg.tgname, tbl.relname as trigger_table ,pg_get_triggerdef(trg.oid) AS ddl \r\n" + 
					"FROM pg_trigger trg\r\n" + 
					"JOIN pg_class tbl on trg.tgrelid = tbl.oid\r\n" + 
					"JOIN pg_namespace ns ON ns.oid = tbl.relnamespace\r\n" + 
					"and trg.tgconstraint=0 and ns.nspname like \'"+schema+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("tgname");
				String sql = rs.getString("ddl");
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSchema(schema);
				trigger.setOwner("postgres");
				rowToProperties(rs, trigger.getOptions());
				listTrigger.put(name, trigger);
			}
			stmt.close();
			return listTrigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);	
		}
	}
	@Override
	public DBTrigger getTrigger(String schema, String name) {
		DBTrigger trigger = null;
		try {
			String query = "SELECT trg.tgname, tbl.relname as trigger_table ,pg_get_triggerdef(trg.oid) AS ddl \r\n" + 
					"FROM pg_trigger trg\r\n" + 
					"JOIN pg_class tbl on trg.tgrelid = tbl.oid\r\n" + 
					"JOIN pg_namespace ns ON ns.oid = tbl.relnamespace\r\n" + 
					"and trg.tgconstraint=0 and ns.nspname like \'"+schema+"\' and trg.tgname like \'"+name+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String sql = rs.getString("ddl");
				trigger = new DBTrigger(name);		
				trigger.setSchema(schema);
				trigger.setOwner("postgres");
				rowToProperties(rs, trigger.getOptions());
			}
			stmt.close();
			return trigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);	
		}

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
		Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		try {
			String query = "SELECT n.nspname as \"schema\",u.rolname,\r\n" + 
					"       p.proname as \"name\",\r\n" + 
					"       pg_catalog.pg_get_function_arguments(p.oid) as \"arguments\",\r\n" + 
					"	   pg_get_functiondef(p.oid) AS ddl\r\n" + 
					"FROM pg_catalog.pg_proc p\r\n" + 
					"  JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\r\n" + 
					"  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\r\n" + 
					"WHERE pg_catalog.pg_function_is_visible(p.oid)\r\n" + 
					"  AND n.nspname = \'"+schema+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("name");
				String owner = rs.getString("rolname");
				String args = rs.getString("arguments");
				DBFunction func = new DBFunction(name);
				func.setSchema(schema);
				func.setOwner(owner);
				rowToProperties(rs,func.getOptions());
				//func.setArguments(args);
				listFunction.put(name, func);
			}
			stmt.close();
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
		}
		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		
		try {
			String query = "SELECT n.nspname as \"schema\",u.rolname,\r\n" + 
					"       p.proname as \"name\",\r\n" + 
					"       pg_catalog.pg_get_function_arguments(p.oid) as \"arguments\",\r\n" + 
					"	   pg_get_functiondef(p.oid) AS ddl\r\n" + 
					"FROM pg_catalog.pg_proc p\r\n" + 
					"  JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\r\n" + 
					"  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\r\n" + 
					"WHERE pg_catalog.pg_function_is_visible(p.oid)\r\n" + 
					"  AND n.nspname = \'"+schema+ "\' AND p.proname=\'"+name+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			DBFunction func = null;
			while (rs.next()) {
				func = new DBFunction(rs.getString("name"));
				String owner = rs.getString("rolname");
				String args = rs.getString("arguments");
				func.setSchema(schema);
				func.setOwner(owner);
				//func.setArguments(args);
				rowToProperties(rs,func.getOptions());
			}
			stmt.close();
			
			return func;
			
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);			
		}
	}

	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		String tableName = schema+"."+nameTable;
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
			}	
			Statement st = getConnection().createStatement();
			ResultSet rs = st.executeQuery("select * from "+tableName);
			data.setResultSet(rs);			
			
			//TODO other state
			
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
	public DBTableRow getTableRow(DBTable tbl, Object id) {
		// TODO Auto-generated method stub
		return null;
	}
*/
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
			stmt.close();
		}catch(Exception e) {
			logger.error(e.getMessage());
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
			String query = "select *,array_to_string(array(SELECT rolname " + 
					"FROM pg_roles,pg_auth_members " + 
					"WHERE member = auth.oid and roleid=oid), ', ') as rolmemberof from pg_authid as auth where auth.rolname not like 'pg_%'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
					String name = rs.getString("rolname");
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
		return true;
	}

	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		return null;
	}
	
	@Override
	public String getDbType() {
		return "postgresql";
	}
	
	@Override
	public String getDbVersion() {
		try {
		PreparedStatement stmt = getConnection().prepareStatement("SHOW server_version");
		ResultSet resultSet = stmt.executeQuery();			
		resultSet.next();
		
		String result = resultSet.getString("server_version");
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
		try {
			Statement st = connect.createStatement();
			ResultSet rs = st.executeQuery("select count(*) cnt from pg_catalog.pg_roles where upper(rolname) = '" + 
					roleName.toUpperCase() + "'");
			
			rs.next();
			if (rs.getInt("cnt") == 0) {
				StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());
				stLog.execute("CREATE ROLE " + roleName + " LOGIN PASSWORD '" + roleName +  "'");
	
				stLog.close();
			}
			
			connect.commit();
			rs.close();
			st.close();
		} catch (SQLException e) {			
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "createSchema") + ": " + e.getLocalizedMessage());
		}		
	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		return "public";
	}

	@Override
	public boolean isReservedWord(String word) {
		// TODO Auto-generated method stub
		return false;
	}	

}
