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
import ru.fusionsoft.dbgit.core.ExceptionDBGitObjectNotFound;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.data_table.MapFileData;
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
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;


public class DBAdapterPostgres extends DBAdapter {
	public static final String DEFAULT_MAPPING_TYPE = "string";

	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestorePostgres restoreFactory = new FactoryDBAdapterRestorePostgres();
	
	public void registryMappingTypes() {
		FactoryCellData.regMappingTypes(DEFAULT_MAPPING_TYPE, StringData.class);
		FactoryCellData.regMappingTypes("integer", LongData.class);
		FactoryCellData.regMappingTypes("bigint", LongData.class);
		FactoryCellData.regMappingTypes("bytea", MapFileData.class);
		//FactoryCellData.regMappingTypes("bytea", LargeBlobPg.class);
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
			logger.error("Error load schemes!", e);
			throw new ExceptionDBGitRunTime("Error load schemes!", e);
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
					"select s.*, rol.rolname as owner " + 
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
					"select s.*, rol.rolname as owner " + 
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
			rs.next();
			String nameSeq = rs.getString("sequence_name");
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
			String query = 
					"select tablename,tableowner,tablespace,hasindexes,hasrules,hastriggers "
					+ "from pg_tables where schemaname not in ('information_schema', 'pg_catalog') "
					+ "and schemaname not like 'pg_toast%' and schemaname = :schema ";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameTable = rs.getString("tablename");
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
			String query = 
					"select tablename,tableowner,tablespace,hasindexes,hasrules,hastriggers from pg_tables where schemaname not in ('information_schema', 'pg_catalog') "
					+ "and schemaname not like 'pg_toast%' and schemaname = \'"+schema+"\' and tablename = \'"+name+"\' ";
			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			
			ResultSet rs = stmt.executeQuery(query);
			rs.next();
			String nameTable = rs.getString("tablename");
			DBTable table = new DBTable(nameTable);
			table.setSchema(schema);
			rowToProperties(rs, table.getOptions());

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
					"SELECT distinct col.column_name,col.is_nullable,col.data_type,col.character_maximum_length, tc.constraint_name  FROM " + 
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
		String tp = rs.getString("data_type");
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
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}	
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<>();
		try {
			String query = "select i.*\r\n" + 
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
				index.setSql(rs.getString("indexdef"));		
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
			String query = "select conname as constraint_name,contype as constraint_type, " + 
					"  pg_catalog.pg_get_constraintdef(r.oid, true) as constraint_def " + 
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
				con.setConstraintDef(rs.getString("constraint_def"));
				con.setConstraintType(rs.getString("constraint_type"));
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
			String query = "select nsp.nspname as object_schema, " + 
				       "cls.relname as object_name,  rol.rolname as owner, pg_get_viewdef(cls.oid) as sql "+
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
				view.setSql(rs.getString("sql"));
				view.setSchema(rs.getString("object_schema"));
				view.setOwner(rs.getString("owner"));
				listView.put(rs.getString("object_name"), view);
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
			String query = "select nsp.nspname as object_schema, " + 
				       "cls.relname as object_name,  rol.rolname as owner, pg_get_viewdef(cls.oid) as sql "+
				       "from pg_class cls " + 
				         " join pg_roles rol on rol.oid = cls.relowner" +
				         " join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
				       " where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
				       " and nsp.nspname not like 'pg_toast%' " +
				       "and cls.relkind = 'v' and nsp.nspname = '" + schema + "' and cls.relname='"+name+"'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			
			rs.next();
			view.setOwner(rs.getString("owner"));
			view.setSql(rs.getString("sql"));
			
			stmt.close();
			return view;
			
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		
	}
	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		try {
			String query = "SELECT trg.tgname, tbl.relname as trigger_table ,pg_get_triggerdef(trg.oid) AS src \r\n" + 
					"FROM pg_trigger trg\r\n" + 
					"JOIN pg_class tbl on trg.tgrelid = tbl.oid\r\n" + 
					"JOIN pg_namespace ns ON ns.oid = tbl.relnamespace\r\n" + 
					"and trg.tgconstraint=0 and ns.nspname like \'"+schema+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("tgname");
				String sql = rs.getString("src");
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSql(sql);
				trigger.setSchema(schema);
				trigger.setOwner("postgres");
				rowToProperties(rs, trigger.getOptions());
				listTrigger.put(name, trigger);
			}
			stmt.close();
			return listTrigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime("Error ", e);	
		}
	}
	@Override
	public DBTrigger getTrigger(String schema, String name) {
		DBTrigger trigger = null;
		try {
			String query = "SELECT trg.tgname, tbl.relname as trigger_table ,pg_get_triggerdef(trg.oid) AS src \r\n" + 
					"FROM pg_trigger trg\r\n" + 
					"JOIN pg_class tbl on trg.tgrelid = tbl.oid\r\n" + 
					"JOIN pg_namespace ns ON ns.oid = tbl.relnamespace\r\n" + 
					"and trg.tgconstraint=0 and ns.nspname like \'"+schema+"\' and trg.tgname like \'"+name+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String sql = rs.getString("src");
				trigger = new DBTrigger(name);
				trigger.setSql(sql);			
				trigger.setSchema(schema);
				trigger.setOwner("postgres");
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
					"	   pg_get_functiondef(p.oid) AS src\r\n" + 
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
				String sql = rs.getString("src");
				String owner = rs.getString("rolname");
				String args = rs.getString("arguments");
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
			String query = "SELECT n.nspname as \"schema\",u.rolname,\r\n" + 
					"       p.proname as \"name\",\r\n" + 
					"       pg_catalog.pg_get_function_arguments(p.oid) as \"arguments\",\r\n" + 
					"	   pg_get_functiondef(p.oid) AS src\r\n" + 
					"FROM pg_catalog.pg_proc p\r\n" + 
					"  JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\r\n" + 
					"  LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\r\n" + 
					"WHERE pg_catalog.pg_function_is_visible(p.oid)\r\n" + 
					"  AND n.nspname = \'"+schema+ "\' AND p.proname=\'"+name+"\'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			rs.next();
			DBFunction func = new DBFunction(rs.getString("name"));
			String owner = rs.getString("rolname");
			String args = rs.getString("arguments");
			func.setSchema(schema);
			func.setSql(rs.getString("src"));
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
	public DBTableData getTableData(String schema, String nameTable, int paramFetch) {
		String tableName = schema+"."+nameTable;
		try {
			DBTableData data = new DBTableData();
			
			
			if (paramFetch == LIMIT_FETCH) {
				Statement st = getConnection().createStatement();
				String query = "select COALESCE(count(*), 0) kolvo from ( select 1 from "+
						tableName+" limit "+(MAX_ROW_COUNT_FETCH+1)+" ) tbl";
				ResultSet rs = st.executeQuery(query);
				rs.next();
				if (rs.getInt("kolvo") > MAX_ROW_COUNT_FETCH) {
					data.setErrorFlag(DBTableData.ERROR_LIMIT_ROWS);
					return data;
				}
				
				rs = st.executeQuery("select * from "+tableName);
				data.setResultSet(rs);
				return data;
			}
			
			//TODO other state
			
			return data;
		} catch(Exception e) {
			logger.error("Error load data from "+tableName, e);
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
	

}
