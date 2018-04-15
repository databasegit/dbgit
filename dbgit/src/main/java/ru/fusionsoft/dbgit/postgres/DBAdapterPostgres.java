package ru.fusionsoft.dbgit.postgres;


import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
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

	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestorePostgres restoreFactory = new FactoryDBAdapterRestorePostgres();
	
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
			String query = "select * from pg_namespace where nspname!='pg_toast' and nspname!='pg_temp_1'"+
					"and nspname!='pg_toast_temp_1' and nspname!='pg_catalog'"+
					"and nspname!='information_schema' and nspname!='pgagent'"+
					"and nspname!='pg_temp_3' and nspname!='pg_toast_temp_3'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("nspname");
				DBSchema scheme = new DBSchema(name);
				listScheme.put(name, scheme);	
			}			
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
			String query = "Select * from pg_tablespace where spcname!='pg_default' and spcname!='pg_global'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBTableSpace dbTableSpace = new DBTableSpace(name);
				listTableSpace.put(name, dbTableSpace);
			}			
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listTableSpace;
	}

	@Override
	public Map<String, DBSequence> getSequences(DBSchema schema) {
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
					"  and cls.relkind = 'S' and nsp.nspname = :schema ";
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);
			stmt.setString("schema", schema.getName());
						
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameSeq = rs.getString("sequence_name");
				DBSequence sequence = new DBSequence();
				sequence.setName(nameSeq);
				sequence.setSchema(schema.getName());
				sequence.setValue(0L);
				rowToProperties(rs, sequence.getOptions());
				listSequence.put(nameSeq, sequence);
			}
		}catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new ExceptionDBGitRunTime(e.getMessage(), e);
		}
		return listSequence;
	}

	@Override
	public DBSequence getSequence(DBSchema schema, String name) {
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
					"  and cls.relkind = 'S' and nsp.nspname = :schema and nsp.relname = :name ";
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);
			stmt.setString("schema", schema.getName());
			stmt.setString("name", name);
						
			ResultSet rs = stmt.executeQuery();
			rs.next();
			String nameSeq = rs.getString("relname");
			DBSequence sequence = new DBSequence();
			sequence.setName(nameSeq);
			sequence.setSchema(schema.getName());
			sequence.setValue(0L);
			rowToProperties(rs, sequence.getOptions());
				
			return sequence;
		}catch(Exception e) {
			logger.error(e.getMessage(), e);
			throw new ExceptionDBGitRunTime(e.getMessage(), e);
		}
	}

	@Override
	public Map<String, DBTable> getTables(DBSchema schema) {
		Map<String, DBTable> listTable = new HashMap<String, DBTable>();
		try {
			String query = 
					"select nsp.nspname as object_schema, " + 
					"       cls.relname as object_name,  rol.rolname as owner " + 
					"from pg_class cls " + 
					"  join pg_roles rol on rol.oid = cls.relowner " + 
					"  join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
					"where nsp.nspname not in ('information_schema', 'pg_catalog')  " + 
					"  and nsp.nspname not like 'pg_toast%'" + 
					"  and cls.relkind = 'r' and nsp.nspname = :schema ";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema.getName());
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameTable = rs.getString("object_name");
				DBTable table = new DBTable(nameTable);
				table.setSchema(schema.getName());
				table.getOptions().addChild("owner", rs.getString("owner"));
				listTable.put(nameTable, table);
			}			
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
		return listTable;
	}

	@Override
	public DBTable getTable(DBSchema schema, String name) {
		try {
			String query = 
					"select nsp.nspname as object_schema, " + 
					"       cls.relname as object_name,  rol.rolname as owner " + 
					"from pg_class cls " + 
					"  join pg_roles rol on rol.oid = cls.relowner " + 
					"  join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
					"where nsp.nspname not in ('information_schema', 'pg_catalog')  " + 
					"  and nsp.nspname not like 'pg_toast%'" + 
					"  and cls.relkind = 'r' and nsp.nspname = :schema and cls.relname = :name ";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema.getName());
			stmt.setString("name", name);
			
			ResultSet rs = stmt.executeQuery();
			rs.next();
			String nameTable = rs.getString("object_name");
			DBTable table = new DBTable(nameTable);
			table.setSchema(schema.getName());
			table.getOptions().addChild("owner", rs.getString("owner"));

			return table;
		
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}
	}

	@Override
	public Map<String, DBTableField> getTableFields(DBTable tbl) {
		
		try {
			Map<String, DBTableField> listField = new HashMap<String, DBTableField>();
			
			String query = 
					"SELECT * FROM information_schema.columns where table_schema = :schema and table_name = :table ";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", tbl.getSchema());
			stmt.setString("table", tbl.getName());
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){				
				DBTableField field = new DBTableField();
				field.setName(rs.getString("column_name"));
				field.setTypeSQL(getFieldType(rs));
				listField.put(field.getName(), field);
			}			
			return listField;
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}		
	}
	
	protected String getFieldType(ResultSet rs) {
		try {
			StringBuilder type = new StringBuilder(); 
			type.append(rs.getString("data_type"));
			
			Integer max_length = rs.getInt("character_maximum_length");
			if (!rs.wasNull()) {
				type.append("("+max_length.toString()+")");
			}
			
			if (rs.getString("is_nullable") == "NO") {
				type.append(" NOT NULL");
			}
			
			
			return type.toString();
		}catch(Exception e) {
			logger.error("Error load tables.", e);			
			throw new ExceptionDBGitRunTime("Error load tables.", e);
		}	
	}

	@Override
	public Map<String, DBIndex> getIndexes(DBTable tbl) {
		Map<String, DBIndex> indexes = new HashMap<>();
		// TODO Auto-generated method stub
		return indexes;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(DBTable tbl) {
		Map<String, DBConstraint> constraints = new HashMap<>();
		// TODO Auto-generated method stub
		return constraints;
	}

	@Override
	public Map<String, DBView> getViews(DBSchema schema) {
		Map<String, DBView> listView = new HashMap<String, DBView>();
		try {
			String query = "select nsp.nspname as object_schema, " + 
				       "cls.relname as object_name,  rol.rolname as owner, pg_get_viewdef(cls.oid) as sql "+
				       "from pg_class cls " + 
				         " join pg_roles rol on rol.oid = cls.relowner" +
				         " join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
				       " where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
				       " and nsp.nspname not like 'pg_toast%' " +
				       "and cls.relkind = 'v' and nsp.nspname = '" + schema.getName() + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBView view = new DBView(rs.getString("object_name"));
				view.setSql(rs.getString("sql"));
				listView.put(rs.getString("object_name"), view);
			}
			System.out.println("Collection views:");
			for(DBView view:listView.values())
				System.out.println(view.getName());
		}catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listView;
	}

	@Override
	public DBView getView(DBSchema schema, String name) {
		DBView view = new DBView(name);
		try {
			String query = "select nsp.nspname as object_schema, " + 
				       "cls.relname as object_name,  rol.rolname as owner, pg_get_viewdef(cls.oid) as sql "+
				       "from pg_class cls " + 
				         " join pg_roles rol on rol.oid = cls.relowner" +
				         " join pg_namespace nsp on nsp.oid = cls.relnamespace " + 
				       " where nsp.nspname not in ('information_schema', 'pg_catalog') " + 
				       " and nsp.nspname not like 'pg_toast%' " +
				       "and cls.relkind = 'v' and nsp.nspname = '" + schema.getName() + "' and cls.relname='"+name+"'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				view.setSql(rs.getString("sql"));
			}
			
		}catch(Exception e) {
			logger.error(e.getMessage());
			System.out.println(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return view;
	}
	
	public Map<String, DBTrigger> getTriggers(DBSchema schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		try {
			String query = "SELECT pg_trigger.tgname, pg_get_triggerdef(pg_trigger.oid) AS src \r\n" + 
					"FROM pg_trigger, pg_class, pg_namespace\r\n" + 
					"where pg_namespace.nspname like '" + schema.getName()+"' and pg_namespace.oid=pg_class.relnamespace and pg_trigger.tgrelid=pg_class.oid " +
					"and pg_trigger not like '%Constraint%'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				String sql = rs.getString(2);
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSql(sql);
				listTrigger.put(name, trigger);
			}
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		// TODO Auto-generated method stub
		return listTrigger;
	}
	
	public DBTrigger getTrigger(DBSchema schema, String name) {
		DBTrigger trigger = null;
		try {
			String query = "SELECT pg_trigger.tgname, pg_get_triggerdef(pg_trigger.oid) AS src \r\n" + 
					"FROM pg_trigger, pg_class, pg_namespace\r\n" + 
					"where pg_namespace.nspname like '" + schema.getName()+"' and tgname='"+name+"' and pg_namespace.oid=pg_class.relnamespace and pg_trigger.tgrelid=pg_class.oid ";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String sql = rs.getString(2);
				trigger = new DBTrigger(name);
				trigger.setSql(sql);				
			}
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		// TODO Auto-generated method stub
		return trigger;
	}

	@Override
	public Map<String, DBPackage> getPackages(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBPackage getPackage(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBProcedure getProcedure(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBFunction> getFunctions(DBSchema schema) {
		Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		try {
			String query = "SELECT pg_proc.proname, pg_get_functiondef(pg_proc.oid) AS src \r\n" + 
					"FROM pg_proc, pg_namespace\r\n" + 
					"where pg_namespace.nspname like '" + schema.getName()+"' and pg_namespace.oid=pg_proc.pronamespace";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				String sql = rs.getString(2);
				DBFunction func = new DBFunction(name);
				func.setSql(sql);
				listFunction.put(name, func);
			}
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		// TODO Auto-generated method stub
		return listFunction;
	}

	@Override
	public DBFunction getFunction(DBSchema schema, String name) {
		DBFunction func = new DBFunction(name);
		try {
			String query = "SELECT pg_proc.proname, pg_get_functiondef(pg_proc.oid) AS src \r\n" + 
					"FROM pg_proc, pg_namespace\r\n" + 
					"where pg_namespace.nspname like '" + schema.getName()+"' and pg_namespace.oid=pg_proc.pronamespace" +
					"pg_proc.name='"+name+"'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String sql = rs.getString(2);
				func.setSql(sql);
				return func;
			}
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(e.getMessage());			
		}
		return func;
	}

	@Override
	public DBTableData getTableData(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableRow getTableRow(DBTable tbl, Object id) {
		// TODO Auto-generated method stub
		return null;
	}

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
			String query = "select *from pg_roles where rolname not like 'pg_%'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString(1);
				DBRole role = new DBRole(name);
				listRole.put(name, role);
			}
		}catch(Exception e) {
			logger.error(e.getMessage());
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
		return listRole;
	}
	
	public static void main(String[] args) {
		System.out.println("start");
		try {
			DBAdapterPostgres dbAdapter = (DBAdapterPostgres) AdapterFactory.createAdapter();
			
			DBView view = dbAdapter.getView(new DBSchema("fusion"), "sa_types");
			System.out.println(view.getName());
			System.out.println(view.getSql());
		}catch(ExceptionDBGitRunTime e) {
			e.printStackTrace();
		}catch(Exception e) {
			System.out.println(e.getMessage());
		}
	}
}
