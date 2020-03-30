package ru.fusionsoft.dbgit.postgres;


import java.sql.*;
import java.text.MessageFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitObjectNotFound;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.core.db.FieldType;
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
	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestorePostgres restoreFactory = new FactoryDBAdapterRestorePostgres();
	private FactoryDbConvertAdapterPostgres convertFactory = new FactoryDbConvertAdapterPostgres();	
	private FactoryDBBackupAdapterPostgres backupFactory = new FactoryDBBackupAdapterPostgres();
	private static Set<String> reservedWords = new HashSet<>();

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
					"select \n" +
					"	tablename as table_name,\n" +
					"	tableowner as owner,\n" +
					"	tablespace,hasindexes,hasrules,hastriggers, \n" +
					"	obj_description(to_regclass(schemaname || '.\"' || tablename || '\"')::oid) table_comment, ( \n" +
					"		select array_agg(distinct n2.nspname || '.' || c2.relname) as dependencies\n" +
					"	 	FROM pg_catalog.pg_constraint c  \n" +
					"		JOIN ONLY pg_catalog.pg_class c1     ON c1.oid = c.confrelid\n" +
					"		JOIN ONLY pg_catalog.pg_class c2     ON c2.oid = c.conrelid\n" +
					"		JOIN ONLY pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace\n" +
					"		WHERE c.confrelid = to_regclass(schemaname || '.\"' || tablename || '\"')::oid\n" +
					"		and c1.relkind = 'r' AND c.contype = 'f'\n" +
					"	)\n" +
					"from pg_tables \n" +
					"where upper(schemaname) = upper(:schema)";
			Connection connect = getConnection();
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			
			ResultSet rs = stmt.executeQuery();
			while(rs.next()){
				String nameTable = rs.getString("table_name");
				DBTable table = new DBTable(nameTable);
				table.setSchema(schema);
				table.setComment(rs.getString("table_comment"));
				if(rs.getArray("dependencies") != null){
					table.setDependencies(new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray())));
				} else table.setDependencies(new HashSet<>());
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
				"select \n" +
				"	tablename as table_name,\n" +
				"	tableowner as owner,\n" +
				"	tablespace,hasindexes,hasrules,hastriggers, \n" +
				"	obj_description(to_regclass(schemaname || '.\"' || tablename || '\"')::oid) table_comment, ( \n" +
				"		select array_agg(distinct n2.nspname || '.' || c2.relname) as dependencies\n" +
				"	 	FROM pg_catalog.pg_constraint c  \n" +
				"		JOIN ONLY pg_catalog.pg_class c1     ON c1.oid = c.confrelid\n" +
				"		JOIN ONLY pg_catalog.pg_class c2     ON c2.oid = c.conrelid\n" +
				"		JOIN ONLY pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace\n" +
				"		WHERE c.confrelid = to_regclass(schemaname || '.\"' || tablename || '\"')::oid\n" +
				"		and c1.relkind = 'r' AND c.contype = 'f'\n" +
				"	)\n" +
				"from pg_tables \n" +
				"where upper(schemaname) = upper('"+schema+"') \n" +
				"and upper(tablename) = upper('"+name+"')\n";

			Connection connect = getConnection();
			
			Statement stmt = connect.createStatement();
			
			ResultSet rs = stmt.executeQuery(query);

			DBTable table = null;
			
			while (rs.next()) {
				String nameTable = rs.getString("table_name");
				table = new DBTable(nameTable);
				table.setSchema(schema);
				table.setComment(rs.getString("table_comment"));
				if(rs.getArray("dependencies") != null){
					table.setDependencies(new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray())));
				} else table.setDependencies(new HashSet<>());
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
					"SELECT distinct col.column_name,col.is_nullable,col.data_type, col.udt_name::regtype dtype,col.character_maximum_length, col.column_default, tc.constraint_name, " +
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
					"  pgd.description," +
					"col.*  FROM " + 
					"information_schema.columns col  " + 
					"left join information_schema.key_column_usage kc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and col.column_name=kc.column_name " + 
					"left join information_schema.table_constraints tc on col.table_schema = kc.table_schema and col.table_name = kc.table_name and kc.constraint_name = tc.constraint_name and tc.constraint_type = 'PRIMARY KEY' " + 
					"left join pg_catalog.pg_statio_all_tables st on st.schemaname = col.table_schema and st.relname = col.table_name " +
					"left join pg_catalog.pg_description pgd on (pgd.objoid=st.relid and pgd.objsubid=col.ordinal_position) " +
					"where upper(col.table_schema) = upper(:schema) and upper(col.table_name) = upper(:table) " + 
					"order by col.column_name ";
			Connection connect = getConnection();			
			
			NamedParameterPreparedStatement stmt = NamedParameterPreparedStatement.createNamedParameterPreparedStatement(connect, query);			
			stmt.setString("schema", schema);
			stmt.setString("table", nameTable);

			ResultSet rs = stmt.executeQuery();
			while(rs.next()){				
				DBTableField field = new DBTableField();
				
				field.setName(rs.getString("column_name"));  
				field.setDescription(rs.getString("description"));
				field.setNameExactly(!rs.getString("column_name").equals(rs.getString("column_name").toLowerCase()));
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
				field.setOrder(rs.getInt("ordinal_position"));
				field.setDefaultValue(rs.getString("column_default"));
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
			type.append(rs.getString("dtype"));
			
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
					"where i.tablename not like 'pg%' and i.schemaname = :schema and i.tablename = :table --and idx.indisprimary = false and idx.indisunique=false ";
			
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
			/*
			String query = "select conname as constraint_name,contype as constraint_type, " + 
					"  pg_catalog.pg_get_constraintdef(r.oid, true) as ddl " + 
					"from " + 
					"    pg_class c " + 
					"    join pg_namespace n on n.oid = c.relnamespace " + 
					"    join pg_catalog.pg_constraint r on r.conrelid = c.relfilenode " + 
					"WHERE    " + 
					"    relname = :table and nspname = :schema and c.relkind = 'r'";
			*/
			
			
			String query = "SELECT conname as constraint_name,contype as constraint_type, \r\n" + 
					"  pg_catalog.pg_get_constraintdef(con.oid, true) as ddl\r\n" + 
					"       FROM pg_catalog.pg_constraint con\r\n" + 
					"            INNER JOIN pg_catalog.pg_class rel\r\n" + 
					"                       ON rel.oid = con.conrelid\r\n" + 
					"            INNER JOIN pg_catalog.pg_namespace nsp\r\n" + 
					"                       ON nsp.oid = connamespace\r\n" + 
					"       WHERE nsp.nspname = :schema\r\n" + 
					"             AND rel.relname = :table";
			
			
			
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
			String query =
				"select nsp.nspname as object_schema, cls.relname as object_name,  rol.rolname as owner, \n" +
				"'create or replace view ' || nsp.nspname || '.' || cls.relname || ' as ' || pg_get_viewdef(cls.oid) as ddl, (\n" +
				"	select array_agg(distinct source_ns.nspname || '.' || source_table.relname) as dependencySam\n" +
				"	from pg_depend \n" +
				"	join pg_rewrite ON pg_depend.objid = pg_rewrite.oid \n" +
				"	join pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid \n" +
				"	join pg_class as source_table ON pg_depend.refobjid = source_table.oid \n" +
				"	join pg_attribute ON pg_attribute.attrelid  = pg_depend.refobjid \n" +
				"		and pg_attribute.attnum = pg_depend.refobjsubid  \n" +
				"	join pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace\n" +
				"	join pg_namespace source_ns ON source_ns.oid = source_table.relnamespace\n" +
				"	where pg_attribute.attnum > 0 \n" +
				"	and dependent_view.relname = cls.relname\n" +
				") as dependencies\n" +
				"from pg_class cls  \n" +
				"join pg_roles rol on rol.oid = cls.relowner \n" +
				"join pg_namespace nsp on nsp.oid = cls.relnamespace  \n" +
				"where nsp.nspname not in ('information_schema', 'pg_catalog')  \n" +
				"and nsp.nspname not like 'pg_toast%' \n" +
				"and cls.relkind = 'v' \n" +
				"and nsp.nspname = '"+schema+"' \n";

			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()){
				DBView view = new DBView(rs.getString("object_name"));
				view.setSchema(rs.getString("object_schema"));
				view.setOwner(rs.getString("owner"));
				rowToProperties(rs, view.getOptions());
				if(rs.getArray("dependencies") != null){
					view.setDependencies(new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray())));
				}
				listView.put(rs.getString("object_name"), view);
			}

			stmt.close();
			return listView;
		} catch (Exception e) {
			logger.error(e.getMessage());
			System.out.println(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
		}
	}

	@Override
	public DBView getView(String schema, String name) {
		DBView view = new DBView(name);
		view.setSchema(schema);
		view.setDependencies(new HashSet<>());

		try {

			String query =
				"select nsp.nspname as object_schema, cls.relname as object_name,  rol.rolname as owner, \n" +
				"'create or replace view ' || nsp.nspname || '.' || cls.relname || ' as ' || pg_get_viewdef(cls.oid) as ddl, (\n" +
				"	select array_agg(distinct source_ns.nspname || '.' || source_table.relname) as dependencySam\n" +
				"	from pg_depend \n" +
				"	join pg_rewrite ON pg_depend.objid = pg_rewrite.oid \n" +
				"	join pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid \n" +
				"	join pg_class as source_table ON pg_depend.refobjid = source_table.oid \n" +
				"	join pg_attribute ON pg_attribute.attrelid  = pg_depend.refobjid \n" +
				"		and pg_attribute.attnum = pg_depend.refobjsubid  \n" +
				"	join pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace\n" +
				"	join pg_namespace source_ns ON source_ns.oid = source_table.relnamespace\n" +
				"	where pg_attribute.attnum > 0 \n" +
				"	and dependent_view.relname = cls.relname\n" +
				") as dependencies\n" +
				"from pg_class cls  \n" +
				"join pg_roles rol on rol.oid = cls.relowner \n" +
				"join pg_namespace nsp on nsp.oid = cls.relnamespace  \n" +
				"where nsp.nspname not in ('information_schema', 'pg_catalog')  \n" +
				"and nsp.nspname not like 'pg_toast%' \n" +
				"and cls.relkind = 'v' \n" +
				"and nsp.nspname = '"+schema+"' \n" +
				"and cls.relname='"+name+"'\n";

			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while (rs.next()) {
				view.setOwner(rs.getString("owner"));
				if(rs.getArray("dependencies") != null){
					view.setDependencies(new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray())));
				}
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
								
				listFunction.put(listFunction.containsKey(name) ? name + "_" + func.getHash() : name, func);
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
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		DBTableData data = new DBTableData();
		
		try {
			int portionSize = DBGitConfig.getInstance().getInteger("core", "PORTION_SIZE", DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000));
			
			int begin = 1 + portionSize*portionIndex;
			int end = portionSize + portionSize*portionIndex;
			
			Statement st = getConnection().createStatement();
			String query = "    SELECT * FROM \r\n" + 
					"   (SELECT f.*, ROW_NUMBER() OVER (ORDER BY ctid) DBGIT_ROW_NUM FROM " + schema + "." + (nameTable.contains(".")?("\"" + nameTable + "\""):nameTable) + " f) s\r\n" + 
					"   WHERE DBGIT_ROW_NUM BETWEEN " + begin  + " and " + end;
			ResultSet rs = st.executeQuery(query);
			
			data.setResultSet(rs);	
			return data;
		} catch(Exception e) {
			ConsoleWriter.println("err: " + e.getLocalizedMessage());
			
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
		return backupFactory;
	}
	
	@Override
	public DbType getDbType() {
		return DbType.POSTGRES;
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
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return convertFactory;
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
				stLog.execute("create schema " + schemaName + ";\n");

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
		return reservedWords.contains(word.toUpperCase());
	}


	public static String escapeNameIfNeeded(String name){
		boolean shouldBeEscaped = !name.equals(name.toLowerCase()) || name.contains(".") || reservedWords.contains(name.toUpperCase()); //TODO maybe check on isReservedWord?
		if(name.startsWith("\"") && name.endsWith("\"")) shouldBeEscaped = false;
		return MessageFormat.format("{1}{0}{1}", name, shouldBeEscaped ? "\"" : "");
	}

	static {
		reservedWords.add("A");
		reservedWords.add("ABORT");
		reservedWords.add("ABS");
		reservedWords.add("ABSOLUTE");
		reservedWords.add("ACCESS");
		reservedWords.add("ACTION");
		reservedWords.add("ADA");
		reservedWords.add("ADD");
		reservedWords.add("ADMIN");
		reservedWords.add("AFTER");
		reservedWords.add("AGGREGATE");
		reservedWords.add("ALIAS");
		reservedWords.add("ALL");
		reservedWords.add("ALLOCATE");
		reservedWords.add("ALSO");
		reservedWords.add("ALTER");
		reservedWords.add("ALWAYS");
		reservedWords.add("ANALYSE");
		reservedWords.add("ANALYZE");
		reservedWords.add("AND");
		reservedWords.add("ANY");
		reservedWords.add("ARE");
		reservedWords.add("ARRAY");
		reservedWords.add("AS");
		reservedWords.add("ASC");
		reservedWords.add("ASENSITIVE");
		reservedWords.add("ASSERTION");
		reservedWords.add("ASSIGNMENT");
		reservedWords.add("ASYMMETRIC");
		reservedWords.add("AT");
		reservedWords.add("ATOMIC");
		reservedWords.add("ATTRIBUTE");
		reservedWords.add("ATTRIBUTES");
		reservedWords.add("AUTHORIZATION");
		reservedWords.add("AVG");
		reservedWords.add("BACKWARD");
		reservedWords.add("BEFORE");
		reservedWords.add("BEGIN");
		reservedWords.add("BERNOULLI");
		reservedWords.add("BETWEEN");
		reservedWords.add("BIGINT");
		reservedWords.add("BINARY");
		reservedWords.add("BIT");
		reservedWords.add("BITVAR");
		reservedWords.add("BIT_LENGTH");
		reservedWords.add("BLOB");
		reservedWords.add("BOOLEAN");
		reservedWords.add("BOTH");
		reservedWords.add("BREADTH");
		reservedWords.add("BY");
		reservedWords.add("C");
		reservedWords.add("CACHE");
		reservedWords.add("CALL");
		reservedWords.add("CALLED");
		reservedWords.add("CARDINALITY");
		reservedWords.add("CASCADE");
		reservedWords.add("CASCADED");
		reservedWords.add("CASE");
		reservedWords.add("CAST");
		reservedWords.add("CATALOG");
		reservedWords.add("CATALOG_NAME");
		reservedWords.add("CEIL");
		reservedWords.add("CEILING");
		reservedWords.add("CHAIN");
		reservedWords.add("CHAR");
		reservedWords.add("CHARACTER");
		reservedWords.add("CHARACTERISTICS");
		reservedWords.add("CHARACTERS");
		reservedWords.add("CHARACTER_LENGTH");
		reservedWords.add("CHARACTER_SET_CATALOG");
		reservedWords.add("CHARACTER_SET_NAME");
		reservedWords.add("CHARACTER_SET_SCHEMA");
		reservedWords.add("CHAR_LENGTH");
		reservedWords.add("CHECK");
		reservedWords.add("CHECKED");
		reservedWords.add("CHECKPOINT");
		reservedWords.add("CLASS");
		reservedWords.add("CLASS_ORIGIN");
		reservedWords.add("CLOB");
		reservedWords.add("CLOSE");
		reservedWords.add("CLUSTER");
		reservedWords.add("COALESCE");
		reservedWords.add("COBOL");
		reservedWords.add("COLLATE");
		reservedWords.add("COLLATION");
		reservedWords.add("COLLATION_CATALOG");
		reservedWords.add("COLLATION_NAME");
		reservedWords.add("COLLATION_SCHEMA");
		reservedWords.add("COLLECT");
		reservedWords.add("COLUMN");
		reservedWords.add("COLUMN_NAME");
		reservedWords.add("COMMAND_FUNCTION");
		reservedWords.add("COMMAND_FUNCTION_CODE");
		reservedWords.add("COMMENT");
		reservedWords.add("COMMIT");
		reservedWords.add("COMMITTED");
		reservedWords.add("COMPLETION");
		reservedWords.add("CONDITION");
		reservedWords.add("CONDITION_NUMBER");
		reservedWords.add("CONNECT");
		reservedWords.add("CONNECTION");
		reservedWords.add("CONNECTION_NAME");
		reservedWords.add("CONSTRAINT");
		reservedWords.add("CONSTRAINTS");
		reservedWords.add("CONSTRAINT_CATALOG");
		reservedWords.add("CONSTRAINT_NAME");
		reservedWords.add("CONSTRAINT_SCHEMA");
		reservedWords.add("CONSTRUCTOR");
		reservedWords.add("CONTAINS");
		reservedWords.add("CONTINUE");
		reservedWords.add("CONVERSION");
		reservedWords.add("CONVERT");
		reservedWords.add("COPY");
		reservedWords.add("CORR");
		reservedWords.add("CORRESPONDING");
		reservedWords.add("COUNT");
		reservedWords.add("COVAR_POP");
		reservedWords.add("COVAR_SAMP");
		reservedWords.add("CREATE");
		reservedWords.add("CREATEDB");
		reservedWords.add("CREATEROLE");
		reservedWords.add("CREATEUSER");
		reservedWords.add("CROSS");
		reservedWords.add("CSV");
		reservedWords.add("CUBE");
		reservedWords.add("CUME_DIST");
		reservedWords.add("CURRENT");
		reservedWords.add("CURRENT_DATE");
		reservedWords.add("CURRENT_DEFAULT_TRANSFORM_GROUP");
		reservedWords.add("CURRENT_PATH");
		reservedWords.add("CURRENT_ROLE");
		reservedWords.add("CURRENT_TIME");
		reservedWords.add("CURRENT_TIMESTAMP");
		reservedWords.add("CURRENT_TRANSFORM_GROUP_FOR_TYPE");
		reservedWords.add("CURRENT_USER");
		reservedWords.add("CURSOR");
		reservedWords.add("CURSOR_NAME");
		reservedWords.add("CYCLE");
		reservedWords.add("DATA");
		reservedWords.add("DATABASE");
		reservedWords.add("DATE");
		reservedWords.add("DATETIME_INTERVAL_CODE");
		reservedWords.add("DATETIME_INTERVAL_PRECISION");
		reservedWords.add("DAY");
		reservedWords.add("DEALLOCATE");
		reservedWords.add("DEC");
		reservedWords.add("DECIMAL");
		reservedWords.add("DECLARE");
		reservedWords.add("DEFAULT");
		reservedWords.add("DEFAULTS");
		reservedWords.add("DEFERRABLE");
		reservedWords.add("DEFERRED");
		reservedWords.add("DEFINED");
		reservedWords.add("DEFINER");
		reservedWords.add("DEGREE");
		reservedWords.add("DELETE");
		reservedWords.add("DELIMITER");
		reservedWords.add("DELIMITERS");
		reservedWords.add("DENSE_RANK");
		reservedWords.add("DEPTH");
		reservedWords.add("DEREF");
		reservedWords.add("DERIVED");
		reservedWords.add("DESC");
		reservedWords.add("DESCRIBE");
		reservedWords.add("DESCRIPTOR");
		reservedWords.add("DESTROY");
		reservedWords.add("DESTRUCTOR");
		reservedWords.add("DETERMINISTIC");
		reservedWords.add("DIAGNOSTICS");
		reservedWords.add("DICTIONARY");
		reservedWords.add("DISABLE");
		reservedWords.add("DISCONNECT");
		reservedWords.add("DISPATCH");
		reservedWords.add("DISTINCT");
		reservedWords.add("DO");
		reservedWords.add("DOMAIN");
		reservedWords.add("DOUBLE");
		reservedWords.add("DROP");
		reservedWords.add("DYNAMIC");
		reservedWords.add("DYNAMIC_FUNCTION");
		reservedWords.add("DYNAMIC_FUNCTION_CODE");
		reservedWords.add("EACH");
		reservedWords.add("ELEMENT");
		reservedWords.add("ELSE");
		reservedWords.add("ENABLE");
		reservedWords.add("ENCODING");
		reservedWords.add("ENCRYPTED");
		reservedWords.add("END");
		reservedWords.add("END-EXEC");
		reservedWords.add("EQUALS");
		reservedWords.add("ESCAPE");
		reservedWords.add("EVERY");
		reservedWords.add("EXCEPT");
		reservedWords.add("EXCEPTION");
		reservedWords.add("EXCLUDE");
		reservedWords.add("EXCLUDING");
		reservedWords.add("EXCLUSIVE");
		reservedWords.add("EXEC");
		reservedWords.add("EXECUTE");
		reservedWords.add("EXISTING");
		reservedWords.add("EXISTS");
		reservedWords.add("EXP");
		reservedWords.add("EXPLAIN");
		reservedWords.add("EXTERNAL");
		reservedWords.add("EXTRACT");
		reservedWords.add("FALSE");
		reservedWords.add("FETCH");
		reservedWords.add("FILTER");
		reservedWords.add("FINAL");
		reservedWords.add("FIRST");
		reservedWords.add("FLOAT");
		reservedWords.add("FLOOR");
		reservedWords.add("FOLLOWING");
		reservedWords.add("FOR");
		reservedWords.add("FORCE");
		reservedWords.add("FOREIGN");
		reservedWords.add("FORTRAN");
		reservedWords.add("FORWARD");
		reservedWords.add("FOUND");
		reservedWords.add("FREE");
		reservedWords.add("FREEZE");
		reservedWords.add("FROM");
		reservedWords.add("FULL");
		reservedWords.add("FUNCTION");
		reservedWords.add("FUSION");
		reservedWords.add("G");
		reservedWords.add("GENERAL");
		reservedWords.add("GENERATED");
		reservedWords.add("GET");
		reservedWords.add("GLOBAL");
		reservedWords.add("GO");
		reservedWords.add("GOTO");
		reservedWords.add("GRANT");
		reservedWords.add("GRANTED");
		reservedWords.add("GREATEST");
		reservedWords.add("GROUP");
		reservedWords.add("GROUPING");
		reservedWords.add("HANDLER");
		reservedWords.add("HAVING");
		reservedWords.add("HEADER");
		reservedWords.add("HIERARCHY");
		reservedWords.add("HOLD");
		reservedWords.add("HOST");
		reservedWords.add("HOUR");
		reservedWords.add("IDENTITY");
		reservedWords.add("IGNORE");
		reservedWords.add("ILIKE");
		reservedWords.add("IMMEDIATE");
		reservedWords.add("IMMUTABLE");
		reservedWords.add("IMPLEMENTATION");
		reservedWords.add("IMPLICIT");
		reservedWords.add("IN");
		reservedWords.add("INCLUDING");
		reservedWords.add("INCREMENT");
		reservedWords.add("INDEX");
		reservedWords.add("INDICATOR");
		reservedWords.add("INFIX");
		reservedWords.add("INHERIT");
		reservedWords.add("INHERITS");
		reservedWords.add("INITIALIZE");
		reservedWords.add("INITIALLY");
		reservedWords.add("INNER");
		reservedWords.add("INOUT");
		reservedWords.add("INPUT");
		reservedWords.add("INSENSITIVE");
		reservedWords.add("INSERT");
		reservedWords.add("INSTANCE");
		reservedWords.add("INSTANTIABLE");
		reservedWords.add("INSTEAD");
		reservedWords.add("INT");
		reservedWords.add("INTEGER");
		reservedWords.add("INTERSECT");
		reservedWords.add("INTERSECTION");
		reservedWords.add("INTERVAL");
		reservedWords.add("INTO");
		reservedWords.add("INVOKER");
		reservedWords.add("IS");
		reservedWords.add("ISNULL");
		reservedWords.add("ISOLATION");
		reservedWords.add("ITERATE");
		reservedWords.add("JOIN");
		reservedWords.add("K");
		reservedWords.add("KEY");
		reservedWords.add("KEY_MEMBER");
		reservedWords.add("KEY_TYPE");
		reservedWords.add("LANCOMPILER");
		reservedWords.add("LANGUAGE");
		reservedWords.add("LARGE");
		reservedWords.add("LAST");
		reservedWords.add("LATERAL");
		reservedWords.add("LEADING");
		reservedWords.add("LEAST");
		reservedWords.add("LEFT");
		reservedWords.add("LENGTH");
		reservedWords.add("LESS");
		reservedWords.add("LEVEL");
		reservedWords.add("LIKE");
		reservedWords.add("LIMIT");
		reservedWords.add("LISTEN");
		reservedWords.add("LN");
		reservedWords.add("LOAD");
		reservedWords.add("LOCAL");
		reservedWords.add("LOCALTIME");
		reservedWords.add("LOCALTIMESTAMP");
		reservedWords.add("LOCATION");
		reservedWords.add("LOCATOR");
		reservedWords.add("LOCK");
		reservedWords.add("LOGIN");
		reservedWords.add("LOWER");
		reservedWords.add("M");
		reservedWords.add("MAP");
		reservedWords.add("MATCH");
		reservedWords.add("MATCHED");
		reservedWords.add("MAX");
		reservedWords.add("MAXVALUE");
		reservedWords.add("MEMBER");
		reservedWords.add("MERGE");
		reservedWords.add("MESSAGE_LENGTH");
		reservedWords.add("MESSAGE_OCTET_LENGTH");
		reservedWords.add("MESSAGE_TEXT");
		reservedWords.add("METHOD");
		reservedWords.add("MIN");
		reservedWords.add("MINUTE");
		reservedWords.add("MINVALUE");
		reservedWords.add("MOD");
		reservedWords.add("MODE");
		reservedWords.add("MODIFIES");
		reservedWords.add("MODIFY");
		reservedWords.add("MODULE");
		reservedWords.add("MONTH");
		reservedWords.add("MORE");
		reservedWords.add("MOVE");
		reservedWords.add("MULTISET");
		reservedWords.add("MUMPS");
		reservedWords.add("NAME");
		reservedWords.add("NAMES");
		reservedWords.add("NATIONAL");
		reservedWords.add("NATURAL");
		reservedWords.add("NCHAR");
		reservedWords.add("NCLOB");
		reservedWords.add("NESTING");
		reservedWords.add("NEW");
		reservedWords.add("NEXT");
		reservedWords.add("NO");
		reservedWords.add("NOCREATEDB");
		reservedWords.add("NOCREATEROLE");
		reservedWords.add("NOCREATEUSER");
		reservedWords.add("NOINHERIT");
		reservedWords.add("NOLOGIN");
		reservedWords.add("NONE");
		reservedWords.add("NORMALIZE");
		reservedWords.add("NORMALIZED");
		reservedWords.add("NOSUPERUSER");
		reservedWords.add("NOT");
		reservedWords.add("NOTHING");
		reservedWords.add("NOTIFY");
		reservedWords.add("NOTNULL");
		reservedWords.add("NOWAIT");
		reservedWords.add("NULL");
		reservedWords.add("NULLABLE");
		reservedWords.add("NULLIF");
		reservedWords.add("NULLS");
		reservedWords.add("NUMBER");
		reservedWords.add("NUMERIC");
		reservedWords.add("OBJECT");
		reservedWords.add("OCTETS");
		reservedWords.add("OCTET_LENGTH");
		reservedWords.add("OF");
		reservedWords.add("OFF");
		reservedWords.add("OFFSET");
		reservedWords.add("OIDS");
		reservedWords.add("OLD");
		reservedWords.add("ON");
		reservedWords.add("ONLY");
		reservedWords.add("OPEN");
		reservedWords.add("OPERATION");
		reservedWords.add("OPERATOR");
		reservedWords.add("OPTION");
		reservedWords.add("OPTIONS");
		reservedWords.add("OR");
		reservedWords.add("ORDER");
		reservedWords.add("ORDERING");
		reservedWords.add("ORDINALITY");
		reservedWords.add("OTHERS");
		reservedWords.add("OUT");
		reservedWords.add("OUTER");
		reservedWords.add("OUTPUT");
		reservedWords.add("OVER");
		reservedWords.add("OVERLAPS");
		reservedWords.add("OVERLAY");
		reservedWords.add("OVERRIDING");
		reservedWords.add("OWNER");
		reservedWords.add("PAD");
		reservedWords.add("PARAMETER");
		reservedWords.add("PARAMETERS");
		reservedWords.add("PARAMETER_MODE");
		reservedWords.add("PARAMETER_NAME");
		reservedWords.add("PARAMETER_ORDINAL_POSITION");
		reservedWords.add("PARAMETER_SPECIFIC_CATALOG");
		reservedWords.add("PARAMETER_SPECIFIC_NAME");
		reservedWords.add("PARAMETER_SPECIFIC_SCHEMA");
		reservedWords.add("PARTIAL");
		reservedWords.add("PARTITION");
		reservedWords.add("PASCAL");
		reservedWords.add("PASSWORD");
		reservedWords.add("PATH");
		reservedWords.add("PERCENTILE_CONT");
		reservedWords.add("PERCENTILE_DISC");
		reservedWords.add("PERCENT_RANK");
		reservedWords.add("PLACING");
		reservedWords.add("PLI");
		reservedWords.add("POSITION");
		reservedWords.add("POSTFIX");
		reservedWords.add("POWER");
		reservedWords.add("PRECEDING");
		reservedWords.add("PRECISION");
		reservedWords.add("PREFIX");
		reservedWords.add("PREORDER");
		reservedWords.add("PREPARE");
		reservedWords.add("PREPARED");
		reservedWords.add("PRESERVE");
		reservedWords.add("PRIMARY");
		reservedWords.add("PRIOR");
		reservedWords.add("PRIVILEGES");
		reservedWords.add("PROCEDURAL");
		reservedWords.add("PROCEDURE");
		reservedWords.add("PUBLIC");
		reservedWords.add("QUOTE");
		reservedWords.add("RANGE");
		reservedWords.add("RANK");
		reservedWords.add("READ");
		reservedWords.add("READS");
		reservedWords.add("REAL");
		reservedWords.add("RECHECK");
		reservedWords.add("RECURSIVE");
		reservedWords.add("REF");
		reservedWords.add("REFERENCES");
		reservedWords.add("REFERENCING");
		reservedWords.add("REGR_AVGX");
		reservedWords.add("REGR_AVGY");
		reservedWords.add("REGR_COUNT");
		reservedWords.add("REGR_INTERCEPT");
		reservedWords.add("REGR_R2");
		reservedWords.add("REGR_SLOPE");
		reservedWords.add("REGR_SXX");
		reservedWords.add("REGR_SXY");
		reservedWords.add("REGR_SYY");
		reservedWords.add("REINDEX");
		reservedWords.add("RELATIVE");
		reservedWords.add("RELEASE");
		reservedWords.add("RENAME");
		reservedWords.add("REPEATABLE");
		reservedWords.add("REPLACE");
		reservedWords.add("RESET");
		reservedWords.add("RESTART");
		reservedWords.add("RESTRICT");
		reservedWords.add("RESULT");
		reservedWords.add("RETURN");
		reservedWords.add("RETURNED_CARDINALITY");
		reservedWords.add("RETURNED_LENGTH");
		reservedWords.add("RETURNED_OCTET_LENGTH");
		reservedWords.add("RETURNED_SQLSTATE");
		reservedWords.add("RETURNS");
		reservedWords.add("REVOKE");
		reservedWords.add("RIGHT");
		reservedWords.add("ROLE");
		reservedWords.add("ROLLBACK");
		reservedWords.add("ROLLUP");
		reservedWords.add("ROUTINE");
		reservedWords.add("ROUTINE_CATALOG");
		reservedWords.add("ROUTINE_NAME");
		reservedWords.add("ROUTINE_SCHEMA");
		reservedWords.add("ROW");
		reservedWords.add("ROWS");
		reservedWords.add("ROW_COUNT");
		reservedWords.add("ROW_NUMBER");
		reservedWords.add("RULE");
		reservedWords.add("SAVEPOINT");
		reservedWords.add("SCALE");
		reservedWords.add("SCHEMA");
		reservedWords.add("SCHEMA_NAME");
		reservedWords.add("SCOPE");
		reservedWords.add("SCOPE_CATALOG");
		reservedWords.add("SCOPE_NAME");
		reservedWords.add("SCOPE_SCHEMA");
		reservedWords.add("SCROLL");
		reservedWords.add("SEARCH");
		reservedWords.add("SECOND");
		reservedWords.add("SECTION");
		reservedWords.add("SECURITY");
		reservedWords.add("SELECT");
		reservedWords.add("SELF");
		reservedWords.add("SENSITIVE");
		reservedWords.add("SEQUENCE");
		reservedWords.add("SERIALIZABLE");
		reservedWords.add("SERVER_NAME");
		reservedWords.add("SESSION");
		reservedWords.add("SESSION_USER");
		reservedWords.add("SET");
		reservedWords.add("SETOF");
		reservedWords.add("SETS");
		reservedWords.add("SHARE");
		reservedWords.add("SHOW");
		reservedWords.add("SIMILAR");
		reservedWords.add("SIMPLE");
		reservedWords.add("SIZE");
		reservedWords.add("SMALLINT");
		reservedWords.add("SOME");
		reservedWords.add("SOURCE");
		reservedWords.add("SPACE");
		reservedWords.add("SPECIFIC");
		reservedWords.add("SPECIFICTYPE");
		reservedWords.add("SPECIFIC_NAME");
		reservedWords.add("SQL");
		reservedWords.add("SQLCODE");
		reservedWords.add("SQLERROR");
		reservedWords.add("SQLEXCEPTION");
		reservedWords.add("SQLSTATE");
		reservedWords.add("SQLWARNING");
		reservedWords.add("SQRT");
		reservedWords.add("STABLE");
		reservedWords.add("START");
		reservedWords.add("STATE");
		reservedWords.add("STATEMENT");
		reservedWords.add("STATIC");
		reservedWords.add("STATISTICS");
		reservedWords.add("STDDEV_POP");
		reservedWords.add("STDDEV_SAMP");
		reservedWords.add("STDIN");
		reservedWords.add("STDOUT");
		reservedWords.add("STORAGE");
		reservedWords.add("STRICT");
		reservedWords.add("STRUCTURE");
		reservedWords.add("STYLE");
		reservedWords.add("SUBCLASS_ORIGIN");
		reservedWords.add("SUBLIST");
		reservedWords.add("SUBMULTISET");
		reservedWords.add("SUBSTRING");
		reservedWords.add("SUM");
		reservedWords.add("SUPERUSER");
		reservedWords.add("SYMMETRIC");
		reservedWords.add("SYSID");
		reservedWords.add("SYSTEM");
		reservedWords.add("SYSTEM_USER");
		reservedWords.add("TABLE");
		reservedWords.add("TABLESAMPLE");
		reservedWords.add("TABLESPACE");
		reservedWords.add("TABLE_NAME");
		reservedWords.add("TEMP");
		reservedWords.add("TEMPLATE");
		reservedWords.add("TEMPORARY");
		reservedWords.add("TERMINATE");
		reservedWords.add("THAN");
		reservedWords.add("THEN");
		reservedWords.add("TIES");
		reservedWords.add("TIME");
		reservedWords.add("TIMESTAMP");
		reservedWords.add("TIMEZONE_HOUR");
		reservedWords.add("TIMEZONE_MINUTE");
		reservedWords.add("TO");
		reservedWords.add("TOAST");
		reservedWords.add("TOP_LEVEL_COUNT");
		reservedWords.add("TRAILING");
		reservedWords.add("TRANSACTION");
		reservedWords.add("TRANSACTIONS_COMMITTED");
		reservedWords.add("TRANSACTIONS_ROLLED_BACK");
		reservedWords.add("TRANSACTION_ACTIVE");
		reservedWords.add("TRANSFORM");
		reservedWords.add("TRANSFORMS");
		reservedWords.add("TRANSLATE");
		reservedWords.add("TRANSLATION");
		reservedWords.add("TREAT");
		reservedWords.add("TRIGGER");
		reservedWords.add("TRIGGER_CATALOG");
		reservedWords.add("TRIGGER_NAME");
		reservedWords.add("TRIGGER_SCHEMA");
		reservedWords.add("TRIM");
		reservedWords.add("TRUE");
		reservedWords.add("TRUNCATE");
		reservedWords.add("TRUSTED");
		reservedWords.add("TYPE");
		reservedWords.add("UESCAPE");
		reservedWords.add("UNBOUNDED");
		reservedWords.add("UNCOMMITTED");
		reservedWords.add("UNDER");
		reservedWords.add("UNENCRYPTED");
		reservedWords.add("UNION");
		reservedWords.add("UNIQUE");
		reservedWords.add("UNKNOWN");
		reservedWords.add("UNLISTEN");
		reservedWords.add("UNNAMED");
		reservedWords.add("UNNEST");
		reservedWords.add("UNTIL");
		reservedWords.add("UPDATE");
		reservedWords.add("UPPER");
		reservedWords.add("USAGE");
		reservedWords.add("USER");
		reservedWords.add("USER_DEFINED_TYPE_CATALOG");
		reservedWords.add("USER_DEFINED_TYPE_CODE");
		reservedWords.add("USER_DEFINED_TYPE_NAME");
		reservedWords.add("USER_DEFINED_TYPE_SCHEMA");
		reservedWords.add("USING");
		reservedWords.add("VACUUM");
		reservedWords.add("VALID");
		reservedWords.add("VALIDATOR");
		reservedWords.add("VALUE");
		reservedWords.add("VALUES");
		reservedWords.add("VARCHAR");
		reservedWords.add("VARIABLE");
		reservedWords.add("VARYING");
		reservedWords.add("VAR_POP");
		reservedWords.add("VAR_SAMP");
		reservedWords.add("VERBOSE");
		reservedWords.add("VIEW");
		reservedWords.add("VOLATILE");
		reservedWords.add("WHEN");
		reservedWords.add("WHENEVER");
		reservedWords.add("WHERE");
		reservedWords.add("WIDTH_BUCKET");
		reservedWords.add("WINDOW");
		reservedWords.add("WITH");
		reservedWords.add("WITHIN");
		reservedWords.add("WITHOUT");
		reservedWords.add("WORK");
		reservedWords.add("WRITE");
		reservedWords.add("YEAR");
		reservedWords.add("ZONE");
	}
}
