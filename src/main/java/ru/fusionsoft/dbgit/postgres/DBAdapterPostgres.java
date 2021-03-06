package ru.fusionsoft.dbgit.postgres;


import java.sql.*;
import java.text.MessageFormat;
import java.time.Period;
import java.util.*;
import java.util.concurrent.TimeUnit;

import com.diogonunes.jcdp.color.api.Ansi;
import com.google.common.collect.ImmutableMap;
import java.util.stream.Collectors;
import org.apache.commons.lang3.exception.ExceptionUtils;
import ru.fusionsoft.dbgit.adapters.*;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import org.slf4j.Logger;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;
import ru.fusionsoft.dbgit.utils.StringProperties;


public class DBAdapterPostgres extends DBAdapter {
	private final Logger logger = LoggerUtil.getLogger(this.getClass());
	private final FactoryDBAdapterRestorePostgres restoreFactory = new FactoryDBAdapterRestorePostgres();
	private final FactoryDbConvertAdapterPostgres convertFactory = new FactoryDbConvertAdapterPostgres();
	private final FactoryDBBackupAdapterPostgres backupFactory = new FactoryDBBackupAdapterPostgres();
	private final static Set<String> reservedWords = new HashSet<>();

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
		return new TreeMapMetaObject(Collections.emptyList());
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		final Map<String, DBSchema> listScheme = new HashMap<String, DBSchema>();
		final String query =
			"select nspname,usename,nspacl from pg_namespace,pg_user where nspname!='pg_toast' and nspname!='pg_temp_1'"+
			"and nspname!='pg_toast_temp_1' and nspname!='pg_catalog'"+
			"and nspname!='information_schema' and nspname!='pgagent'"+
			"and nspname!='pg_temp_3' and nspname!='pg_toast_temp_3'"+
			"and usesysid = nspowner";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("nspname");
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
			"SELECT tblspaces.spcname,tblspaces.spcacl,tblspaces.spcoptions,users.usename,pg_tablespace_location(tblspacesoid.oid) " +
			"FROM pg_tablespace as tblspaces,pg_user as users,(Select oid FROM pg_tablespace where spcname!='pg_default' and spcname!='pg_global') as tblspacesoid " +
			"WHERE users.usesysid=tblspaces.spcowner and spcname!='pg_default' and spcname!='pg_global' and tblspacesoid.oid=tblspaces.oid";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("spcname");
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
		final Map<String, DBSequence> listSequence = new HashMap<>();
		final String query = MessageFormat.format(
			"select s.sequence_name, rol.rolname as owner, s.start_value, s.minimum_value, s.maximum_value, s.increment, s.cycle_option, cl.relname as blocking_table   \n" +
			"from pg_class cls \n" +
			"  join pg_roles rol on rol.oid = cls.relowner  \n" +
			"  join pg_namespace nsp on nsp.oid = cls.relnamespace \n" +
			"  join information_schema.sequences s on cls.relname = s.sequence_name \n" +
			"  left join pg_depend d on d.objid=cls.oid and d.classid=''pg_class''::regclass and d.refclassid=''pg_class''::regclass\n" +
			"  left join pg_class cl on cl.oid = d.refobjid and d.deptype=''a''  \n" +
			"where nsp.nspname not in (''information_schema'', ''pg_catalog'')\n" +
			"  and nsp.nspname not like ''pg_toast%'' \n" +
			"  and cls.relkind = ''S'' and s.sequence_schema = ''{0}''",
			schema
		);

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				final String nameSeq = rs.getString("sequence_name");
				final String ownerSeq = rs.getString("blocking_table");
				final Long valueSeq = 0L;
				//TODO find actual value

				final DBSequence sequence = new DBSequence(nameSeq, new StringProperties(rs), schema, ownerSeq, Collections.emptySet(), valueSeq);
				listSequence.put(nameSeq, sequence);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "sequence").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listSequence;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {

		final String query =
			"select s.sequence_name, rol.rolname as owner, s.start_value, s.minimum_value, s.maximum_value, s.increment, s.cycle_option, cl.relname as blocking_table   \n" +
			"from pg_class cls \n" +
			"  join pg_roles rol on rol.oid = cls.relowner  \n" +
			"  join pg_namespace nsp on nsp.oid = cls.relnamespace \n" +
			"  join information_schema.sequences s on cls.relname = s.sequence_name \n" +
			"  left join pg_depend d on d.objid=cls.oid and d.classid='pg_class'::regclass and d.refclassid='pg_class'::regclass\n" +
			"  left join  pg_class cl on cl.oid = d.refobjid and d.deptype='a'  \n" +
			"where nsp.nspname not in ('information_schema', 'pg_catalog') \n" +
			"  and nsp.nspname not like 'pg_toast%' \n" +
			"  and cls.relkind = 'S' and s.sequence_schema = :schema and s.sequence_name = :name ";

		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema, "name", name));
			ResultSet rs = stmt.executeQuery();
		) {
			if (rs.next()) {
				//TODO find actual value
				final Long valueSeq = 0L;
				final String nameSeq = rs.getString("sequence_name");
				final String ownerSeq = rs.getString("blocking_table");
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
			"SELECT \n" +
			"	tablename AS table_name,\n" +
			"	tableowner AS owner,\n" +
			"	tablespace, hasindexes, hasrules, hastriggers, \n" +
			"	obj_description( " + 
			"		(('\"' || schemaname || '\".\"' || tablename || '\"')::regclass)::oid" + 
			"	) AS table_comment,\n" +
			"	(\n" +
			"		SELECT array_agg( distinct n2.nspname || '/' || c2.relname || '.tbl' ) AS dependencies\n" +
			"	 	FROM pg_catalog.pg_constraint c  \n" +
			"		JOIN ONLY pg_catalog.pg_class c1     ON c1.oid = c.conrelid\n" +
			"		JOIN ONLY pg_catalog.pg_class c2     ON c2.oid = c.confrelid\n" +
			"		JOIN ONLY pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace\n" +
			"		WHERE c.conrelid = (('\"' || schemaname || '\".\"' || tablename || '\"')::regclass)::oid\n" +
			"		and c1.relkind = 'r' AND c.contype = 'f'\n" +
			"	) " +
			"	AS dependencies, \n" +
			( (getDbVersionNumber() >= 10)
					? 	"   pg_get_partkeydef((\n" +
					"		SELECT oid \n" +
					"		FROM pg_class \n" +
					"		WHERE relname = tablename \n" +
					"		AND relnamespace = (select oid from pg_namespace where nspname = :schema )\n" +
					"	)) \n" +
					"   AS partkeydef, \n" +
					"  	pg_get_expr(child.relpartbound, child.oid) " +
					"	AS pg_get_expr, \n"
					: 	" "
			) +
			"   parent.relname AS parent \n" +
			"FROM pg_tables \n" +
			"LEFT OUTER JOIN pg_inherits on (SELECT oid FROM pg_class WHERE relname = tablename and relnamespace = (select oid from pg_namespace where nspname = :schema)) = pg_inherits.inhrelid \n" +
			"LEFT OUTER JOIN pg_class parent ON pg_inherits.inhparent = parent.oid \n" +
			"LEFT OUTER JOIN pg_class child ON pg_inherits.inhrelid = child.oid \n" +
			"WHERE upper(schemaname) = upper(:schema)";

		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema));
			ResultSet rs = stmt.executeQuery();
		) {


			while(rs.next()){
				final String nameTable = rs.getString("table_name");
				final String ownerTable = rs.getString("owner");
				final String commentTable = rs.getString("table_comment");
				final Set<String> dependencies = rs.getArray("dependencies") != null
					? new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()))
					: Collections.emptySet();

				if (rs.getString("parent") != null) {
					dependencies.add(schema + "/" + rs.getString("parent") + ".tbl");
				}

				final DBTable table = new DBTable(nameTable, new StringProperties(rs), schema, ownerTable, dependencies, commentTable);
				listTable.put(nameTable, table);
			}
		} catch (Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tables").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
		return listTable;
	}

	@Override
	public DBTable getTable(String schema, String name) {
		final String query =
			"SELECT \n" +
			"	tablename AS table_name,\n" +
			"	tableowner AS owner,\n" +
			"	tablespace, hasindexes, hasrules, hastriggers, \n" +
			"	obj_description((('\"' || schemaname || '\".\"' || tablename || '\"')::regclass)::oid) AS table_comment,\n" +
			"	(\n" +
			"		SELECT array_agg( distinct n2.nspname || '/' || c2.relname || '.tbl' ) AS dependencies\n" +
			"	 	FROM pg_catalog.pg_constraint c  \n" +
			"		JOIN ONLY pg_catalog.pg_class c1     ON c1.oid = c.conrelid\n" +
			"		JOIN ONLY pg_catalog.pg_class c2     ON c2.oid = c.confrelid\n" +
			"		JOIN ONLY pg_catalog.pg_namespace n2 ON n2.oid = c2.relnamespace\n" +
			"		WHERE c.conrelid = (('\"' || schemaname || '\".\"' || tablename || '\"')::regclass)::oid\n" +
			"		and c1.relkind = 'r' AND c.contype = 'f'\n" +
			"	) " +
			"	AS dependencies, \n" +
			( (getDbVersionNumber() >= 10)
					? 	"   pg_get_partkeydef((\n" +
					"		SELECT oid \n" +
					"		FROM pg_class \n" +
					"		WHERE relname = tablename \n" +
					"		AND relnamespace = (select oid from pg_namespace where nspname = :schema )\n" +
					"	)) \n" +
					"   AS partkeydef, \n" +
					"  	pg_get_expr(child.relpartbound, child.oid) " +
					"	AS pg_get_expr, \n"
					: 	" "
			) +
			"   parent.relname AS parent \n" +
			"FROM pg_tables \n" +
			"LEFT OUTER JOIN pg_inherits on (SELECT oid FROM pg_class WHERE relname = tablename and relnamespace = (select oid from pg_namespace where nspname = :schema)) = pg_inherits.inhrelid \n" +
			"LEFT OUTER JOIN pg_class parent ON pg_inherits.inhparent = parent.oid \n" +
			"LEFT OUTER JOIN pg_class child ON pg_inherits.inhrelid = child.oid \n" +
			"WHERE upper(schemaname) = upper(:schema)" +
			"AND tablename = :name";
		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema, "name", name));
			ResultSet rs = stmt.executeQuery();
		) {

			if (rs.next()) {
				final String nameTable = rs.getString("table_name");
				final String ownerTable = rs.getString("owner");
				final String commentTable = rs.getString("table_comment");
				final Set<String> dependencies = rs.getArray("dependencies") != null
					? new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()))
					: Collections.emptySet();

				if (rs.getString("parent") != null) {
					dependencies.add(schema + "/" + rs.getString("parent") + ".tbl");
				}

				return new DBTable(nameTable, new StringProperties(rs), schema, ownerTable, dependencies, commentTable);

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
	public Map<String, DBTableField>  getTableFields(String schema, String nameTable) {
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
			"where upper(col.table_schema) = upper(:schema) and col.table_name = :table " +
			"order by col.column_name ";

		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema, "table", nameTable));
			ResultSet rs = stmt.executeQuery();
		) {


			while(rs.next()){
				final String typeSQL = getFieldType(rs);
				final String nameField = rs.getString("column_name");
				final String descField = rs.getString("description");
				final String columnDefault = rs.getString("column_default");
				final boolean isFixed = rs.getBoolean("fixed");
				final boolean isPrimaryKey = rs.getString("constraint_name") != null;
				final boolean isNullable = !typeSQL.toLowerCase().contains("not null");
				final FieldType typeUniversal = FieldType.fromString(rs.getString("tp"));
//				System.out.println((
//					nameField
//					+ " > "
//					+ typeSQL
//					+ " ("
//					+ typeUniversal.toString()
//					+ ")"
//				));
				final FieldType actualTypeUniversal = typeUniversal.equals(FieldType.TEXT) ? FieldType.STRING_NATIVE : typeUniversal;
				final int length = rs.getInt("character_maximum_length");
				final int precision = rs.getInt("numeric_precision");
				final int scale = rs.getInt("numeric_scale");
				final int ordinalPosition = rs.getInt("ordinal_position");


				final DBTableField field = new DBTableField(
					nameField, descField, isPrimaryKey, isNullable,
					typeSQL, actualTypeUniversal, ordinalPosition, columnDefault,
					length, scale, precision, isFixed
				);

				listField.put(nameField, field);
			}


		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listField;
	}

	private String getFieldType(ResultSet rs) {
		try {
			final StringBuilder type = new StringBuilder();
			type.append(rs.getString("dtype"));

			Integer max_length = rs.getInt("character_maximum_length");
			if (!rs.wasNull()) {
				type.append("("+max_length.toString()+")");
			}
			if (rs.getString("is_nullable").equals("NO")){
				type.append(" NOT NULL");
			}

			return type.toString();
		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tables").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		final Map<String, DBIndex> indexes = new HashMap<>();
		final String query =
			"SELECT " +
			"	i.schemaname,\r\n" +
			"	i.tablename, \r\n" +
			"	i.indexname, \r\n" +
			"	i.tablespace, \r\n" +
			"	i.indexdef AS ddl, \r\n" +
			"	t.tableowner AS owner \r\n" +
			"FROM pg_indexes AS i " +
			"JOIN pg_class AS cl ON i.indexname = cl.relname\r\n" +
			"JOIN pg_index AS idx ON cl.oid = idx.indexrelid\r\n" +
			"JOIN pg_tables AS t ON i.schemaname = t.schemaname AND i.tablename = t.tablename\r\n" +
			"WHERE i.tablename not like 'pg%' " +
			"AND i.schemaname = :schema " +
			"AND i.tablename = :table and idx.indisprimary = false " +
			"-- AND idx.indisunique=false ";

		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema, "table", nameTable));
			ResultSet rs = stmt.executeQuery();
		){

			while(rs.next()){
				final String name = rs.getString("indexname");
				final String owner = rs.getString("owner");
				final String ddl = rs.getString("ddl");

				final DBIndex index = new DBIndex(name, new StringProperties(rs), schema, owner, Collections.emptySet(), ddl);
				indexes.put(name, index);
			}

			return indexes;

		} catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "indexes").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		final Map<String, DBConstraint> constraints = new HashMap<>();
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

		final String query =
			"SELECT " +
			"	t.tableowner as owner," +
			"	conname as constraint_name," +
			"	contype as constraint_type, \r\n" +
			"  	pg_catalog.pg_get_constraintdef(con.oid, true) as ddl\r\n" +
			"FROM pg_catalog.pg_constraint con\r\n" +
			"INNER JOIN pg_catalog.pg_class rel ON rel.oid = con.conrelid\r\n" +
			"INNER JOIN pg_catalog.pg_namespace nsp ON nsp.oid = connamespace\r\n" +
			"INNER JOIN pg_tables t ON nsp.nspname = t.schemaname AND rel.relname = t.tablename\r\n" +
			"WHERE nsp.nspname = :schema\r\n" +
			"AND rel.relname = :table";

		try (
			PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("schema", schema, "table", nameTable));
			ResultSet rs = stmt.executeQuery();
		){

			while(rs.next()){
				final String name = rs.getString("constraint_name");
				final String type = rs.getString("constraint_type");
				final String owner = rs.getString("owner");
				final String ddl = rs.getString("ddl");
				final StringProperties options = new StringProperties(rs);
				final DBConstraint con = new DBConstraint(name, options, schema, owner, Collections.emptySet(), ddl, type);

				constraints.put(con.getName(), con);
			}

			return constraints;

		} catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "constraints").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		final Map<String, DBView> listView = new HashMap<String, DBView>();
		final String query =
			"select nsp.nspname as object_schema, cls.relname as object_name,  rol.rolname as owner, \n" +
			"'create or replace view ' || nsp.nspname || '.' || cls.relname || ' as ' || pg_get_viewdef(cls.oid) as ddl, (\n" +
			"	select array_agg(distinct source_ns.nspname || '/' || source_table.relname || '.vw') as dependencySam\n" +
			"	from pg_depend \n" +
			"	join pg_rewrite ON pg_depend.objid = pg_rewrite.oid \n" +
			"	join pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid \n" +
			"	join pg_class as source_table ON pg_depend.refobjid = source_table.oid AND source_table.relkind = 'v'\n" +
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
			"and cls.relname not in ('pg_buffercache', 'pg_stat_statements') \n" +
			"and nsp.nspname not like 'pg_toast%' \n" +
			"and cls.relkind = 'v' \n" +
			"and nsp.nspname = '"+schema+"' \n";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String objectName = rs.getString("object_name");
				final String objectSchema = rs.getString("object_schema");
				final String owner = rs.getString("owner");
				final String ddl = rs.getString("ddl");
				final StringProperties options = new StringProperties(rs);
				final Set<String> dependencies = rs.getArray("dependencies") == null
					? Collections.emptySet()
					: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				DBView view = new DBView(objectName, options, objectSchema, owner, dependencies, ddl);
				listView.put(rs.getString("object_name"), view);
			}

		} catch (Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "views");
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listView;
	}

	@Override
	public DBView getView(String schema, String name) {

		String query =
			"select nsp.nspname as object_schema, cls.relname as object_name,  rol.rolname as owner, \n" +
			"'create or replace view ' || nsp.nspname || '.' || cls.relname || ' as ' || pg_get_viewdef(cls.oid) as ddl, (\n" +
			"	select array_agg(distinct source_ns.nspname || '/' || source_table.relname || '.vw') as dependencySam\n" +
			"	from pg_depend \n" +
			"	join pg_rewrite ON pg_depend.objid = pg_rewrite.oid \n" +
			"	join pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid \n" +
			"	join pg_class as source_table ON pg_depend.refobjid = source_table.oid AND source_table.relkind = 'v'\n" +
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




		try(Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			if (rs.next()) {
				String owner = rs.getString("owner");
				String ddl = rs.getString("ddl");
				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = rs.getArray("dependencies") == null
					? Collections.emptySet()
					: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				return new DBView(name, options, schema, owner, dependencies, ddl);

			} else {
				String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			DBGitLang msg = lang.getValue("errors", "adapter", "views");
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		String query =
			"SELECT trg.tgname, tbl.relname as trigger_table ,pg_get_triggerdef(trg.oid) AS ddl \r\n" +
			"FROM pg_trigger trg\r\n" +
			"JOIN pg_class tbl on trg.tgrelid = tbl.oid\r\n" +
			"JOIN pg_namespace ns ON ns.oid = tbl.relnamespace\r\n" +
			"and trg.tgconstraint=0 and ns.nspname like \'"+schema+"\'";

		try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		) {

			while(rs.next()){
				String name = rs.getString("tgname");
				String owner = "postgres";
				String sql = rs.getString("ddl");
				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = Collections.emptySet(); 
//					rs.getArray("dependencies") == null
//					? Collections.emptySet()
//					: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				DBTrigger trigger = new DBTrigger(name, options, schema, owner, dependencies, sql);
				listTrigger.put(name, trigger);
			}

		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);
		}

		return listTrigger;
	}
	@Override
	public DBTrigger getTrigger(String schema, String name) {
		String query =
			"SELECT trg.tgname, tbl.relname as trigger_table ,pg_get_triggerdef(trg.oid) AS ddl \r\n" +
			"FROM pg_trigger trg\r\n" +
			"JOIN pg_class tbl on trg.tgrelid = tbl.oid\r\n" +
			"JOIN pg_namespace ns ON ns.oid = tbl.relnamespace\r\n" +
			"AND trg.tgconstraint=0 and ns.nspname like \'"+schema+"\' and trg.tgname like \'"+name+"\'";

		try (Statement stmt = getConnection().createStatement();ResultSet rs = stmt.executeQuery(query);){

			if(rs.next()){
				String sql = rs.getString("ddl");
				String owner = "postgres";

				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = rs.getArray("dependencies") == null
					? Collections.emptySet()
					: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				return new DBTrigger(name, options, schema, owner, dependencies, sql);

			} else {
				String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);
		}

	}
	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		return Collections.emptyMap();
	}

	@Override
	public DBPackage getPackage(String schema, String name)  {
		throw new ExceptionDBGitRunTime(new ExceptionDBGitObjectNotFound("cannot get packages on postgres"));

	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		Map<String, DBProcedure> mapProcs = new HashMap<>();
		String query =
			"SELECT n.nspname AS \"schema\", u.rolname, p.proname AS \"name\", \n" +
			"	pg_catalog.pg_get_function_arguments(p.oid) AS \"arguments\",\n" +
			"	pg_get_functiondef(p.oid) AS ddl\n" +
			"FROM pg_catalog.pg_proc p\n" +
			"	JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\n" +
			"	LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n" +
		( (getDbVersionNumber() >= 10)
			? 	"WHERE p.prokind = 'p' \n"
			:	"WHERE 1=0 \n"
		) +
			"	AND n.nspname not in('pg_catalog', 'information_schema')\n" +
			"	AND n.nspname = '"+schema+"'";

		try (Statement stmt = getConnection().createStatement();ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				String name = rs.getString("name");
				String owner = rs.getString("rolname");
				String sql = rs.getString("ddl");
				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = rs.getArray("dependencies") == null
					? Collections.emptySet()
					: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				DBProcedure proc = new DBProcedure(name, options, schema, owner, dependencies, sql);

				String nameInMap = mapProcs.containsKey(name) ? name + "_" + proc.getHash() : name;
				mapProcs.put(nameInMap, proc);
			}

		} catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "prc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return mapProcs;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		String query =
			"SELECT n.nspname AS \"schema\", u.rolname, p.proname AS \"name\", \n" +
			"	pg_catalog.pg_get_function_arguments(p.oid) AS \"arguments\",\n" +
			"	pg_get_functiondef(p.oid) AS ddl\n" +
			"FROM pg_catalog.pg_proc p\n" +
			"	JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\n" +
			"	LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n" +
			( (getDbVersionNumber() >= 10)
					? "WHERE p.prokind = 'p' \n"
					: "WHERE 1=0 \n"
			) +
			"	AND n.nspname not in('pg_catalog', 'information_schema')\n" +
			"	AND n.nspname = '"+schema+"'" +
			"	AND p.proname = '"+name+"'";

		try (Statement stmt = getConnection().createStatement();ResultSet rs = stmt.executeQuery(query);){

			if(rs.next()){
				String owner = rs.getString("rolname");
				String sql = rs.getString("ddl");
				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = rs.getArray("dependencies") == null
						? Collections.emptySet()
						: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				return new DBProcedure(name, options, schema, owner, dependencies, sql);

			} else {
				String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}
		} catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "prc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		Map<String, DBFunction> listFunction = new HashMap<String, DBFunction>();
		String query =
			"SELECT n.nspname AS \"schema\", u.rolname, p.proname AS \"name\", \n" +
			"	pg_catalog.pg_get_function_arguments(p.oid) AS \"arguments\",\n" +
			"	pg_get_functiondef(p.oid) AS ddl\n" +
			"FROM pg_catalog.pg_proc p\n" +
			"	JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\n" +
			"	LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n" +
			( (getDbVersionNumber() >= 10)
					? "WHERE p.prokind = 'f' \n"
					: "WHERE p.proisagg is false "
			)+
			"AND n.nspname not in('pg_catalog', 'information_schema')\n" +
			"AND n.nspname = '"+schema+"'";

		try (Statement stmt = getConnection().createStatement();ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				String name = rs.getString("name");
				String owner = rs.getString("rolname");
				String sql = rs.getString("ddl");
				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = Collections.emptySet();
//					rs.getArray("dependencies") == null
//						? Collections.emptySet()
//						: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				DBFunction dbFunction = new DBFunction(name, options, schema, owner, dependencies, sql);

				String nameInMap = listFunction.containsKey(name) ? name + "_" + dbFunction.getHash() : name;
				listFunction.put(nameInMap, dbFunction);
			}

		}catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "fnc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		String query =
			"SELECT n.nspname AS \"schema\", u.rolname, p.proname AS \"name\", \n" +
			"	pg_catalog.pg_get_function_arguments(p.oid) AS \"arguments\",\n" +
			"	pg_get_functiondef(p.oid) AS ddl\n" +
			"FROM pg_catalog.pg_proc p\n" +
			"	JOIN pg_catalog.pg_roles u ON u.oid = p.proowner\n" +
			"	LEFT JOIN pg_catalog.pg_namespace n ON n.oid = p.pronamespace\n" +
			( (getDbVersionNumber() >= 10)
					? "WHERE p.prokind = 'f' \n"
					: "WHERE 1=1 \n"
			) +
			"AND n.nspname not in('pg_catalog', 'information_schema')\n" +
			"AND n.nspname = '"+schema+"' AND p.proname = '"+name+"'";

		try (Statement stmt = getConnection().createStatement();ResultSet rs = stmt.executeQuery(query);){


			if (rs.next()) {
				String owner = rs.getString("rolname");
				String sql = rs.getString("ddl");
				StringProperties options = new StringProperties(rs);
				Set<String> dependencies = rs.getArray("dependencies") == null
						? Collections.emptySet()
						: new HashSet<>(Arrays.asList((String[])rs.getArray("dependencies").getArray()));

				return new DBFunction(name, options, schema, owner, dependencies, sql);
				//String args = rs.getString("arguments");
				//func.setArguments(args);

			} else {
				String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}


		} catch(Exception e) {
			String msg = lang.getValue("errors", "adapter", "fnc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		final DBGitConfig config = DBGitConfig.getInstance();
		try {
			final boolean isFetchLimitedDefault = config.getBooleanGlobal("core", "LIMIT_FETCH", true);
			final boolean isFetchLimited        = config.getBoolean("core", "LIMIT_FETCH", isFetchLimitedDefault);
			final int maxRowsCountDefault       = config.getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH);
			final int maxRowsCount              = config.getInteger("core", "MAX_ROW_COUNT_FETCH", maxRowsCountDefault);
			final int portionSize 				= config.getInteger("core", "PORTION_SIZE", config.getIntegerGlobal("core", "PORTION_SIZE", 1000));
			final int portionBegin = 1 + portionSize*portionIndex;
			final int portionEnd = portionSize + portionSize*portionIndex;

			final String tableRowsCountQuery =
				"select COALESCE(count(*), 0) kolvo " +
				"from ( " +
				"	select 1 from " + escapeNameIfNeeded(schema) + "." + escapeNameIfNeeded(nameTable) +
				" 	limit " + (maxRowsCount + 1) + " " +
				") tbl";

			final String dataQuery =
				"    SELECT * FROM \r\n" +
				"   (SELECT f.*, ROW_NUMBER() OVER (ORDER BY ctid) DBGIT_ROW_NUM FROM " + escapeNameIfNeeded(schema) + "." + escapeNameIfNeeded(nameTable) + " f) s\r\n" +
				"   WHERE DBGIT_ROW_NUM BETWEEN " + portionBegin  + " and " + portionEnd;


			if (isFetchLimited) {
				try(Statement st = getConnection().createStatement(); ResultSet rs = st.executeQuery(tableRowsCountQuery);){
					if(!rs.next()) {
						String msg = "error fetch table rows count";
						throw new ExceptionDBGitRunTime(msg);
					} else if (rs.getInt("kolvo") > maxRowsCount) {
						return new DBTableData(DBTableData.ERROR_LIMIT_ROWS);
					}
				}

			}

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
		final String tableName = escapeNameIfNeeded(schema)+"."+ escapeNameIfNeeded(nameTable);
		final int maxRowsCount = DBGitConfig.getInstance().getInteger("core", "MAX_ROW_COUNT_FETCH", DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH));
		final Boolean isFetchLimited = DBGitConfig.getInstance().getBoolean("core", "LIMIT_FETCH", DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true));
		try {
			if (isFetchLimited) {

				String query =
					"select COALESCE(count(*), 0) kolvo " +
					"from ( " +
					"	select 1 from "+ tableName +
					" 	limit " + (maxRowsCount + 1) +
					" ) tbl";

				try(Statement st = getConnection().createStatement(); ResultSet rs = st.executeQuery(query);){
					if(!rs.next()) throw new ExceptionDBGitRunTime("Could not execute rows count query");
					if (rs.getInt("kolvo") > maxRowsCount) {
						return new DBTableData(DBTableData.ERROR_LIMIT_ROWS);
					}
				}
			}

			final String tdQuery = "select * from " + tableName;
			return new DBTableData(getConnection(), tdQuery);
			//TODO other state
			//TODO find out what does 'other state' todo comment mean...

		} catch(Exception e) {
			final String msg = DBGitLang.getInstance().getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBUser> getUsers() {
		Map<String, DBUser> listUser = new HashMap<String, DBUser>();
		String query = "select * from pg_user";
		try (Statement stmt = getConnection().createStatement();ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				String name = rs.getString(1);
				StringProperties options = new StringProperties(rs);

				DBUser user = new DBUser(name, options);
				listUser.put(name, user);
			}
		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
		//select *from pg_catalog.pg_namespace;
		return listUser;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		Map<String, DBRole> listRole = new HashMap<String, DBRole>();
		String query =
			"SELECT r.rolname, r.rolsuper, r.rolinherit,\n" +
			"  r.rolcreaterole, r.rolcreatedb, r.rolcanlogin, \n" +
			"  r.rolreplication," + ((getDbVersionNumber() > 9.5) ? "r.rolbypassrls,\n" : "\n") +
			"  r.rolconnlimit, r.rolpassword, r.rolvaliduntil,\n" +
			"  ARRAY(SELECT b.rolname\n" +
			"        FROM pg_catalog.pg_auth_members m\n" +
			"        JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)\n" +
			"        WHERE m.member = r.oid) as memberof\n" +
			"FROM pg_catalog.pg_roles r\n" +
			"WHERE r.rolname !~ '^pg_'\n" +
			"ORDER BY 1;";
		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				String name = rs.getString("rolname");
				StringProperties options = new StringProperties(rs);

				DBRole role = new DBRole(name, options);
				listRole.put(name, role);
			}

		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}

		return listRole;
	}

	@Override
	public Map<String, DBUserDefinedType> getUDTs(String schema) {
		final Map<String, DBUserDefinedType> objects = new HashMap<>();
		final String query =
			"SELECT \n"
			+ "    n.nspname AS schema,\n"
			+ "    t.typname AS name, \n"
			+ "    r.rolname AS owner,\n"
			+ "    pg_catalog.obj_description ( t.oid, 'pg_type' ) AS description\n"
			+ "FROM        pg_type t \n"
			+ "LEFT JOIN   pg_catalog.pg_namespace n ON n.oid = t.typnamespace \n"
			+ "LEFT JOIN   pg_roles r ON r.oid = t.typowner\n"
			+ "WHERE       (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) \n"
			+ "AND     NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)\n"
			+ "AND     t.typtype = 'c'\n" 
			+ "AND 	   n.nspname = '"+schema+"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
			while (rs.next()) {
				final String name = rs.getString("name");
				final String owner = rs.getString("owner");
				final List<String> attributesSqls = new ArrayList<>();
				final String attributesQuery =
					"SELECT \n"
					+ "    attribute_name   AS \"name\", \n"
					+ "    data_type 	    AS \"type\", \n"
					+ "    ordinal_position AS \"order\"\n"
					+ "FROM information_schema.attributes a\n"
					+ "WHERE udt_schema = '"+schema+"' AND udt_name = '"+name+"'\n"
					+ "ORDER BY ordinal_position";
				try (Statement stmt2 = getConnection().createStatement(); ResultSet attributeRs = stmt2.executeQuery(
					attributesQuery)) {
					while (attributeRs.next()) {
						final String attrName = attributeRs.getString("name");
						final String attrType = attributeRs.getString("type");
						final String udtAttrDefinition = MessageFormat.format(
							"{0} {1}",
							escapeNameIfNeeded(attrName),
							attrType
						);
						attributesSqls.add(udtAttrDefinition);
					}
				}
				final StringProperties options = new StringProperties();
				final String sql = MessageFormat.format(
					"CREATE TYPE {0}.{1} AS (\n{2}\n);\n"
					+ "ALTER TYPE {0}.{1} OWNER TO {3};",
					escapeNameIfNeeded(schema),
					escapeNameIfNeeded(name),
					String.join(",\n    ", attributesSqls),
					owner
				);

				DBUserDefinedType object = new DBUserDefinedType(name, options, schema, owner, Collections.emptySet(), sql);
				options.addChild("attributes", String.join(",", attributesSqls));
				options.addChild("description", rs.getString("description"));
				objects.put(name, object);
			}

		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}

		return objects;
	}

	@Override
	public Map<String, DBDomain> getDomains(String schema) {
		System.out.println("getting domains");
		final Map<String, DBDomain> objects = new HashMap<>();
		final String query =
			"SELECT \n"
			+ "    n.nspname,\n"
			+ "    t.typname, \n"
			+ "    dom.data_type, \n"
			+ "    dom.domain_default,\n"
			+ "    r.rolname,\n"
			+ "    pg_catalog.obj_description ( t.oid, 'pg_type' ) AS description\n"
			+ "FROM        pg_type t \n"
			+ "LEFT JOIN   pg_catalog.pg_namespace n ON n.oid = t.typnamespace \n"
			+ "RIGHT JOIN  information_schema.domains dom ON dom.domain_name = t.typname AND dom.domain_schema = n.nspname\n"
			+ "LEFT JOIN   pg_roles r ON r.oid = t.typowner\n"
			+ "WHERE       (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) \n"
			+ "AND     NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)\n"
			+ "AND     n.nspname NOT IN ('pg_catalog', 'information_schema')\n"
			+ "AND dom.domain_schema = '"+schema+"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while (rs.next()) {
				final String name = rs.getString("typname");
				final String owner = rs.getString("rolname");
				final String type = rs.getString("data_type");
				final String defaultValue = rs.getString("domain_default");
				
				final List<String> constraintSqls = new ArrayList<>();
				final String constraintsQuery = 
					"SELECT con.conname, con.convalidated, con.consrc\n"
					+ "FROM information_schema.domains dom\n"
					+ "INNER JOIN information_schema.domain_constraints dcon ON dcon.domain_schema = dom.domain_schema AND dcon.domain_name = dom.domain_name\n"
					+ "INNER JOIN pg_catalog.pg_constraint con ON dcon.constraint_name = con.conname\n"
					+ "WHERE dom.domain_schema = '"+schema+"' AND dom.domain_name = '"+name+"'";
				try (Statement stmt2 = getConnection().createStatement(); ResultSet conRs = stmt2.executeQuery(constraintsQuery)) {
					while (conRs.next()) {
						final String conName = conRs.getString("conname");
						final String conSrc = conRs.getString("consrc");
						final Boolean conIsValidated = conRs.getBoolean("convalidated");
						constraintSqls.add(MessageFormat.format(
							"ALTER DOMAIN {0}.{1} ADD CONSTRAINT {2} CHECK {3} {4};",
							escapeNameIfNeeded(schema),
							escapeNameIfNeeded(name),
							escapeNameIfNeeded(conName),
							conSrc,
							conIsValidated ? "" : "NOT VALID"
						));
					}
				}
				
				final StringProperties options = new StringProperties(rs);
				final String sql = MessageFormat.format(
					"CREATE DOMAIN {0}.{1} AS {2} {3}; ALTER DOMAIN {0}.{1} OWNER TO {4};\n{5}",
					escapeNameIfNeeded(schema), escapeNameIfNeeded(name), type, defaultValue != null ? "DEFAULT " + defaultValue : "", owner,
					String.join("\n", constraintSqls)
				);

				DBDomain object = new DBDomain(name, options, schema, owner, Collections.emptySet(), sql);
				objects.put(name, object);
			}

		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}

		return objects;
	}

	@Override
	public Map<String, DBEnum> getEnums(String schema) {
		System.out.println("getting enums");
		final Map<String, DBEnum> objects = new HashMap<>();
		final String query = 
			"SELECT t.typname, r.rolname, pg_catalog.obj_description ( t.oid, 'pg_type' ) AS description," 
			 + "ARRAY( \n"
			 + "    SELECT e.enumlabel\n"
			 + "    FROM pg_catalog.pg_enum e\n"
			 + "    WHERE e.enumtypid = t.oid\n"
			 + "    ORDER BY e.oid \n"
			 + ") AS elements\n"
			 + "FROM        pg_type t \n"
			 + "LEFT JOIN   pg_catalog.pg_namespace n ON n.oid = t.typnamespace \n"
			 + "LEFT JOIN   pg_roles r ON r.oid = t.typowner\n"
			 + "WHERE       (t.typrelid = 0 OR (SELECT c.relkind = 'c' FROM pg_catalog.pg_class c WHERE c.oid = t.typrelid)) \n"
			 + "AND     NOT EXISTS(SELECT 1 FROM pg_catalog.pg_type el WHERE el.oid = t.typelem AND el.typarray = t.oid)\n"
			 + "AND     n.nspname NOT IN ('pg_catalog', 'information_schema')\n"
			 + "AND n.nspname = '"+schema+"'"
			 + "AND t.typtype = 'e'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while (rs.next()) {
				final String name = rs.getString("typname");
				final String owner = rs.getString("rolname");
				final String elements = String.join(
					",",
					Arrays.stream((String[]) rs.getArray("elements").getArray()).map( x->"'"+x+"'").collect(Collectors.toList())
				);
				final StringProperties options = new StringProperties();
				final String sql = MessageFormat.format( 
					"CREATE TYPE {0}.{1} AS ENUM ({2});\nALTER TYPE {0}.{1} OWNER TO {3}",
					schema, name, elements, owner
				);

				DBEnum object = new DBEnum(name, options, schema, owner, Collections.emptySet(), sql);
				options.addChild("elements", elements);
				objects.put(name, object);
			}

		} catch (Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}

		return objects;
	}

	@Override
	public DBUserDefinedType getUDT(String schema, String name) {
		final Map<String, DBUserDefinedType> udTs = getUDTs(schema);
		if (udTs.containsKey(name)) {
			return udTs.get(name);
		} else {
			throw new ExceptionDBGitObjectNotFound(lang.getValue("errors", "adapter", "objectNotFoundInDb").toString());
		}
	}

	@Override
	public DBDomain getDomain(String schema, String name) {
		final Map<String, DBDomain> domains = getDomains(schema);
		if (domains.containsKey(name)) {
			return domains.get(name);
		} else {
			throw new ExceptionDBGitObjectNotFound(lang.getValue("errors", "adapter", "objectNotFoundInDb").toString());
		}
	}

	@Override
	public DBEnum getEnum(String schema, String name) {
		final Map<String, DBEnum> enums = getEnums(schema);
		if (enums.containsKey(name)) {
			return enums.get(name);
		} else {
			throw new ExceptionDBGitObjectNotFound(lang.getValue("errors", "adapter", "objectNotFoundInDb").toString());
		}
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() { return true; }
	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() { return backupFactory; }
	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() { return convertFactory; }
	@Override
	public DbType getDbType() { return DbType.POSTGRES; }
	@Override
	public String getDbVersion() {
		final String query = "SHOW server_version";
		try (
			PreparedStatement stmt = getConnection().prepareStatement(query);
			ResultSet resultSet = stmt.executeQuery();
		) {

			if(!resultSet.next()){
				final String msg = "failed to get schema data";
				throw new ExceptionDBGitRunTime(msg);
			}

			return resultSet.getString("server_version");

		} catch (SQLException e) {
			final String msg = "failed to get database version";
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}
	@Override
	public String getDefaultScheme() throws ExceptionDBGit { return "public"; }
	@Override
	public boolean isReservedWord(String word) { return reservedWords.contains(word.toUpperCase()); }

	@Override
	public void createSchemaIfNeed(String schemaName) {
		final String query =
			"select count(*) cnt " +
			"from information_schema.schemata " +
			"where upper(schema_name) = '" + schemaName.toUpperCase() + "'";

		try (Statement st = connect.createStatement(); ResultSet rs = st.executeQuery(query);){

			if(!rs.next()) {
				final String msg = "failed to get schema data";
				throw new ExceptionDBGitRunTime(msg);
			}

			if (rs.getInt("cnt") == 0) {
				try(StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());){
					stLog.execute("create schema " + schemaName + ";\n");
				}
			} else {
				return;
			}

		} catch (SQLException e) {
			final String msg = lang.getValue("errors", "adapter", "createSchema").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

	}

	@Override
	public void createRoleIfNeed(String roleName) {
		final String query =
			"select count(*) cnt " +
			"from pg_catalog.pg_roles " +
			"where upper(rolname) = '" + roleName.toUpperCase() + "'";

		try (Statement st = connect.createStatement(); ResultSet rs = st.executeQuery(query);) {

			if(!rs.next()) {
				final String msg = "failed to get role data";
				throw new ExceptionDBGitRunTime(msg);
			}

			if (rs.getInt("cnt") == 0){
				try(StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());) {
					//TODO which kind of PASSWORD that equals to ROLE NAME?
					stLog.execute("CREATE ROLE " + roleName + " LOGIN PASSWORD '" + roleName + "'");
				}
			} else {
				return;
			}

		} catch (SQLException e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "createSchema");
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	public String escapeNameIfNeeded(String name){
		boolean shouldBeEscaped = !name.equals(name.toLowerCase())
			|| name.contains(".")
			|| name.contains(".")
			|| reservedWords.contains(name.toUpperCase());
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
//		reservedWords.add("PUBLIC");
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
