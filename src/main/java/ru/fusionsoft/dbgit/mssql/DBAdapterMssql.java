package ru.fusionsoft.dbgit.mssql;

import com.google.common.collect.ImmutableMap;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.core.db.FieldType;
import ru.fusionsoft.dbgit.data_table.*;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.sql.*;
import java.util.*;
import java.util.concurrent.TimeUnit;


public class DBAdapterMssql extends DBAdapter {

	public static final String DEFAULT_MAPPING_TYPE = "varchar";
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
		final Map<String, DBSchema> listScheme = new HashMap<>();
		try (ResultSet rs = getConnection().getMetaData().getSchemas()){

			// made without query
			// Statement stmt = connect.createStatement();
			// ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				final String name = rs.getString("TABLE_SCHEM");

				// May also get catalog names that belong to scheme as "TABLE_CATALOG"
				if(!systemSchemas.contains(name)) {
					final DBSchema scheme = new DBSchema(name, new StringProperties(rs));
					listScheme.put(name, scheme);
				}
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "schemes").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listScheme;
	}

	@Override
	public Map<String, DBTableSpace> getTableSpaces() {
		final Map<String, DBTableSpace> listTableSpace = new HashMap<>();
		final String query = "SELECT           \n" +
			"[SFG].name AS [File Group Name],\n" +
			"[SFG].*,\n" +
			"[SDB].name AS [Database Name],\n" +
			"[F].name AS [File Name],\n" +
			"[SDBF].name AS [Database File Name],\n" +
			"[SDBF].physical_name\n" +
			"INTO #fgroups\n" +
			"FROM [master].sys.master_files						AS [F]\n" +
			"INNER JOIN  sys.databases							AS [SDB]\n" +
			"    ON [SDB].database_id = [F].database_id\n" +
			"INNER JOIN sys.database_files						AS [SDBF]\n" +
			"    ON [SDBF].[file_id] = [F].[file_id]\n" +
			"INNER JOIN sys.filegroups							AS [SFG]\n" +
			"    ON [sfg].data_space_id = [F].data_space_id\n" +
			"SELECT \n" +
			"  [File Group Name],\n" +
			"  [data_space_id],\n" +
			"  [type],\n" +
			"  [type_desc],\n" +
			"  [is_default],\n" +
			"  [is_system],\n" +
			"  [is_read_only],\n" +
			"  [filegroup_guid],\n" +
			"  [log_filegroup_id],\n" +
			"  STUFF((\n" +
			"    SELECT DISTINCT ', ' + [Database Name] \n" +
			"    FROM #fgroups \n" +
			"    WHERE ([File Group Name] = Results.[File Group Name]) \n" +
			"    FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')\n" +
			"  ,1,2,'') AS DatabaseNames,  \n" +
			"  STUFF((\n" +
			"    SELECT DISTINCT ', ' + [Database File Name] \n" +
			"    FROM #fgroups \n" +
			"    WHERE ([File Group Name] = Results.[File Group Name]) \n" +
			"    FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')\n" +
			"  ,1,2,'') AS DatabaseFileNames,\n" +
			"  STUFF((\n" +
			"    SELECT DISTINCT ', ' + [File Name] \n" +
			"    FROM #fgroups \n" +
			"    WHERE ([File Group Name] = Results.[File Group Name]) \n" +
			"    FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')\n" +
			"  ,1,2,'') AS FileNames,\n" +
			"  STUFF((\n" +
			"    SELECT DISTINCT ', ' + [physical_name] \n" +
			"    FROM #fgroups \n" +
			"    WHERE ([File Group Name] = Results.[File Group Name]) \n" +
			"    FOR XML PATH(''),TYPE).value('(./text())[1]','VARCHAR(MAX)')\n" +
			"  ,1,2,'') AS PhysicalNames\n" +
			"FROM #fgroups Results\n" +
			"GROUP BY [File Group Name],[data_space_id],\n" +
			"  [type],\n" +
			"  [type_desc],\n" +
			"  [is_default],\n" +
			"  [is_system],\n" +
			"  [is_read_only],\n" +
			"  [filegroup_guid],\n" +
			"  [log_filegroup_id]\n" +
			"DROP TABLE #fgroups\n";


		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("File Group Name");
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
		final String query =
			"SELECT seq.*,\n" +
			"TYPE_NAME(seq.system_type_id) as typeName,\n" +
			"SCHEMA_NAME(seq.schema_id) as owner \n" +
			"FROM sys.objects, sys.SEQUENCES seq \n" +
			"WHERE sys.objects.object_id = seq.object_id \n" +
			"AND SCHEMA_NAME(seq.schema_id) = '"+schema+"'";

		try(Statement stmtValue = getConnection().createStatement(); ResultSet rs = stmtValue.executeQuery(query)){

            while(rs.next()){
				final String ownerSeq = "dbo";
				final String nameSeq = rs.getString("name");
				final Long valueSeq = rs.getLong("current_value");
				final DBSequence seq =  new DBSequence(nameSeq, new StringProperties(rs), schema, ownerSeq, Collections.emptySet(), valueSeq);
                listSequence.put(nameSeq, seq);
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
			"SELECT seq.*,\n" +
			"USER_NAME(objectproperty(seq.object_id,'OwnerId')) as owner,\n" +
			"TYPE_NAME(seq.system_type_id) as typeName, " +
			"SCHEMA_NAME(seq.schema_id) as schemaName " +
			"FROM sys.objects, sys.SEQUENCES seq " +
			"WHERE sys.objects.object_id = seq.object_id " +
			"AND SCHEMA_NAME(seq.schema_id) = '"+schema+"' " +
			"AND seq.name = '" + name + "'\n";

		try(Statement stmtValue = getConnection().createStatement(); ResultSet rs = stmtValue.executeQuery(query)){

			if(rs.next()){
				final String ownerSeq = "dbo";
				final String nameSeq = rs.getString("name");
				final Long valueSeq = rs.getLong("current_value");
				return new DBSequence(nameSeq, new StringProperties(rs), schema, ownerSeq, Collections.emptySet(), valueSeq);
			} else {
				final String msg = lang.getValue("errors", "adapter", "objectNotFoundInDb").toString();
				throw new ExceptionDBGitObjectNotFound(msg);
			}

        } catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "seq").toString();
			throw new ExceptionDBGitRunTime(msg, e);
        }
    }

	@Override
	public Map<String, DBTable> getTables(String schema) {
		final Map<String, DBTable> listTable = new HashMap<>();
		final String query =
			"SELECT TABLE_NAME as 'name', TABLE_CATALOG as 'database', TABLE_SCHEMA as 'schema'\n" +
			"FROM INFORMATION_SCHEMA.TABLES \n" +
			"WHERE INFORMATION_SCHEMA.TABLES.TABLE_SCHEMA = '" + schema + "'\n" +
			"AND INFORMATION_SCHEMA.TABLES.TABLE_TYPE = 'BASE TABLE'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
            while(rs.next()){
				//TODO retrieve table comment
				//TODO retrieve table owner
				final String nameTable = rs.getString("name");
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
			"SELECT\n" +
			"	o.name tableName, t.TABLE_SCHEMA schemaName, t.TABLE_CATALOG catalogName,\n" +
			"	CASE WHEN o.principal_id is NOT NULL THEN (SELECT name FROM sys.database_principals dp WHERE dp.principal_id=o.principal_id)\n" +
			"	ELSE (SELECT dp.name FROM sys.database_principals dp,sys.schemas s WHERE s.schema_id=o.schema_id and s.principal_id=dp.principal_id)\n" +
			"	END as owner\n" +
			"FROM sys.objects o, INFORMATION_SCHEMA.TABLES t\n" +
			"WHERE o.type='U' AND o.name = t.TABLE_NAME AND t.TABLE_NAME = '"+name+"' AND t.TABLE_SCHEMA = '"+schema+"'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {
			if (rs.next()){
				//TODO retrieve table comment
				//TODO retrieve table owner
				final String nameTable = rs.getString("name");
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
		final Map<String, DBTableField> listField = new HashMap<>();
		final String query =
			"SELECT DISTINCT\n" +
			"	c.TABLE_SCHEMA as schemaName,\n" +
			"	c.TABLE_NAME as tableName,\n" +
			"	c.COLUMN_NAME as columnName,\n" +
			"	c.ORDINAL_POSITION as columnOrder,\n" +
			"	c.DATA_TYPE as mssqlType,\n" +
			"	CASE WHEN lower(c.DATA_TYPE) in ('bigint', 'int', 'float', 'decimal', 'money', 'numeric', 'real', 'smallint', 'smallmoney', 'tinyint') then 'number' \n" +
			"		when lower(c.DATA_TYPE) in ('char','varchar','xml','nchar','nvarchar', 'uniqueidentifier') then 'string'\n" +
			"		when lower(c.DATA_TYPE) in ('bit') then 'boolean'\n" +
			"		when lower(c.DATA_TYPE) in ('datetime', 'smalldatetime', 'time') then 'date'\n" +
			"		when lower(c.DATA_TYPE) in ('text','ntext') then 'text'\n" +
			"		when lower(c.DATA_TYPE) in ('timestamp', 'binary', 'varbinary', 'geometry', 'geography') then 'binary'\n" +
			"		else 'native'\n" +
			"		end dbgitType,\n" +
			"	CASE WHEN 1 IN ( \n" +
			"		SELECT OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)),'IsPrimaryKey')\n" +
			"		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE\n" +
			"		WHERE c.COLUMN_NAME = COLUMN_NAME AND c.TABLE_NAME = TABLE_NAME\n" +
			"	)\n" +
			"	THEN 1 ELSE 0 END isPk,\n" +
			"	c.IS_NULLABLE as isNullable,\n" +
			"	c.NUMERIC_SCALE as scale,\n" +
			"	c.CHARACTER_MAXIMUM_LENGTH as length,\n" +
			"	CASE WHEN lower(c.DATA_TYPE) in ('char', 'nchar') then '1' else '0' end isFixed," +
			"	c.NUMERIC_PRECISION as precision\n" +
			"FROM INFORMATION_SCHEMA.COLUMNS as c\n" +
			"WHERE TABLE_SCHEMA = '" +  schema +  "' AND TABLE_NAME = '" + nameTable + "'";

		try(Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);) {

			while(rs.next()){
				final DBTableField field = DBTableFieldFromRs(rs);
				listField.put(field.getName(), field);
			}

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

		return listField;
	}

	private DBTableField DBTableFieldFromRs(ResultSet rs) throws SQLException {
		final boolean isPrimaryKey = rs.getString("isPk").equals("1");
		final boolean isFixed = rs.getBoolean("isFixed");
		final boolean isNullable = rs.getBoolean("isNullable");
		final String columnName = rs.getString("columnName").toLowerCase();
		//TODO make find out column comment
		final String columnDesc = "";
		//TODO make find out column default value
		final String columnDefault = "";
		final String typeSQL = getFieldType(rs);
		final FieldType typeUniversal = FieldType.fromString(rs.getString("dbgitType").toUpperCase());
		final int length = rs.getInt("length");
		final int scale = rs.getInt("scale");
		final int precision = rs.getInt("precision");
		final int order = rs.getInt("order");

		return new DBTableField(
			columnName, 
			columnDesc == null ? "" : columnDesc,
			isPrimaryKey, isNullable,
			typeSQL, typeUniversal, order, 
			columnDefault == null ? "" : columnDefault,
			length, precision, scale, isFixed
		);

	}

	protected String getFieldType(ResultSet rs) throws SQLException {

		final StringBuilder type = new StringBuilder();
		final Integer max_length = rs.getInt("length");
		final String mssqlType = rs.getString("mssqlType");
		final boolean isNotNull = rs.getString("isNullable").equals("NO");

		type.append(mssqlType);
		if (!rs.wasNull()) {
			type.append("("+max_length.toString()+")");
		}
		if (isNotNull){
			type.append(" NOT NULL");
		}

		return type.toString();

	}

	public Map<String, DBIndex> getIndexesWithPks(String schema, String nameTable) {
		final Map<String, DBIndex> indexes = new HashMap<>();
		final String query =
			"    SELECT DB_NAME() AS databaseName,\n" +
			"    sc.name as schemaName, \n" +
			"	 t.name AS tableName,\n" +
			"	 col.name as columnName,\n" +
			"    si.name AS indexName,\n" +
			"	 si.is_primary_key isPk," +
			"	 si.index_id as indexId,\n" +
			"	 si.type_desc as typeName, \n" +
			"    CASE si.index_id WHEN 0 THEN NULL\n" +
			"    ELSE \n" +
			"        CASE is_primary_key WHEN 1 THEN\n" +
			"            N'ALTER TABLE ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name) + N' ADD CONSTRAINT ' + QUOTENAME(si.name) + N' PRIMARY KEY ' +\n" +
			"                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED '\n" +
			"            ELSE N'CREATE ' + \n" +
			"                CASE WHEN si.is_unique = 1 then N'UNIQUE ' ELSE N'' END +\n" +
			"                CASE WHEN si.index_id > 1 THEN N'NON' ELSE N'' END + N'CLUSTERED ' +\n" +
			"                N'INDEX ' + QUOTENAME(si.name) + N' ON ' + QUOTENAME(sc.name) + N'.' + QUOTENAME(t.name) + N' '\n" +
			"        END +\n" +
			"        /* key def */ N'(' + key_definition + N')' +\n" +
			"        /* includes */ CASE WHEN include_definition IS NOT NULL THEN \n" +
			"            N' INCLUDE (' + include_definition + N')'\n" +
			"            ELSE N''\n" +
			"        END +\n" +
			"        /* filters */ CASE WHEN filter_definition IS NOT NULL THEN \n" +
			"            N' WHERE ' + filter_definition ELSE N''\n" +
			"        END +\n" +
			"        /* with clause - compression goes here */\n" +
			"        CASE WHEN row_compression_partition_list IS NOT NULL OR page_compression_partition_list IS NOT NULL \n" +
			"            THEN N' WITH (' +\n" +
			"                CASE WHEN row_compression_partition_list IS NOT NULL THEN\n" +
			"                    N'DATA_COMPRESSION = ROW ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + row_compression_partition_list + N')' END\n" +
			"                ELSE N'' END +\n" +
			"                CASE WHEN row_compression_partition_list IS NOT NULL AND page_compression_partition_list IS NOT NULL THEN N', ' ELSE N'' END +\n" +
			"                CASE WHEN page_compression_partition_list IS NOT NULL THEN\n" +
			"                    N'DATA_COMPRESSION = PAGE ' + CASE WHEN psc.name IS NULL THEN N'' ELSE + N' ON PARTITIONS (' + page_compression_partition_list + N')' END\n" +
			"                ELSE N'' END\n" +
			"            + N')'\n" +
			"            ELSE N''\n" +
			"        END +\n" +
			"        ' ON ' + CASE WHEN psc.name is null \n" +
			"            THEN ISNULL(QUOTENAME(fg.name),N'')\n" +
			"            ELSE psc.name + N' (' + partitioning_column.column_name + N')' \n" +
			"            END\n" +
			"        + N';'\n" +
			"    END AS ddl,\n" +
			"    si.has_filter,\n" +
			"    si.is_unique,\n" +
			"    ISNULL(pf.name, NULL) AS partition_function,\n" +
			"    ISNULL(psc.name, fg.name) AS partition_scheme_or_filegroup\n" +
			"FROM sys.indexes AS si \n" +
			"JOIN sys.index_columns ic ON  si.object_id = ic.object_id and si.index_id = ic.index_id \n" +
			"JOIN sys.columns col ON ic.object_id = col.object_id and ic.column_id = col.column_id \n" +
			"JOIN sys.tables AS t ON si.object_id=t.object_id\n" +
			"JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id\n" +
			"LEFT JOIN sys.dm_db_index_usage_stats AS stat ON \n" +
			"    stat.database_id = DB_ID() \n" +
			"    and si.object_id=stat.object_id \n" +
			"    and si.index_id=stat.index_id\n" +
			"LEFT JOIN sys.partition_schemes AS psc ON si.data_space_id=psc.data_space_id\n" +
			"LEFT JOIN sys.partition_functions AS pf ON psc.function_id=pf.function_id\n" +
			"LEFT JOIN sys.filegroups AS fg ON si.data_space_id=fg.data_space_id\n" +
			"OUTER APPLY ( SELECT STUFF (\n" +
			"    (SELECT N', ' + QUOTENAME(c.name) +\n" +
			"        CASE ic.is_descending_key WHEN 1 then N' DESC' ELSE N'' END\n" +
			"    FROM sys.index_columns AS ic \n" +
			"    JOIN sys.columns AS c ON \n" +
			"        ic.column_id=c.column_id  \n" +
			"        and ic.object_id=c.object_id\n" +
			"    WHERE ic.object_id = si.object_id\n" +
			"        and ic.index_id=si.index_id\n" +
			"        and ic.key_ordinal > 0\n" +
			"    ORDER BY ic.key_ordinal FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS keys ( key_definition )\n" +
			"OUTER APPLY (\n" +
			"    SELECT MAX(QUOTENAME(c.name)) AS column_name\n" +
			"    FROM sys.index_columns AS ic \n" +
			"    JOIN sys.columns AS c ON \n" +
			"        ic.column_id=c.column_id  \n" +
			"        and ic.object_id=c.object_id\n" +
			"    WHERE ic.object_id = si.object_id\n" +
			"        and ic.index_id=si.index_id\n" +
			"        and ic.partition_ordinal = 1) AS partitioning_column\n" +
			"OUTER APPLY ( SELECT STUFF (\n" +
			"    (SELECT N', ' + QUOTENAME(c.name)\n" +
			"    FROM sys.index_columns AS ic \n" +
			"    JOIN sys.columns AS c ON \n" +
			"        ic.column_id=c.column_id  \n" +
			"        and ic.object_id=c.object_id\n" +
			"    WHERE ic.object_id = si.object_id\n" +
			"        and ic.index_id=si.index_id\n" +
			"        and ic.is_included_column = 1\n" +
			"    ORDER BY c.name FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS includes ( include_definition )\n" +
			"OUTER APPLY ( SELECT STUFF (\n" +
			"    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))\n" +
			"    FROM sys.partitions AS p\n" +
			"    WHERE p.object_id = si.object_id\n" +
			"        and p.index_id=si.index_id\n" +
			"        and p.data_compression = 1\n" +
			"    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS row_compression_clause ( row_compression_partition_list )\n" +
			"OUTER APPLY ( SELECT STUFF (\n" +
			"    (SELECT N', ' + CAST(p.partition_number AS VARCHAR(32))\n" +
			"    FROM sys.partitions AS p\n" +
			"    WHERE p.object_id = si.object_id\n" +
			"        and p.index_id=si.index_id\n" +
			"        and p.data_compression = 2\n" +
			"    ORDER BY p.partition_number FOR XML PATH(''), TYPE).value('.', 'NVARCHAR(MAX)'),1,2,'')) AS page_compression_clause ( page_compression_partition_list )\n" +
			"WHERE si.type IN (1,2) /* clustered, nonclustered */\n" +
//					"AND si.is_primary_key = 0 /* no PKs */\n" +
			"AND si.is_hypothetical = 0 /* bugged feature, always better to delete, no need to store and reconstruct them */\n" +
			"AND upper(t.name) = upper('" + nameTable + "') AND upper(sc.name) = upper('" + schema + "')" +
			"OPTION (RECOMPILE);";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("indexName");
				final String owner = rs.getString("owner");
				final String sql = rs.getString("ddl");
				final DBIndex index = new DBIndex(name, new StringProperties(rs), schema, owner, Collections.emptySet(), sql);

				indexes.put(index.getName(), index);
			}

			return indexes;

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "indexes").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable){
		final Map<String, DBIndex> indexes = getIndexesWithPks(schema, nameTable);

		indexes.values().removeIf(x->x.getOptions().getChildren().get("ispk").getData().equals("1"));
		return indexes;
	}

    @Override
    public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		final Map<String, DBConstraint> constraints = new HashMap<>();
		final ArrayList<String> queries = new ArrayList<>();
        //TODO [] in object names
        //check
        queries.add(
			"SELECT sc.name as schemaName, t.name as tableName, col.name as columnName, c.name as constraintName, c.name as indexName, c.type_desc as constraintType, \n" +
			"'ALTER TABLE ' + sc.name + '.' + t.name + ' ADD CONSTRAINT ' + c.name + ' CHECK ' + c.definition + ';' as ddl\n" +
			"FROM sys.check_constraints c\n" +
			"JOIN sys.tables t ON c.parent_object_id = t.object_id \n" +
			"LEFT OUTER JOIN sys.columns col on col.column_id = c.parent_column_id AND col.object_id = c.parent_object_id\n" +
			"JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id \n" +
			"WHERE t.name = :name AND sc.name = :schema");
        //default
        queries.add(
			"SELECT sc.name AS schemaName, t.name AS tableName, col.name AS columnName, c.name AS constraintName, c.type_desc AS constraintType, \n" +
			"'ALTER TABLE ' + sc.name + '.' + t.name + ' ADD CONSTRAINT ' + c.name+ ' DEFAULT ' \n" +
			"	+ CASE WHEN ISNUMERIC(ic.COLUMN_DEFAULT) = 1 \n" +
			"		THEN TRY_CONVERT(nvarchar, TRY_CONVERT(numeric, ic.COLUMN_DEFAULT))\n" +
			"		ELSE '' + ic.COLUMN_DEFAULT + '' END\n" +
			"	+ ' FOR [' + col.name + '];' AS ddl\n" +
			"FROM sys.default_constraints c\n" +
			"JOIN sys.tables t ON c.parent_object_id = t.object_id \n" +
			"JOIN sys.columns col ON col.default_object_id = c.object_id\n" +
			"JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id \n" +
			"JOIN INFORMATION_SCHEMA.COLUMNS ic on t.name = ic.TABLE_NAME AND col.name = ic.COLUMN_NAME \n" +
			"WHERE t.name = :name AND sc.name = :schema\n"
		);
        //unique
        queries.add(
			"SELECT TC.TABLE_SCHEMA AS schemaName, TC.TABLE_NAME AS tableName, CC.Column_Name AS columnName, TC.Constraint_Name AS constraintName, TC.CONSTRAINT_TYPE AS constraintType,\n" +
			"'ALTER TABLE ' + TC.TABLE_SCHEMA + '.' + TC.TABLE_NAME + ' ADD CONSTRAINT ' + TC.CONSTRAINT_NAME + ' UNIQUE NONCLUSTERED ([' + CC.COLUMN_NAME + ']);' AS ddl\n" +
			"FROM INFORMATION_SCHEMA.table_constraints TC\n" +
			"INNER JOIN INFORMATION_SCHEMA.constraint_column_usage CC on TC.Constraint_Name = CC.Constraint_Name\n" +
			"WHERE TC.constraint_type = 'Unique' AND TC.TABLE_NAME = :name AND TC.TABLE_SCHEMA = :schema ---- PARAMETER 1,2\n"
		);
        //foreign
        queries.add(
			"SELECT ss.name as schemaName, t.name as tableName, sc.name as columnName, o.name as constraintName, o.type_desc as constraintType, refss.name as refSchemaName, refst.name as refTableName, refsc.name as refColumnName, " +
			"'ALTER TABLE ' + ss.name + '.' + t.name + ' ADD CONSTRAINT ' + o.name + ' FOREIGN KEY ('+ sc.name + ') references ' + refss.name + '.' + refst.name + '(' + refsc.name + ');' as ddl\n" +
			"FROM sys.foreign_key_columns c\n" +
			"JOIN sys.objects o ON c.constraint_object_id = o.object_id\n" +
			"LEFT OUTER JOIN sys.tables t on t.object_id = c.parent_object_id \n" +
			"LEFT OUTER JOIN sys.schemas ss on ss.schema_id = o.schema_id \n" +
			"LEFT OUTER JOIN sys.columns sc on sc.object_id = c.parent_object_id AND sc.column_id = c.parent_column_id\n" +
			"LEFT OUTER JOIN sys.tables refst on refst.object_id = c.referenced_object_id\n" +
			"LEFT OUTER JOIN sys.schemas refss on refss.schema_id = refst.schema_id\n" +
			"LEFT OUTER JOIN sys.columns refsc on refsc.object_id = c.referenced_object_id AND refsc.column_id = c.referenced_column_id \n" +
			"WHERE t.name = :name AND ss.name = :schema\n"
		);


		final Iterator<String> it = queries.iterator();
		while (it.hasNext()) {
			final String query = it.next();
			try (
				PreparedStatement stmt = preparedStatement(getConnection(), query, ImmutableMap.of("name", nameTable, "schema" , schema));
				ResultSet rs = stmt.executeQuery(query);
			){

				while (rs.next()) {
					final String name = rs.getString("constraintName");
					final String type = rs.getString("constraintType");
					final String sql = rs.getString("ddl");
					final String owner = schema;

					final DBConstraint con = new DBConstraint(name, new StringProperties(rs), schema, owner, Collections.emptySet(), sql, type);
					constraints.put(con.getName(), con);
				}
			} catch (Exception ex){
				final String msg = lang.getValue("errors", "adapter", "constraints").toString();
				throw new ExceptionDBGitRunTime(msg, ex);
			}
		}

		//primary keys
		final Map<String, DBIndex> indexes = getIndexesWithPks(schema, nameTable);
		indexes.values().removeIf(x->x.getOptions().getChildren().get("ispk").getData().equals("0"));

		for( DBIndex pki : indexes.values() ){
			final String constraintType = pki.getOptions().getChildren().get("typename").getData();
			final DBConstraint pkc = new DBConstraint(
				pki.getName(),
				pki.getOptions(),
				pki.getSchema(),
				pki.getOwner(),
				new HashSet<>(pki.getDependencies()),
				pki.getSql(),
				constraintType
			);
			pkc.setOptions(pki.getOptions());
			constraints.put(pkc.getName(), pkc);
		}

		return constraints;

    }

    @Override
    public Map<String, DBView> getViews(String schema) {
		final Map<String, DBView> listView = new HashMap<String, DBView>();
		final String query =
			"SELECT \n" +
			"	sp.name as ownerName, sp.type_desc as ownerType, ss.name AS schemaName, sv.name AS viewName, sm.definition as ddl, \n" +
			"	sv.type_desc as typeName, sm.uses_ansi_nulls, sm.uses_quoted_identifier, sm.is_schema_bound, \n" +
			"	OBJECTPROPERTYEX(sv.object_id,'IsIndexable') AS IsIndexable,\n" +
			"	OBJECTPROPERTYEX(sv.object_id,'IsIndexed') AS IsIndexed\n" +
			"FROM sys.views sv\n" +
			"JOIN sys.schemas ss ON sv.schema_id = ss.schema_id\n" +
			"LEFT OUTER JOIN sys.sql_modules sm on sv.object_id = sm.object_id\n" +
			"LEFT OUTER JOIN sys.database_principals sp on sv.principal_id = sp.principal_id";

        try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

            while(rs.next()){
				final String name = rs.getString("viewName");
				final String schemaName = rs.getString("schemaName");
				final String owner = rs.getString("ownerName");
				final String sql = rs.getString("ddl");

				final DBView view = new DBView(name, new StringProperties(rs), schema, owner, Collections.emptySet(), sql);
                listView.put(name, view);
            }
            return listView;

        } catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "views");
            throw new ExceptionDBGitRunTime(msg, e);
        }
    }

    @Override
    public DBView getView(String schema, String name) {
		//TODO single-version query with ExceptionDBGitNotFound
        try {
            return getViews(schema).get(name);
        } catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "views");
			throw new ExceptionDBGitRunTime(msg, e);
        }
    }

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		// No such implementation in MSSQL
		return Collections.emptyMap();
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		// No such implementation in MSSQL
		throw new ExceptionDBGitRunTime(new ExceptionDBGitObjectNotFound("cannot get packages on mssql"));
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		final Map<String, DBProcedure> listProcedure = new HashMap<String, DBProcedure>();
		final String query =
			"SELECT s.name schemaName, o.name procedureName, o.type_desc as typeName, definition ddl, USER_NAME(so.uid) AS owner \n" +
			"FROM sys.sql_modules m\n" +
			"JOIN sys.procedures p ON m.object_id = p.object_id\n" +
			"JOIN sys.objects o \n" +
			"	ON o.object_id = p.object_id \n" +
			"	AND Left(o.name, 3) NOT IN ('sp_', 'xp_', 'ms_') \n" +
			"JOIN sys.schemas s ON s.schema_id = o.schema_id\n" +
			"JOIN sysobjects so on o.object_id = so.id\n" +
			"WHERE s.name = '" + schema + "'\n";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){
			while(rs.next()){
				final String name = rs.getString("procedureName");
				final String owner = rs.getString("owner");
				final String sql = rs.getString("ddl");
				final StringProperties options = new StringProperties(rs);
				final DBProcedure proc = new DBProcedure(name, options, schema, owner, Collections.emptySet(), sql);
				listProcedure.put(name, proc);
			}
		}catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "prc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
		return listProcedure;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		final String query =
			"SELECT s.name schemaName, o.name procedureName, o.type_desc as typeName, definition ddl, USER_NAME(so.uid) AS owner \n" +
			"FROM sys.sql_modules m\n" +
			"JOIN sys.procedures p ON m.object_id = p.object_id\n" +
			"JOIN sys.objects o \n" +
			"	ON o.object_id = p.object_id \n" +
			"	AND Left(o.name, 3) NOT IN ('sp_', 'xp_', 'ms_') -- filter out system ones\n" +
			"JOIN sys.schemas s ON s.schema_id = o.schema_id\n" +
			"JOIN sysobjects so on o.object_id = so.id \n" +
			"WHERE s.name = '" + schema + "' AND o.name = '" + name + "'";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			if (!rs.next()) throw new ExceptionDBGitObjectNotFound("");

			final String owner = rs.getString("owner");
			final String procedureName = rs.getString("procedureName");
			final String sql = rs.getString("ddl");
			final StringProperties options = new StringProperties(rs);

			return new DBProcedure(procedureName, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "prc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		final Map<String, DBFunction> listFunction = new HashMap<>();
		final String query =
			"SELECT ss.name schemaName, o.name functionName, type_desc typeName, definition ddl, USER_NAME(so.uid) owner \n" +
			"FROM sys.sql_modules m \n" +
			"INNER JOIN sys.objects o ON m.object_id = o.object_id\n" +
			"INNER JOIN sysobjects so ON m.object_id = so.id\n" +
			"INNER JOIN sys.schemas ss ON ss.schema_id = o.schema_id\n" +
			"WHERE type_desc like '%function%' AND ss.name = '" + schema + "'\n";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){

				final String name = rs.getString("functionName");
				final String owner = rs.getString("owner");
				final String sql = rs.getString("ddl");
				final StringProperties options = new StringProperties(rs);

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
			"SELECT ss.name schemaName, o.name functionName, type_desc typeName, definition ddl, USER_NAME(so.uid) owner \n" +
			"FROM sys.sql_modules m \n" +
			"INNER JOIN sys.objects o ON m.object_id = o.object_id\n" +
			"INNER JOIN sysobjects so ON m.object_id = so.id\n" +
			"INNER JOIN sys.schemas ss ON ss.schema_id = o.schema_id\n" +
			"WHERE type_desc like '%function%' AND ss.name = '" + schema + "' AND o.name = '" + name + "'\n";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			if (!rs.next()) throw new ExceptionDBGitObjectNotFound("");

			final String functionName = rs.getString("functionName");
			final String owner = rs.getString("owner");
			final String sql = rs.getString("ddl");
			final StringProperties options = new StringProperties(rs);

			return new DBFunction(functionName, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "fnc").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	//TODO Discuss scenario when we get an encrypted TRIGGER, IMO display a warning,
	// it is not possible to get definition of an encrypted trigger

	public Map<String, DBTrigger> getTriggers(String schema) {
		final Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		final String query =
			"SELECT \n" +
			"   s.name schemaName, \n" +
			"   o.name triggerName, \n" +
			"   USER_NAME(o.uid) owner, \n" +
			"   OBJECT_NAME(parent_obj) tableName, \n" +
			"   m.definition as ddl, \n" +
			"   OBJECTPROPERTY(id, 'IsEncrypted') AS encrypted \n" +
			"FROM sysobjects o\n" +
			"INNER JOIN sys.tables t ON o.parent_obj = t.object_id \n" +
			"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id \n" +
			"INNER JOIN sys.sql_modules m ON m.object_id = o.id\n" +
			"WHERE o.type = 'TR' AND s.name = '" + schema + "'\n";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			while(rs.next()){
				final String name = rs.getString("triggerName");
				final String owner = rs.getString("owner");
				final String sql = rs.getString("ddl");
				final StringProperties options = new StringProperties(rs);

				DBTrigger trigger = new DBTrigger(name, options, schema, owner, Collections.emptySet(), sql);
				listTrigger.put(name, trigger);
			}
			return listTrigger;

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "triggers").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

	public DBTrigger getTrigger(String schema, String name) {
		final String query =
			"SELECT \n" +
			"   s.name schemaName, \n" +
			"   o.name triggerName, \n" +
			"   USER_NAME(o.uid) owner, \n" +
			"   OBJECT_NAME(parent_obj) tableName, \n" +
			"   m.definition as ddl, \n" +
			"   OBJECTPROPERTY(id, 'IsEncrypted') AS encrypted \n" +
			"FROM sysobjects o\n" +
			"INNER JOIN sys.tables t ON o.parent_obj = t.object_id \n" +
			"INNER JOIN sys.schemas s ON t.schema_id = s.schema_id \n" +
			"INNER JOIN sys.sql_modules m ON m.object_id = o.id\n" +
			"WHERE o.type = 'TR' AND s.name = '" + schema + "' AND o.name = '" + name + "'\n";

		try (Statement stmt = getConnection().createStatement(); ResultSet rs = stmt.executeQuery(query);){

			if(!rs.next()) throw new ExceptionDBGitObjectNotFound("");

			final String tname = rs.getString("triggerName");
			final String owner = rs.getString("owner");
			final String sql = rs.getString("ddl");
			final StringProperties options = new StringProperties(rs);

			return new DBTrigger(name, options, schema, owner, Collections.emptySet(), sql);

		} catch(Exception e) {
			final String msg = lang.getValue("errors", "adapter", "triggers").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}
	}

    @Override
    public DBTableData getTableData(String schema, String nameTable) {
		final String dataQuery = "SELECT * FROM [" + schema + "].[" + nameTable + "]";

		final int maxRowsCount = DBGitConfig.getInstance().getInteger(
			"core", "MAX_ROW_COUNT_FETCH",
			DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH)
		);

		final boolean isLimitedFetch = DBGitConfig.getInstance().getBoolean(
			"core", "LIMIT_FETCH",
			DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true)
		);

		try{

			if (isLimitedFetch) {
				final String rowsCountQuery =
					"SELECT COALESCE(SUM(PART.rows), 0) AS rowsCount\n" +
					"FROM sys.tables TBL\n" +
					"INNER JOIN sys.partitions PART ON TBL.object_id = PART.object_id\n" +
					"INNER JOIN sys.indexes IDX ON PART.object_id = IDX.object_id AND PART.index_id = IDX.index_id\n" +
					"INNER JOIN sys.schemas S ON S.schema_id = TBL.schema_id\n" +
					"WHERE TBL.name = '"+nameTable+"' AND S.name = '"+schema+"' AND IDX.index_id < 2\n" +
					"GROUP BY TBL.object_id, TBL.name";

				try(
					Statement st = getConnection().createStatement();
					ResultSet rs = st.executeQuery(rowsCountQuery);
				){
					if(!rs.next()) throw new ExceptionDBGitRunTime("rows coubt resultset is empty");
					if (rs.getInt("rowsCount") > maxRowsCount) {
						return new DBTableData(DBTableData.ERROR_LIMIT_ROWS);
					}
				}

			}

			return new DBTableData(getConnection(), dataQuery);

		} catch (Exception e){
			final String msg = DBGitLang.getInstance().getValue("errors", "adapter", "tableData").toString();
			throw new ExceptionDBGitRunTime(msg, e);
		}

    }

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {

		final int portionSize = DBGitConfig.getInstance().getInteger( "core", "PORTION_SIZE",
			DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000)
		);

		final int dataOffset = portionSize * portionIndex;
		final String dataQuery =
			"SELECT * " +
			"FROM " + schema + "." + nameTable + " " +
			"ORDER BY (SELECT NULL) " +
			"OFFSET " + dataOffset + " ROWS " +
			"FETCH NEXT " + portionSize + " ROWS ONLY ";

		try {

			/* For version <= SQL Server 2005

			int begin = 1 + portionSize*portionIndex;
			int end = portionSize + portionSize*portionIndex;

			ResultSet rs = st.executeQuery(
			"SELECT * FROM (" +
				"   SELECT  *, ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) rownum" +
				"   FROM dbo.Product" +
				") t" +
				"WHERE rownum BETWEEN " + begin + " AND "+ end
			);
			*/

			return new DBTableData(getConnection(), dataQuery);

		} catch (Exception e) {

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
       	final Map<String, DBUser> listUser = new HashMap<String, DBUser>();
		final String query =
			"DECLARE @crlf VARCHAR(2)\n" +
			"SELECT \n" +
			"	u.name userName, sp.name loginName, sp.default_database_name databaseName, dp.default_schema_name as schemaName,\n" +
			"	CASE WHEN sp.is_disabled IS NULL THEN 1 ELSE sp.is_disabled END isDisabledLogin,\n" +
			"	CASE WHEN sp.name IS NOT NULL THEN 'CREATE LOGIN [' + sp.name + '] WITH PASSWORD = ' \n" +
			"	+ UPPER(master.dbo.fn_varbintohexstr (CAST(LOGINPROPERTY(sp.name,'PASSWORDHASH') as VARBINARY (256)))) + ' HASHED; ' ELSE '' END \n" +
			"	+ CASE WHEN sp.is_disabled IS NOT NULL AND sp.is_disabled = 0 AND dr.permission_name IS NOT NULL THEN 'GRANT CONNECT SQL TO [' + sp.name + ']; ' ELSE '' END \n" +
			"	+ 'CREATE USER [' + u.name + '] ' \n" +
			"	+ CASE WHEN sp.name IS NOT NULL THEN 'FOR LOGIN [' + sp.name + ']' ELSE '' END\n" +
			"	+ CASE WHEN dp.default_schema_name IS NOT NULL THEN ' WITH DEFAULT_SCHEMA = [' + dp.default_schema_name + ']' ELSE '' END + ';' \n" +
			"	AS ddl, \n" +
			"	UPPER(master.dbo.fn_varbintohexstr (CAST(LOGINPROPERTY(sp.name,'PASSWORDHASH') as VARBINARY (256)))) passwordHash\n" +
			"FROM sys.sysusers u \n" +
			"INNER JOIN sys.database_principals dp ON dp.sid = u.sid\n" +
			"LEFT OUTER JOIN sys.server_principals sp ON sp.sid = u.sid\n" +
			"LEFT OUTER JOIN sys.database_permissions dr ON dr.grantee_principal_id = dp.principal_id AND dr.permission_name = 'CONNECT'\n" +
			"WHERE dp.type_desc = 'SQL_USER' AND u.name NOT IN ('dbo','guest') AND u.name NOT LIKE '##MS%'";

        try (
			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);
		){

            while(rs.next()){
                final String name = rs.getString(1);
                final StringProperties options = new StringProperties(rs);

                DBUser user = new DBUser(name, options);
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
		final String usersQuery =
			"SELECT \n" +
			"	dp.name roleName, dp.is_fixed_role isFixedRole,\n" +
			"	CASE WHEN dp.is_fixed_role = 0 AND dp.name != 'public' THEN dbo.GetRoleDDL(dp.name) ELSE '' " +
			"   END roleDDL,\n" +
			"	dbo.GetRoleMembersDDL(dp.name) membersDDL,\n" +
			"	CASE WHEN dp.is_fixed_role = 1 OR dp.name = 'public'" +
			"       THEN dbo.GetRoleMembersDDL(dp.name) " +
			"       ELSE dbo.GetRoleDDL(dp.name) + dbo.GetRoleMembersDDL(dp.name) " +
			"   END ddl\n" +
			"FROM sys.database_principals dp\n" +
			"WHERE dp.type = 'R'\n" +
			"DROP FUNCTION GetRoleDDL\n" +
			"DROP FUNCTION GetRoleMembersDDL";

		List<String> setupQueries = Arrays.asList(
			"IF OBJECT_ID(N'GetRoleDDL', N'FN') IS NOT NULL DROP FUNCTION GetRoleDDL\n" +
					"IF OBJECT_ID(N'GetRoleMembersDDL', N'FN') IS NOT NULL DROP FUNCTION GetRoleMembersDDL\n"
			,
			"CREATE FUNCTION dbo.GetRoleDDL(@roleName VARCHAR(255))\n" +
			"RETURNS VARCHAR(MAX)\n" +
			"BEGIN\n" +
			"	-- Script out the Role\n" +
			"	DECLARE @roleDesc VARCHAR(MAX)\n" +
			"	SET @roleDesc = 'CREATE ROLE [' + @roleName + '];'\n" +
			"	DECLARE @rolePerm VARCHAR(MAX)\n" +
			"	SET @rolePerm = ''\n" +
			"	SELECT    @rolePerm = @rolePerm +\n" +
			"			CASE dp.state\n" +
			"				WHEN 'D' THEN 'DENY '\n" +
			"				WHEN 'G' THEN 'GRANT '\n" +
			"				WHEN 'R' THEN 'REVOKE '\n" +
			"				WHEN 'W' THEN 'GRANT '\n" +
			"			END + \n" +
			"			dp.permission_name + ' ' +\n" +
			"			CASE dp.class\n" +
			"				WHEN 0 THEN ''\n" +
			"				WHEN 1 THEN --table or column subset on the table\n" +
			"					CASE WHEN dp.major_id < 0 THEN\n" +
			"						+ 'ON [sys].[' + OBJECT_NAME(dp.major_id) + '] '\n" +
			"					ELSE\n" +
			"						+ 'ON [' +\n" +
			"						(SELECT SCHEMA_NAME(schema_id) + '].[' + name FROM sys.objects WHERE object_id = dp.major_id)\n" +
			"							+ -- optionally concatenate column names\n" +
			"						CASE WHEN MAX(dp.minor_id) > 0 \n" +
			"							 THEN '] ([' + REPLACE(\n" +
			"											(SELECT name + '], [' \n" +
			"											 FROM sys.columns \n" +
			"											 WHERE object_id = dp.major_id \n" +
			"												AND column_id IN (SELECT minor_id \n" +
			"																  FROM sys.database_permissions \n" +
			"																  WHERE major_id = dp.major_id\n" +
			"																	AND USER_NAME(grantee_principal_id) IN (@roleName)\n" +
			"																 )\n" +
			"											 FOR XML PATH('')\n" +
			"											) --replace final square bracket pair\n" +
			"										+ '])', ', []', '')\n" +
			"							 ELSE ']'\n" +
			"						END + ' '\n" +
			"					END\n" +
			"				WHEN 3 THEN 'ON SCHEMA::[' + SCHEMA_NAME(dp.major_id) + '] '\n" +
			"				WHEN 4 THEN 'ON ' + (SELECT RIGHT(type_desc, 4) + '::[' + name FROM sys.database_principals WHERE principal_id = dp.major_id) + '] '\n" +
			"				WHEN 5 THEN 'ON ASSEMBLY::[' + (SELECT name FROM sys.assemblies WHERE assembly_id = dp.major_id) + '] '\n" +
			"				WHEN 6 THEN 'ON TYPE::[' + (SELECT name FROM sys.types WHERE user_type_id = dp.major_id) + '] '\n" +
			"				WHEN 10 THEN 'ON XML SCHEMA COLLECTION::[' + (SELECT SCHEMA_NAME(schema_id) + '.' + name FROM sys.xml_schema_collections WHERE xml_collection_id = dp.major_id) + '] '\n" +
			"				WHEN 15 THEN 'ON MESSAGE TYPE::[' + (SELECT name FROM sys.service_message_types WHERE message_type_id = dp.major_id) + '] '\n" +
			"				WHEN 16 THEN 'ON CONTRACT::[' + (SELECT name FROM sys.service_contracts WHERE service_contract_id = dp.major_id) + '] '\n" +
			"				WHEN 17 THEN 'ON SERVICE::[' + (SELECT name FROM sys.services WHERE service_id = dp.major_id) + '] '\n" +
			"				WHEN 18 THEN 'ON REMOTE SERVICE BINDING::[' + (SELECT name FROM sys.remote_service_bindings WHERE remote_service_binding_id = dp.major_id) + '] '\n" +
			"				WHEN 19 THEN 'ON ROUTE::[' + (SELECT name FROM sys.routes WHERE route_id = dp.major_id) + '] '\n" +
			"				WHEN 23 THEN 'ON FULLTEXT CATALOG::[' + (SELECT name FROM sys.fulltext_catalogs WHERE fulltext_catalog_id = dp.major_id) + '] '\n" +
			"				WHEN 24 THEN 'ON SYMMETRIC KEY::[' + (SELECT name FROM sys.symmetric_keys WHERE symmetric_key_id = dp.major_id) + '] '\n" +
			"				WHEN 25 THEN 'ON CERTIFICATE::[' + (SELECT name FROM sys.certificates WHERE certificate_id = dp.major_id) + '] '\n" +
			"				WHEN 26 THEN 'ON ASYMMETRIC KEY::[' + (SELECT name FROM sys.asymmetric_keys WHERE asymmetric_key_id = dp.major_id) + '] '\n" +
			"			 END COLLATE SQL_Latin1_General_CP1_CI_AS\n" +
			"			 + 'TO [' + @roleName + ']' + \n" +
			"			 CASE dp.state WHEN 'W' THEN ' WITH GRANT OPTION' ELSE '' END + ';'\n" +
			"	FROM    sys.database_permissions dp\n" +
			"	WHERE    USER_NAME(dp.grantee_principal_id) IN (@roleName)\n" +
			"	GROUP BY dp.state, dp.major_id, dp.permission_name, dp.class\n" +
			"	SELECT @roleDesc = @roleDesc + CASE WHEN @rolePerm IS NOT NULL THEN @rolePerm ELSE '' END\n" +
			"	RETURN @roleDesc\n" +
			"END \n"
			,
			"CREATE FUNCTION dbo.GetRoleMembersDDL(@roleName VARCHAR(255))\n" +
			"RETURNS VARCHAR(MAX)\n" +
			"BEGIN\n" +
			"	-- Script out the Role\n" +
			"	DECLARE @roleDesc VARCHAR(MAX)\n" +
			"	SET @roleDesc = ''\n" +
			"	-- Display users within Role.  Code stubbed by Joe Spivey\n" +
			"	SELECT	@roleDesc = @roleDesc  + 'EXECUTE sp_AddRoleMember ''' + roles.name + ''', ''' + users.name + ''';' \n" +
			"	FROM	sys.database_principals users\n" +
			"	INNER JOIN sys.database_role_members link \n" +
			"		ON link.member_principal_id = users.principal_id\n" +
			"	INNER JOIN sys.database_principals roles \n" +
			"		ON roles.principal_id = link.role_principal_id\n" +
			"	WHERE roles.name = @roleName\n" +
			"	RETURN @roleDesc\n" +
			"END \n"
		);

		try (Statement stmt = getConnection().createStatement();) {

			for(String expr : setupQueries){
				stmt.execute(expr);
			}

			try(ResultSet rs = stmt.executeQuery(usersQuery);){
				while(rs.next()){
					final String name = rs.getString("rolename");
					final StringProperties options = new StringProperties(rs);

					DBRole role = new DBRole(name, options);
					listRole.put(name, role);
				}
			}

		}catch(Exception e) {
			final DBGitLang msg = lang.getValue("errors", "adapter", "roles");
			throw new ExceptionDBGitRunTime(msg, e);
		}
		return listRole;
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() {
	    try{

            Connection connect = getConnection();
            Statement stmt = connect.createStatement();
            ResultSet rs = stmt.executeQuery(
            "SELECT CASE WHEN EXISTS " +
                "(SELECT * FROM fn_my_permissions(NULL, 'DATABASE') WHERE permission_name LIKE '%DEFINITION%') " +
                "THEN 1 ELSE 0 END hasRights;"
            );
            rs.next();
            boolean hasRights = rs.getBoolean(1);
            stmt.close();
            return hasRights;

        }catch(Exception e) {
            logger.error(lang.getValue("errors", "adapter", "roles") + ": " + e.getMessage());
            throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "roles") + ": " + e.getMessage());
        }
	}

	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		return backupFactory;
	}

	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return convertFactory;
	}

	@Override
	public DbType getDbType() {
		return DbType.MSSQL;
	}

    @Override
    public String getDbVersion() {
        try {
            Statement stmt = getConnection().createStatement();

            //Gives 8.00, 9.00, 10.00 and 10.50 for SQL 2000, 2005, 2008 and 2008R2 respectively.
            String query = "SELECT left(cast(serverproperty('productversion') as varchar), 4)";

            ResultSet resultSet = stmt.executeQuery(query);
            resultSet.next();
            String result = resultSet.getString(1);

            resultSet.close();
            stmt.close();
            return result;

        } catch (SQLException e) { return "";}
    }

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		try {
			if(!getSchemes().containsKey(schemaName)) {
				StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());
				stLog.execute(
				"IF NOT EXISTS ( SELECT  * FROM sys.schemas WHERE name = N'" + schemaName + "' )\n" +
					"EXEC('CREATE SCHEMA ["+schemaName+"]');"
				);

				stLog.close();
			}
		} catch (SQLException e) {
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "createSchema") + ": " + e.getLocalizedMessage());
		}

	}

	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		try {
			if(!getRoles().containsKey(roleName)) {
				StatementLogging stLog = new StatementLogging(connect, getStreamOutputSqlCommand(), isExecSql());
				stLog.execute(
				"IF NOT EXISTS ( SELECT  * FROM sys.schemas WHERE name = N'" + roleName + "' )\n" +
					"EXEC('CREATE ROLE ["+roleName+"]');"
				);

				stLog.close();
			}
		} catch (SQLException e) {
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "createSchema") + ": " + e.getLocalizedMessage());
		}

	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		try{
			Statement stmt = getConnection().createStatement();

			String query = "SELECT SCHEMA_NAME()";

			ResultSet resultSet = stmt.executeQuery(query);
			resultSet.next();
			String result = resultSet.getString(1);

			resultSet.close();
			stmt.close();
			return result;
		}
		catch (SQLException e){
			throw new ExceptionDBGit(lang.getValue("errors", "adapter", "createSchema") + ": " + e.getLocalizedMessage());
		}
	}

	@Override
	@SuppressWarnings("SpellCheckingInspection")
	public boolean isReservedWord(String word) {

		Set<String> reservedWords = new HashSet<>(Arrays.asList(
				"DD", "EXTERNAL", "PROCEDURE", "ALL", "FETCH", "PUBLIC", "ALTER", "FILE", "RAISERROR",
				"AND", "FILLFACTOR", "READ", "ANY", "FOR", "READTEXT", "AS", "FOREIGN", "RECONFIGURE",
				"ASC", "FREETEXT", "REFERENCES", "AUTHORIZATION", "FREETEXTTABLE", "REPLICATION",
				"BACKUP", "FROM", "RESTORE", "BEGIN", "FULL", "RESTRICT", "BETWEEN", "FUNCTION", "RETURN",
				"BREAK", "GOTO", "REVERT", "BROWSE", "GRANT", "REVOKE", "BULK", "GROUP", "RIGHT", "BY",
				"HAVING", "ROLLBACK", "CASCADE", "HOLDLOCK", "ROWCOUNT", "CASE", "IDENTITY", "ROWGUIDCOL",
				"CHECK", "IDENTITY_INSERT", "RULE", "CHECKPOINT", "IDENTITYCOL", "SAVE", "CLOSE", "IF",
				"SCHEMA", "CLUSTERED", "IN", "SECURITYAUDIT", "COALESCE", "INDEX", "SELECT", "COLLATE",
				"INNER", "SEMANTICKEYPHRASETABLE", "COLUMN", "INSERT", "SEMANTICSIMILARITYDETAILSTABLE",
				"COMMIT", "INTERSECT", "SEMANTICSIMILARITYTABLE", "COMPUTE", "INTO", "SESSION_USER",
				"CONSTRAINT", "IS", "SET", "CONTAINS", "JOIN", "SETUSER", "CONTAINSTABLE", "KEY", "SHUTDOWN",
				"CONTINUE", "KILL", "SOME", "CONVERT", "LEFT", "STATISTICS", "CREATE", "LIKE", "SYSTEM_USER",
				"CROSS", "LINENO", "TABLE", "CURRENT", "LOAD", "TABLESAMPLE", "CURRENT_DATE", "MERGE",
				"TEXTSIZE", "CURRENT_TIME", "NATIONAL", "THEN", "CURRENT_TIMESTAMP", "NOCHECK", "TO",
				"CURRENT_USER", "NONCLUSTERED", "В начало", "CURSOR", "NOT", "TRAN", "DATABASE", "NULL",
				"TRANSACTION", "DBCC", "NULLIF", "TRIGGER", "DEALLOCATE", "OF", "TRUNCATE", "DECLARE",
				"OFF", "TRY_CONVERT", "DEFAULT", "OFFSETS", "TSEQUAL", "DELETE", "ON", "UNION", "DENY",
				"OPEN", "UNIQUE", "DESC", "OPENDATASOURCE", "UNPIVOT", "DISK", "OPENQUERY", "UPDATE",
				"DISTINCT", "OPENROWSET", "UPDATETEXT", "DISTRIBUTED", "OPENXML", "USE", "DOUBLE",
				"OPTION", "ПОЛЬЗОВАТЕЛЬ", "DROP", "OR", "VALUES", "DUMP", "OVER", "WAITFOR", "ERRLVL",
				"PERCENT", "PIVOT", "PLAN", "WHILE", "на", "PRINT", "WRITETEXT", "EXIT", "PROC", "OVERLAPS",
				"ADA", "ADD", "EXTERNAL", "PASCAL", "ALL", "EXTRACT", "POSITION", "PRECISION", "ALTER",
				"FETCH", "AND", "ANY", "PRIMARY", "FOR", "СЛУЖБЫ", "ANALYSIS SERVICES", "FOREIGN",
				"ASC", "FORTRAN", "PROCEDURE", "PUBLIC", "FROM", "READ", "AUTHORIZATION", "ПОЛНОЕ",
				"REAL", "AVG", "REFERENCES", "BEGIN", "BETWEEN", "RESTRICT", "GOTO", "REVOKE", "BIT_LENGTH",
				"GRANT", "RIGHT", "GROUP", "ROLLBACK", "BY", "HAVING", "CASCADE", "SCHEMA", "IDENTITY",
				"CASE", "IN", "INCLUDE", "SELECT", "INDEX", "CHAR_LENGTH", "SESSION_USER", "SET",
				"CHARACTER_LENGTH", "INNER", "CHECK", "CLOSE", "INSENSITIVE", "SOME", "COALESCE", "INSERT",
				"COLLATE", "SQLCA", "COLUMN", "INTERSECT", "SQLCODE", "COMMIT", "SQLERROR", "INTO", "IS",
				"CONSTRAINT", "SUBSTRING", "JOIN", "SUM", "CONTINUE", "KEY", "SYSTEM_USER", "CONVERT",
				"TABLE", "COUNT", "THEN", "CREATE", "LEFT", "CROSS", "TIMESTAMP", "CURRENT", "LIKE",
				"CURRENT_DATE", "CURRENT_TIME", "LOWER", "Кому", "CURRENT_TIMESTAMP", "CURRENT_USER",
				"MAX", "TRANSACTION", "CURSOR", "MIN", "TRANSLATE", "TRIM", "DEALLOCATE", "UNION", "NATIONAL",
				"UNIQUE", "DECLARE", "DEFAULT", "UPDATE", "UPPER", "DELETE", "NONE", "USER", "DESC", "NOT",
				"NULL", "VALUE", "NULLIF", "VALUES", "OCTET_LENGTH", "VARYING", "DISTINCT", "OF", "VIEW",
				"ON", "WHEN", "DOUBLE", "DROP", "OPEN", "WHERE", "ELSE", "OPTION", "WITH", "END", "OR", "ORDER",
				"ESCAPE", "OUTER", "EXCEPT", "ABSOLUTE", "HOST", "RELATIVE", "ACTION", "HOUR", "RELEASE",
				"ADMIN", "IGNORE", "RESULT", "AFTER", "IMMEDIATE", "RETURNS", "AGGREGATE", "INDICATOR",
				"ROLE", "ALIAS", "INITIALIZE", "ROLLUP", "ALLOCATE", "INITIALLY", "ROUTINE", "ARE", "INOUT",
				"ROW", "ARRAY", "INPUT", "ROWS", "ASENSITIVE", "INT", "SAVEPOINT", "ASSERTION", "INTEGER",
				"SCROLL", "ASYMMETRIC", "INTERSECTION", "SCOPE", "AT", "INTERVAL", "SEARCH", "ATOMIC",
				"ISOLATION", "SECOND", "BEFORE", "ITERATE", "SECTION", "BINARY", "LANGUAGE", "SENSITIVE",
				"BIT", "LARGE", "SEQUENCE", "BLOB", "LAST", "SESSION", "BOOLEAN", "LATERAL", "SETS", "BOTH",
				"LEADING", "SIMILAR", "BREADTH", "LESS", "SIZE", "CALL", "LEVEL", "SMALLINT", "CALLED",
				"LIKE_REGEX", "SPACE", "CARDINALITY", "LIMIT", "SPECIFIC", "CASCADED", "LN", "SPECIFICTYPE",
				"CAST", "LOCAL", "SQL", "CATALOG", "LOCALTIME", "SQLEXCEPTION", "CHAR", "LOCALTIMESTAMP",
				"SQLSTATE", "CHARACTER", "LOCATOR", "SQLWARNING", "CLASS", "MAP", "START", "CLOB", "MATCH",
				"STATE", "COLLATION", "MEMBER", "STATEMENT", "COLLECT", "METHOD", "STATIC", "COMPLETION",
				"MINUTE", "STDDEV_POP", "CONDITION", "MOD", "STDDEV_SAMP", "CONNECT", "MODIFIES", "STRUCTURE",
				"CONNECTION", "MODIFY", "SUBMULTISET", "CONSTRAINTS", "MODULE", "SUBSTRING_REGEX",
				"CONSTRUCTOR", "MONTH", "SYMMETRIC", "CORR", "MULTISET", "SYSTEM", "CORRESPONDING",
				"NAMES", "TEMPORARY", "COVAR_POP", "NATURAL", "TERMINATE", "COVAR_SAMP", "NCHAR", "THAN",
				"CUBE", "NCLOB", "TIME", "CUME_DIST", "NEW", "TIMESTAMP", "CURRENT_CATALOG", "NEXT",
				"TIMEZONE_HOUR", "CURRENT_DEFAULT_TRANSFORM_GROUP", "NO", "TIMEZONE_MINUTE",
				"CURRENT_PATH", "None", "TRAILING", "CURRENT_ROLE", "NORMALIZE", "TRANSLATE_REGEX",
				"CURRENT_SCHEMA", "NUMERIC", "TRANSLATION", "CURRENT_TRANSFORM_GROUP_FOR_TYPE",
				"OBJECT", "TREAT", "CYCLE", "OCCURRENCES_REGEX", "TRUE", "DATA", "OLD", "UESCAPE", "DATE",
				"ONLY", "UNDER", "DAY", "OPERATION", "UNKNOWN", "DEC", "ORDINALITY", "UNNEST", "DECIMAL",
				"OUT", "USAGE", "DEFERRABLE", "OVERLAY", "USING", "DEFERRED", "OUTPUT", "Value", "DEPTH",
				"PAD", "VAR_POP", "DEREF", "Параметр", "VAR_SAMP", "DESCRIBE", "PARAMETERS", "VARCHAR",
				"DESCRIPTOR", "PARTIAL", "VARIABLE", "DESTROY", "PARTITION", "WHENEVER", "DESTRUCTOR",
				"PATH", "WIDTH_BUCKET", "DETERMINISTIC", "POSTFIX", "WITHOUT", "DICTIONARY", "PREFIX",
				"WINDOW", "DIAGNOSTICS", "PREORDER", "WITHIN", "DISCONNECT", "PREPARE", "WORK", "DOMAIN",
				"PERCENT_RANK", "WRITE", "DYNAMIC", "PERCENTILE_CONT", "XMLAGG", "EACH", "PERCENTILE_DISC",
				"XMLATTRIBUTES", "ELEMENT", "POSITION_REGEX", "XMLBINARY", "END-EXEC", "PRESERVE",
				"XMLCAST", "EQUALS", "PRIOR", "XMLCOMMENT", "EVERY", "PRIVILEGES", "XMLCONCAT",
				"EXCEPTION", "RANGE", "XMLDOCUMENT", "FALSE", "READS", "XMLELEMENT", "FILTER", "REAL",
				"XMLEXISTS", "FIRST", "RECURSIVE", "XMLFOREST", "FLOAT", "REF", "XMLITERATE", "FOUND",
				"REFERENCING", "XMLNAMESPACES", "FREE", "REGR_AVGX", "XMLPARSE", "FULLTEXTTABLE",
				"REGR_AVGY", "XMLPI", "FUSION", "REGR_COUNT", "XMLQUERY", "GENERAL", "REGR_INTERCEPT",
				"XMLSERIALIZE", "GET", "REGR_R2", "XMLTABLE", "GLOBAL", "REGR_SLOPE", "XMLTEXT", "GO",
				"REGR_SXX", "XMLVALIDATE", "GROUPING", "REGR_SXY", "YEAR", "HOLD", "REGR_SYY", "ZONE"
		));


		String[] words = word.split("\\s");
		for (String str : words) {
			if(reservedWords.contains(str.toUpperCase())) return true;
		}
		return false;
	}

}
