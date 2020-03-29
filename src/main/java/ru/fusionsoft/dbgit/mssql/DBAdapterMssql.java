package ru.fusionsoft.dbgit.mssql;

import org.slf4j.Logger;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.data_table.*;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

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
		
		String query = "SELECT           \n" +
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
		Map<String, DBTableSpace> listTableSpace = new HashMap<String, DBTableSpace>();
		try {
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("File Group Name");
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

	private DBSequence sequenceFromResultSet(ResultSet rs, String schema){
		try {
			String nameSeq = rs.getString("name");
			Long valueSeq = rs.getLong("current_value");
			DBSequence sequence = new DBSequence();
			sequence.setName(nameSeq);
			sequence.setSchema(schema);
			sequence.setValue(valueSeq);
			rowToProperties(rs, sequence.getOptions());
			return sequence;
		} catch (Exception ex){
			logger.error(ex.getMessage(), ex);
			throw new ExceptionDBGitRunTime(ex.getMessage(), ex);
		}
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
        Map<String, DBSequence> listSequence = new HashMap<String, DBSequence>();
        try {
            Connection connect = getConnection();
            String query =
                "SELECT seq.*,\n" +
                "TYPE_NAME(seq.system_type_id) as typeName,\n" +
                "SCHEMA_NAME(seq.schema_id) as owner \n" +
                "FROM sys.objects, sys.SEQUENCES seq \n" +
                "WHERE sys.objects.object_id = seq.object_id \n" +
                "AND SCHEMA_NAME(seq.schema_id) = '"+schema+"'";

            Statement stmt = connect.createStatement();
            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
				String nameSeq = rs.getString("name");
                listSequence.put(nameSeq, sequenceFromResultSet(rs, schema));
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
                "SELECT seq.*,\n" +
                "USER_NAME(objectproperty(seq.object_id,'OwnerId')) as owner,\n" +
                "TYPE_NAME(seq.system_type_id) as typeName, " +
                "SCHEMA_NAME(seq.schema_id) as schemaName " +
                "FROM sys.objects, sys.SEQUENCES seq " +
                "WHERE sys.objects.object_id = seq.object_id " +
                "AND SCHEMA_NAME(seq.schema_id) = '"+schema+"' " +
                "AND seq.name = '" + name + "'\n";

            Statement stmt = connect.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            DBSequence sequence = null;
            while (rs.next()) {
				sequence = sequenceFromResultSet(rs, schema);
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
            String query =
                "SELECT TABLE_NAME as 'name', TABLE_CATALOG as 'database', TABLE_SCHEMA as 'schema'\n" +
                "FROM INFORMATION_SCHEMA.TABLES \n" +
                "WHERE INFORMATION_SCHEMA.TABLES.TABLE_SCHEMA = '" + schema + "'\n" +
                "AND INFORMATION_SCHEMA.TABLES.TABLE_TYPE = 'BASE TABLE'";
            Connection connect = getConnection();

            Statement stmt = connect.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while(rs.next()){
                String nameTable = rs.getString("name");
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
        DBTable table = null;
        try(Statement stmt = getConnection().createStatement()) {
            String query =
				"SELECT\n" +
                "	o.name tableName, t.TABLE_SCHEMA schemaName, t.TABLE_CATALOG catalogName,\n" +
                "	CASE WHEN o.principal_id is NOT NULL THEN (SELECT name FROM sys.database_principals dp WHERE dp.principal_id=o.principal_id)\n" +
                "	ELSE (SELECT dp.name FROM sys.database_principals dp,sys.schemas s WHERE s.schema_id=o.schema_id and s.principal_id=dp.principal_id)\n" +
                "	END as owner\n" +
                "FROM sys.objects o, INFORMATION_SCHEMA.TABLES t\n" +
                "WHERE o.type='U' AND o.name = t.TABLE_NAME AND t.TABLE_NAME = '"+name+"' AND t.TABLE_SCHEMA = '"+schema+"'";

            ResultSet rs = stmt.executeQuery(query);

            while(rs.next()){
                String nameTable = rs.getString("tableName");
                table = new DBTable(nameTable);
                table.setSchema(schema);
                rowToProperties(rs, table.getOptions());
            }
            return table;
        }catch(Exception e) {
            logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);
            throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
        }
	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		Map<String, DBTableField> listField = new HashMap<>();
		try(Statement stmt = getConnection().createStatement()) {
			String query =
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

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBTableField field = DBTableFieldFromRs(rs);
				listField.put(field.getName(), field);
			}
			return listField;
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}
	}

	private DBTableField DBTableFieldFromRs(ResultSet rs) throws SQLException {
		DBTableField field = new DBTableField();
		field.setName(rs.getString("columnName").toLowerCase());
		if (rs.getString("isPk").equals("1")) {
			field.setIsPrimaryKey(true);
		}
		field.setTypeSQL(getFieldType(rs));
		field.setTypeMapping(getTypeMapping(rs));
		field.setTypeUniversal(rs.getString("dbgitType"));
		field.setLength(rs.getInt("length"));
		field.setScale(rs.getInt("scale"));
		field.setPrecision(rs.getInt("precision"));
		field.setFixed(rs.getBoolean("isFixed"));
		field.setOrder(rs.getInt("columnOrder"));
		return field;
	}

	protected String getTypeMapping(ResultSet rs) throws SQLException {
		String tp = rs.getString("dbgitType");
		if (FactoryCellData.contains(tp) )
			return tp;

		return DEFAULT_MAPPING_TYPE;
	}

	protected String getFieldType(ResultSet rs) {
		try {
			StringBuilder type = new StringBuilder();
			type.append(rs.getString("mssqlType"));

			Integer max_length = rs.getInt("length");
			if (!rs.wasNull()) {
				type.append("("+max_length.toString()+")");
			}
			if (rs.getString("isNullable").equals("NO")){
				type.append(" NOT NULL");
			}

			return type.toString();
		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "tables").toString(), e);
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "tables").toString(), e);
		}
	}

	public Map<String, DBIndex> getIndexesWithPks(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<>();
		try (Statement stmt = getConnection().createStatement()){
			String query =
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

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				DBIndex index = new DBIndex();
				index.setName(rs.getString("indexName"));
				index.setSchema(schema);
				rowToProperties(rs, index.getOptions());
				indexes.put(index.getName(), index);
			}

			return indexes;

		}catch(Exception e) {
			logger.error(lang.getValue("errors", "adapter", "indexes").toString());
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "indexes").toString(), e);
		}
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable){
		Map<String, DBIndex> indexes = getIndexesWithPks(schema, nameTable);
		indexes.values().removeIf(x->x.getOptions().getChildren().get("ispk").getData().equals("1"));
		return indexes;
	}

    @Override
    public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
        Map<String, DBConstraint> constraints = new HashMap<>();
        ArrayList<String> queries = new ArrayList<>();
        //TODO [] in object names
        //check
        queries.add("SELECT sc.name as schemaName, t.name as tableName, col.name as columnName, c.name as constraintName, c.name as indexName, c.type_desc as constraintType, \n" +
                "'ALTER TABLE ' + sc.name + '.' + t.name + ' ADD CONSTRAINT ' + c.name + ' CHECK ' + c.definition + ';' as ddl\n" +
                "FROM sys.check_constraints c\n" +
                "JOIN sys.tables t ON c.parent_object_id = t.object_id \n" +
                "LEFT OUTER JOIN sys.columns col on col.column_id = c.parent_column_id AND col.object_id = c.parent_object_id\n" +
                "JOIN sys.schemas AS sc ON t.schema_id=sc.schema_id \n" +
				"WHERE t.name = ? AND sc.name = ?");
        //default
        queries.add("SELECT sc.name AS schemaName, t.name AS tableName, col.name AS columnName, c.name AS constraintName, c.type_desc AS constraintType, \n" +
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
				"WHERE t.name = ? AND sc.name = ?\n");
        //unique
        queries.add("SELECT TC.TABLE_SCHEMA AS schemaName, TC.TABLE_NAME AS tableName, CC.Column_Name AS columnName, TC.Constraint_Name AS constraintName, TC.CONSTRAINT_TYPE AS constraintType,\n" +
                "'ALTER TABLE ' + TC.TABLE_SCHEMA + '.' + TC.TABLE_NAME + ' ADD CONSTRAINT ' + TC.CONSTRAINT_NAME + ' UNIQUE NONCLUSTERED ([' + CC.COLUMN_NAME + ']);' AS ddl\n" +
                "FROM INFORMATION_SCHEMA.table_constraints TC\n" +
                "INNER JOIN INFORMATION_SCHEMA.constraint_column_usage CC on TC.Constraint_Name = CC.Constraint_Name\n" +
                "WHERE TC.constraint_type = 'Unique' AND TC.TABLE_NAME = ? AND TC.TABLE_SCHEMA = ? ---- PARAMETER 1,2\n");
        //foreign
        queries.add("SELECT ss.name as schemaName, t.name as tableName, sc.name as columnName, o.name as constraintName, o.type_desc as constraintType, refss.name as refSchemaName, refst.name as refTableName, refsc.name as refColumnName, " +
				"'ALTER TABLE ' + ss.name + '.' + t.name + ' ADD CONSTRAINT ' + o.name + ' FOREIGN KEY ('+ sc.name + ') references ' + refss.name + '.' + refst.name + '(' + refsc.name + ');' as ddl\n" +
                "FROM sys.foreign_key_columns c\n" +
                "JOIN sys.objects o ON c.constraint_object_id = o.object_id\n" +
                "LEFT OUTER JOIN sys.tables t on t.object_id = c.parent_object_id \n" +
				"LEFT OUTER JOIN sys.schemas ss on ss.schema_id = o.schema_id \n" +
				"LEFT OUTER JOIN sys.columns sc on sc.object_id = c.parent_object_id AND sc.column_id = c.parent_column_id\n" +
                "LEFT OUTER JOIN sys.tables refst on refst.object_id = c.referenced_object_id\n" +
                "LEFT OUTER JOIN sys.schemas refss on refss.schema_id = refst.schema_id\n" +
                "LEFT OUTER JOIN sys.columns refsc on refsc.object_id = c.referenced_object_id AND refsc.column_id = c.referenced_column_id \n" +
				"WHERE t.name = ? AND ss.name = ?\n"
		);



        Iterator<String> it = queries.iterator();
        try {
            while (it.hasNext()) {
                String query = it.next();
				PreparedStatement stmt = connect.prepareStatement(query);
                stmt.setString(2, schema);
                stmt.setString(1, nameTable);
                ResultSet rs = stmt.executeQuery();


                while (rs.next()) {
                    DBConstraint con = new DBConstraint();
                    con.setName(rs.getString("constraintName"));
                    con.setConstraintType(rs.getString("constraintType"));
                    con.setSchema(schema);
                    rowToProperties(rs, con.getOptions());
                    constraints.put(con.getName(), con);
                }
                stmt.close();
            }

            //primary keys
			Map<String, DBIndex> indexes = getIndexesWithPks(schema, nameTable);
			indexes.values().removeIf(x->x.getOptions().getChildren().get("ispk").getData().equals("0"));
			for(DBIndex pki:indexes.values()){
				DBConstraint pkc = new DBConstraint();
				pkc.setName(pki.getName());
				pkc.setConstraintType(pki.getOptions().getChildren().get("typename").getData());
				pkc.setSchema(pki.getSchema());
				pkc.setOptions(pki.getOptions());
				constraints.put(pkc.getName(), pkc);
			}

			return constraints;

        }catch(Exception e) {
            logger.error(lang.getValue("errors", "adapter", "constraints").toString());
            throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "constraints").toString(), e);
        }
    }

    @Override
    public Map<String, DBView> getViews(String schema) {
        Map<String, DBView> listView = new HashMap<String, DBView>();
        try (Statement stmt = getConnection().createStatement()){
            String query =
                "SELECT \n" +
                "	sp.name as ownerName, sp.type_desc as ownerType, ss.name AS schemaName, sv.name AS viewName, sm.definition as ddl, \n" +
                "	sv.type_desc as typeName, sm.uses_ansi_nulls, sm.uses_quoted_identifier, sm.is_schema_bound, \n" +
                "	OBJECTPROPERTYEX(sv.object_id,'IsIndexable') AS IsIndexable,\n" +
                "	OBJECTPROPERTYEX(sv.object_id,'IsIndexed') AS IsIndexed\n" +
                "FROM sys.views sv\n" +
                "JOIN sys.schemas ss ON sv.schema_id = ss.schema_id\n" +
                "LEFT OUTER JOIN sys.sql_modules sm on sv.object_id = sm.object_id\n" +
                "LEFT OUTER JOIN sys.database_principals sp on sv.principal_id = sp.principal_id";

            ResultSet rs = stmt.executeQuery(query);
            while(rs.next()){
                DBView view = new DBView(rs.getString("viewName"));
                view.setSchema(rs.getString("schemaName"));
                view.setOwner(rs.getString("ownerName"));
                rowToProperties(rs, view.getOptions());
                listView.put(rs.getString("viewName"), view);
            }
            return listView;
        }catch(Exception e) {
            logger.error(lang.getValue("errors", "adapter", "views") + ": "+ e.getMessage());
            throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views") + ": " + e.getMessage());
        }
    }

    @Override
    public DBView getView(String schema, String name) {
        try {
            return getViews(schema).get(name);

        }catch(Exception e) {
            logger.error(lang.getValue("errors", "adapter", "views").toString() + ": "+ e.getMessage());
            throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "views").toString() + ": "+ e.getMessage());
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
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		Map<String, DBProcedure> listProcedure = new HashMap<String, DBProcedure>();
		try (Statement stmt = getConnection().createStatement()){
			String query =
				"SELECT s.name schemaName, o.name procedureName, o.type_desc as typeName, definition ddl, USER_NAME(so.uid) AS owner \n" +
				"FROM sys.sql_modules m\n" +
				"JOIN sys.procedures p ON m.object_id = p.object_id\n" +
				"JOIN sys.objects o \n" +
				"	ON o.object_id = p.object_id \n" +
				"	AND Left(o.name, 3) NOT IN ('sp_', 'xp_', 'ms_') \n" +
				"JOIN sys.schemas s ON s.schema_id = o.schema_id\n" +
				"JOIN sysobjects so on o.object_id = so.id\n" +
				"WHERE s.name = '" + schema + "'\n";

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("procedureName");
				String owner = rs.getString("owner");
				DBProcedure proc = new DBProcedure(name);
				proc.setSchema(schema);
				proc.setOwner(owner);
				proc.setName(name);
				rowToProperties(rs,proc.getOptions());
				listProcedure.put(name, proc);
			}
			stmt.close();
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "prc").toString(), e);
		}
		return listProcedure;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		try (Statement stmt = getConnection().createStatement()){
			String query =
				"SELECT s.name schemaName, o.name procedureName, o.type_desc as typeName, definition ddl, USER_NAME(so.uid) AS owner \n" +
				"FROM sys.sql_modules m\n" +
				"JOIN sys.procedures p ON m.object_id = p.object_id\n" +
				"JOIN sys.objects o \n" +
				"	ON o.object_id = p.object_id \n" +
				"	AND Left(o.name, 3) NOT IN ('sp_', 'xp_', 'ms_') -- filter out system ones\n" +
				"JOIN sys.schemas s ON s.schema_id = o.schema_id\n" +
				"JOIN sysobjects so on o.object_id = so.id \n" +
				"WHERE s.name = '" + schema + "' AND o.name = '" + name + "'";
			ResultSet rs = stmt.executeQuery(query);
			DBProcedure proc = null;

			while (rs.next()) {
				proc = new DBProcedure(rs.getString("procedureName"));
				String owner = rs.getString("owner");
				proc.setSchema(schema);
				proc.setOwner(owner);
				rowToProperties(rs,proc.getOptions());
			}
			stmt.close();

			return proc;

		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "prc").toString(), e);
		}
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		Map<String, DBFunction> listFunction = new HashMap<>();
		try (Statement stmt = getConnection().createStatement()){
			String query =
				"SELECT ss.name schemaName, o.name functionName, type_desc typeName, definition ddl, USER_NAME(so.uid) owner \n" +
				"FROM sys.sql_modules m \n" +
				"INNER JOIN sys.objects o ON m.object_id = o.object_id\n" +
				"INNER JOIN sysobjects so ON m.object_id = so.id\n" +
				"INNER JOIN sys.schemas ss ON ss.schema_id = o.schema_id\n" +
				"WHERE type_desc like '%function%' AND ss.name = '" + schema + "'\n";

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("functionName");
				String owner = rs.getString("owner");
				DBFunction func = new DBFunction(name);
				func.setSchema(schema);
				func.setOwner(owner);
				rowToProperties(rs,func.getOptions());

				listFunction.put(name, func);
			}
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
		}
		return listFunction;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		try (Statement stmt = getConnection().createStatement()){
			String query =
                    "SELECT ss.name schemaName, o.name functionName, type_desc typeName, definition ddl, USER_NAME(so.uid) owner \n" +
                    "FROM sys.sql_modules m \n" +
                    "INNER JOIN sys.objects o ON m.object_id = o.object_id\n" +
                    "INNER JOIN sysobjects so ON m.object_id = so.id\n" +
                    "INNER JOIN sys.schemas ss ON ss.schema_id = o.schema_id\n" +
                    "WHERE type_desc like '%function%' AND ss.name = '" + schema + "' AND o.name = '" + name + "'\n";

			DBFunction func = null;
			ResultSet rs = stmt.executeQuery(query);
			while (rs.next()) {
				func = new DBFunction(rs.getString("functionName"));
				String owner = rs.getString("owner");
				func.setSchema(schema);
				func.setOwner(owner);
				rowToProperties(rs,func.getOptions());
			}
			return func;

		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "fnc").toString(), e);
		}
	}

	//TODO Discuss scenario when we get an encrypted TRIGGER, IMO display a warning,
	// it is not possible to get definition of an encrypted trigger

	public Map<String, DBTrigger> getTriggers(String schema) {
		Map<String, DBTrigger> listTrigger = new HashMap<String, DBTrigger>();
		try (Statement stmt = getConnection().createStatement()){
			String query =
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

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("triggerName");
				String owner = rs.getString("owner");
				DBTrigger trigger = new DBTrigger(name);
				trigger.setSchema(schema);
				trigger.setOwner(owner);
				// -- what means owner? oracle/postgres or owner like database user/schema
				// -- IMO its a database object owner

				rowToProperties(rs, trigger.getOptions());
				listTrigger.put(name, trigger);
			}
			return listTrigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);
		}
	}

	public DBTrigger getTrigger(String schema, String name) {
		DBTrigger trigger = null;
		try (Statement stmt = getConnection().createStatement()){
			String query =
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

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String tname = rs.getString("triggerName");
				String owner = rs.getString("owner");
				trigger = new DBTrigger(tname);
				trigger.setSchema(schema);
				trigger.setOwner(owner);
				rowToProperties(rs, trigger.getOptions());
			}
			return trigger;
		}catch(Exception e) {
			throw new ExceptionDBGitRunTime(lang.getValue("errors", "adapter", "triggers").toString(), e);
		}
	}

    @Override
    public DBTableData getTableData(String schema, String nameTable) {
        try {
			Statement stmt = getConnection().createStatement();
            DBTableData data = new DBTableData();

			int maxRowsCount = DBGitConfig.getInstance().getInteger(
				"core",
				"MAX_ROW_COUNT_FETCH",
				DBGitConfig.getInstance().getIntegerGlobal("core", "MAX_ROW_COUNT_FETCH", MAX_ROW_COUNT_FETCH)
            );

            boolean isLimitedFetch = DBGitConfig.getInstance().getBoolean(
                "core",
                "LIMIT_FETCH",
                DBGitConfig.getInstance().getBooleanGlobal("core", "LIMIT_FETCH", true)
            );

            if (isLimitedFetch)
            {
                String query =
                    "SELECT COALESCE(SUM(PART.rows), 0) AS rowsCount\n" +
                    "FROM sys.tables TBL\n" +
                    "INNER JOIN sys.partitions PART ON TBL.object_id = PART.object_id\n" +
                    "INNER JOIN sys.indexes IDX ON PART.object_id = IDX.object_id AND PART.index_id = IDX.index_id\n" +
                    "INNER JOIN sys.schemas S ON S.schema_id = TBL.schema_id\n" +
                    "WHERE TBL.name = '"+nameTable+"' AND S.name = '"+schema+"' AND IDX.index_id < 2\n" +
                    "GROUP BY TBL.object_id, TBL.name";

                ResultSet rs = stmt.executeQuery(query);
                rs.next();
                if (rs.getInt("rowsCount") > maxRowsCount) {
                    data.setErrorFlag(DBTableData.ERROR_LIMIT_ROWS);
                    return data;
                }
            }

            ResultSet rs = stmt.executeQuery("SELECT * FROM [" + schema + "].[" + nameTable + "]");
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

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		DBTableData data = new DBTableData();

		try {
			Statement stmt = getConnection().createStatement();
			int portionSize = DBGitConfig.getInstance().getInteger( "core", "PORTION_SIZE",
					DBGitConfig.getInstance().getIntegerGlobal("core", "PORTION_SIZE", 1000)
			);

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

			ResultSet rs = stmt.executeQuery( "SELECT * " +
				"FROM "+ schema + "." + nameTable + " " +
				"ORDER BY (SELECT NULL) " +
				"OFFSET " + portionSize*portionIndex + " ROWS " +
				"FETCH NEXT " + portionSize + " ROWS ONLY "
			);

			data.setResultSet(rs);
			return data;
		} catch (Exception e) {

			ConsoleWriter.println("Connection lost!");
			ConsoleWriter.println("Error while getting portion of data, try " + tryNumber);
			logger.error(lang.getValue("errors", "adapter", "tableData").toString(), e);

			//try fetch again
			try {

				int retryDelay = DBGitConfig.getInstance().getInteger( "core", "TRY_DELAY",
						DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_DELAY", 1000)
				);

				int maxTryCount = DBGitConfig.getInstance().getInteger( "core", "TRY_COUNT",
						DBGitConfig.getInstance().getIntegerGlobal("core", "TRY_COUNT", 1000)
				);

				if (tryNumber <= maxTryCount) {

					try { TimeUnit.SECONDS.sleep(retryDelay); }
					catch (InterruptedException e1) { throw new ExceptionDBGitRunTime(e1.getMessage()); }

					return getTableDataPortion(schema, nameTable, portionIndex, tryNumber++);
				}
			} catch (Exception e1) { e1.printStackTrace(); }

			//rollback, needed only when auto-commit mode has been disabled

			try {
				getConnection().rollback();
			} catch (Exception e2) {
				logger.error(lang.getValue("errors", "adapter", "rollback").toString(), e2);
			}
			throw new ExceptionDBGitRunTime(e.getMessage());
		}
	}

    @Override
    public Map<String, DBUser> getUsers() {
        Map<String, DBUser> listUser = new HashMap<String, DBUser>();
        try {

			String query =
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

			Statement stmt = getConnection().createStatement();
			ResultSet rs = stmt.executeQuery(query);;

            while(rs.next()){
                String name = rs.getString(1);
                DBUser user = new DBUser(name);
                rowToProperties(rs, user.getOptions());
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
			List<String> expressions = Arrays.asList(
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

			String query =
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

			Connection connect = getConnection();
			Statement stmt = connect.createStatement();

			for(String expr : expressions){
				stmt.execute(expr);
			}

			ResultSet rs = stmt.executeQuery(query);
			while(rs.next()){
				String name = rs.getString("rolename");
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
	public String getDbType() {
		return IFactoryDBConvertAdapter.MSSQL;
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
