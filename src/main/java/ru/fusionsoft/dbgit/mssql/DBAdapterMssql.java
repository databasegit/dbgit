package ru.fusionsoft.dbgit.mssql;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;
import org.slf4j.Logger;
import org.sqlite.core.DB;
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

import java.sql.*;
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
                "select user_name(objectproperty(sys.sequences.object_id,'OwnerId')) as owner, sys.sequences.* " +
                "from sys.objects, sys.SEQUENCES\n" +
                "where sys.objects.object_id = sys.sequences.object_id\n" +
                "AND user_name(objectproperty(sys.sequences.object_id,'OwnerId')) = '" + schema + "'";

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
                    "select user_name(objectproperty(sys.sequences.object_id,'OwnerId')) as owner, sys.sequences.* " +
                            "from sys.objects, sys.SEQUENCES\n" +
                            "where sys.objects.object_id = sys.sequences.object_id\n" +
                            "AND user_name(objectproperty(sys.sequences.object_id,'OwnerId')) = '" + schema + "'\n" +
                            "AND sys.sequences.name = '" + name + "'\n";
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
        try {
            String query =
                    "SELECT TABLE_NAME as 'name', TABLE_CATALOG as 'database', TABLE_SCHEMA as 'schema'\n" +
                            "FROM INFORMATION_SCHEMA.TABLES \n" +
                            "WHERE INFORMATION_SCHEMA.TABLES.TABLE_SCHEMA = '" + schema + "'\n" +
                            "AND INFORMATION_SCHEMA.TABLES.TABLE_TYPE = 'BASE TABLE'\n" +
                            "AND INFORMATION_SCHEMA.TABLES.TABLE_NAME = '" + name + "'\n";
            Connection connect = getConnection();
            Statement stmt = connect.createStatement();
            ResultSet rs = stmt.executeQuery(query);

            while(rs.next()){
                String nameTable = rs.getString("name");
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
		Map<String, DBTableField> listField = new HashMap<>();
		try {
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
				"	CASE WHEN ( \n" +
				"		SELECT OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + QUOTENAME(CONSTRAINT_NAME)),'IsPrimaryKey')\n" +
				"		FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE\n" +
				"		WHERE c.COLUMN_NAME = COLUMN_NAME AND c.TABLE_NAME = TABLE_NAME\n" +
				"	) = 1\n" +
				"	THEN 1 ELSE 0 END isPk,\n" +
				"	c.IS_NULLABLE as isNullable,\n" +
				"	c.NUMERIC_SCALE as scale,\n" +
				"	c.CHARACTER_MAXIMUM_LENGTH as length,\n" +
				"	CASE WHEN lower(c.DATA_TYPE) in ('char', 'nchar') then '1' else '0' end isFixed," +
				"	c.NUMERIC_PRECISION as precision\n" +
				"FROM INFORMATION_SCHEMA.COLUMNS as c\n" +
				"WHERE TABLE_SCHEMA = '" +  schema +  "' AND TABLE_NAME = '" + nameTable + "'";
			Connection connect = getConnection();
			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()){
				DBTableField field = DBTableFieldFromRs(rs);
				listField.put(field.getName(), field);
			}
			stmt.close();
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

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		Map<String, DBIndex> indexes = new HashMap<>();
		try {
			String query =
					"    SELECT DB_NAME() AS databaseName,\n" +
					"    sc.name as schemaName, \n" +
					"	 t.name AS tableName,\n" +
					"	 col.name as columnName,\n" +
					"    si.name AS indexName,\n" +
					"	 si.index_id as indexId,\n" +
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
					"AND si.is_primary_key = 0 /* no PKs */\n" +
					"AND si.is_hypothetical = 0 /* bugged feature, always better to delete, no need to store and reconstuct them */\n" +
					"AND upper(t.name) = upper('" + nameTable + "') AND upper(sc.name) = upper('" + schema + "')" +
					"OPTION (RECOMPILE);";

			Statement stmt = connect.createStatement();
			ResultSet rs = stmt.executeQuery(query);

			while(rs.next()){
				DBIndex index = new DBIndex();
				index.setName(rs.getString("indexName"));
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
