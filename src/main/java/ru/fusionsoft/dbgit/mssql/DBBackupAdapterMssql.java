package ru.fusionsoft.dbgit.mssql;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.meta.MetaSql;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.Objects;

@SuppressWarnings("Duplicates")
public class DBBackupAdapterMssql extends DBBackupAdapter {

	@Override
	public IMetaObject backupDBObject(IMetaObject obj) throws Exception {

		Connection connection = adapter.getConnection();
		StatementLogging stLog = new StatementLogging(connection, adapter.getStreamOutputSqlCommand(), adapter.isExecSql());

		try {
			if (obj instanceof MetaSql) {
				MetaSql metaSql = (MetaSql) obj;
				String objectName = metaSql.getSqlObject().getName();
				metaSql.loadFromDB();

				String ddl = metaSql.getSqlObject().getSql();
				String schema = metaSql.getSqlObject().getSchema();

				if (isSaveToSchema()) {
					createSchema(stLog, schema);
				}

				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);

				dropIfExists(
					isSaveToSchema() ? PREFIX + schema : schema,
					isSaveToSchema() ? objectName : PREFIX + objectName, stLog
				);

				ddl = ddl.replace(schema + "." + objectName, getFullDbName(schema, objectName));

				stLog.execute(ddl);

				File file = new File(DBGitPath.getFullPath() + metaSql.getFileName());
				if (file.exists())
					obj = metaSql.loadFromFile();

				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			else if (obj instanceof MetaTable) {

				MetaTable metaTable = (MetaTable) obj;
				metaTable.loadFromDB();
				String objectName = metaTable.getTable().getName();
				String schema = metaTable.getTable().getSchema();
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String tableSam = getFullDbName(schema, objectName);
                String origTableName = schema + "." + objectName;


                if(!isExists(schema, objectName)) {
					File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
					if (file.exists())
						obj = metaTable.loadFromFile();
					return obj;
				}

				if (isSaveToSchema()) {
					createSchema(stLog, schema);
				}

				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);

				dropIfExists(isSaveToSchema() ? PREFIX + schema : schema,
						isSaveToSchema() ? objectName : PREFIX + objectName, stLog);

				String ddl = "";
				if (isToSaveData()) {
				    // Fields + data
					ddl = "create table " + tableSam + " as (select * from " + schema + "." + objectName + ")" +";\n";

					// Schema
					ddl += "alter schema "+ adapter.getDefaultScheme() + " transfer "+ tableSam + ";\n";
                } else {
				    // Fields
					ddl ="create table " + tableSam + "(";
					for (DBTableField field : metaTable.getFields().values()) {
						ddl += MessageFormat.format("\n[{0}] {1},", field.getName(), field.getTypeSQL());
					}
					ddl = ddl.substring(0, ddl.length()-1);
					ddl += "\n);\n";

					// Schema
					ddl += "alter schema "+ adapter.getDefaultScheme() + " transfer "+ tableSam + ";\n";
				}

				for (DBConstraint constraint : metaTable.getConstraints().values()) {
				    String constraintSql = constraint.getSql().replace(
                        "ALTER TABLE " + origTableName + " ADD CONSTRAINT " + constraint.getName(),
                    "alter table "+ tableSam +" add constraint " + PREFIX + constraint.getName() + " "
                    );
					ddl += constraintSql + "\n";
				}

				for (DBIndex index : metaTable.getIndexes().values()) {
					String indexDdl = index.getSql() + "\n";
					indexDdl = indexDdl
                        .replace(index.getName(), PREFIX + index.getName())
                        .replace("["+objectName+"]", "["+PREFIX+objectName+"]");
					if (indexDdl.length() > 3)
						ddl += indexDdl;
				}
				stLog.execute(ddl);

				File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
				if (file.exists())
					obj = metaTable.loadFromFile();
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}
			else if (obj instanceof MetaSequence) {
				MetaSequence metaSequence = (MetaSequence) obj;
				metaSequence.loadFromDB();

				String objectName = metaSequence.getSequence().getName();
				String schema = metaSequence.getSequence().getSchema();

				if (isSaveToSchema()) {
					createSchema(stLog, schema);
				}

				String sequenceName = getFullDbName(schema, objectName);

				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);

                StringProperties props = metaSequence.getSequence().getOptions();
				String seqName = props.get("name").getData();
				String seqTypeName = props.get("typename").getData();
                String seqStart = props.get("start_value").getData();
                String seqIncr = props.get("increment").getData();
                StringProperties seqMin = props.get("minimum_value");
				StringProperties seqMax = props.get("maximum_value");
                boolean seqCycle = props.get("is_cycling").getData().equals("1");
                boolean seqHasCache = props.get("is_cached").getData().equals("1");
                //there may be default cache size
				StringProperties seqCacheSize = props.get("cache_size");
                String seqOwner = props.get("owner").getData();

                Objects.requireNonNull(seqTypeName);

				String ddl = "CREATE SEQUENCE " + sequenceName + " AS " + seqTypeName
					+ " START WITH " + seqStart
					+ " INCREMENT BY " + seqIncr
					+ (Objects.nonNull(seqMin) ? " MINVALUE " + seqMin.getData() : " NO MINVALUE ")
					+ (Objects.nonNull(seqMax) ? " MAXVALUE " + seqMax.getData() : " NO MAXVALUE ")
					+ ((seqHasCache)
						? " CACHE " + (seqCacheSize != null ? seqCacheSize : " ")
						: " NO CACHE")
					+ ((seqCycle) ? " CYCLE " : " NO CYCLE " + "\n");

                ddl += MessageFormat.format(
				"ALTER SCHEMA {0} TRANSFER {1}.{2}",
					adapter.getConnection().getSchema(), seqOwner, seqName
				);

				dropIfExists(isSaveToSchema() ? PREFIX + schema : schema,
						isSaveToSchema() ? objectName : PREFIX + objectName, stLog);

				stLog.execute(ddl);

				File file = new File(DBGitPath.getFullPath() + metaSequence.getFileName());
				if (file.exists())
					obj = metaSequence.loadFromFile();
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}

		} catch (SQLException e1) {
			throw new ExceptionDBGitRestore(
				lang.getValue("errors", "restore", "objectRestoreError")
					.withParams(obj.getName() + ": " + e1.getLocalizedMessage())
			);
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			connection.rollback();
			throw new ExceptionDBGitRestore(lang.getValue("errors", "backup", "backupError").withParams(obj.getName()), e);
		} finally {
            stLog.close();
            connection.commit();
		}
		return obj;
	}

	@Override
	public void restoreDBObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}

	private String getFullDbName(String schema, String objectName) {
		if (isSaveToSchema())
			return PREFIX + schema + "." + objectName;
		else
			return schema + "." + PREFIX + objectName;
	}

	private void dropIfExists(String owner, String objectName, StatementLogging stLog) throws Exception {
		String query =
			"SELECT CASE \n" +
			"WHEN type IN ('PC', 'P') THEN 'PROCEDURE'\n" +
			"WHEN type IN ('FN', 'FS', 'FT', 'IF', 'TF') THEN 'FUNCTION' \n" +
			"WHEN type = 'AF' THEN 'AGGREGATE' \n" +
			"WHEN type = 'U' THEN 'TABLE' \n" +
			"WHEN type = 'V' THEN 'VIEW' \n" +
			"WHEN type IN ('SQ', 'SO') THEN 'SEQUENCE' \n" +
			"END type\n" +
			"FROM sys.objects so\n" +
			"WHERE lower(name) = lower('"+objectName+"') \n" +
			"AND lower(SCHEMA_NAME(schema_id)) = lower('"+owner+"')";

		try(Statement st = 	adapter.getConnection().createStatement(); ResultSet rs = st.executeQuery(query)) {
			while (rs.next()) {
				String type = rs.getString("type");
				stLog.execute(MessageFormat.format("DROP {0} {1}.{2}", type, owner, objectName));
			}
		}

	}

	@Override
	public boolean isExists(String owner, String objectName) throws Exception {
		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery(
			"SELECT CASE WHEN OBJECT_ID('"+owner+"."+objectName+"') IS NOT NULL THEN 1 ELSE 0 END"
		);

		rs.next();
		return rs.getInt(1) == 1;
	}

	@Override
	public boolean createSchema(StatementLogging stLog, String schema) {
		try {
			if (!adapter.getSchemes().containsKey(schema)) {
				ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "creatingSchema").withParams(PREFIX + schema));
				stLog.execute(MessageFormat.format("CREATE SCHEMA {0}{1}", PREFIX, schema));
			}
			return true;
		} catch (SQLException e) {
			ConsoleWriter.println(lang.getValue("errors", "backup", "cannotCreateSchema").withParams(e.getLocalizedMessage()));
			return false;
		}
	}

}
