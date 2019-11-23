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

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

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

				// TODO MSSQL backup MetaSql script
				//dropIfExists(isSaveToSchema() ? PREFIX + schema : schema,
				//		isSaveToSchema() ? objectName : PREFIX + objectName, stLog);

				ddl = ddl.replace(schema + "." + objectName, getFullDbName(schema, objectName));

				//ddl += "alter table "+ tableName + " owner to "+ metaTable.getTable().getOptions().get("tableowner").getData()+";\n";

				stLog.execute(ddl);

				File file = new File(DBGitPath.getFullPath() + metaSql.getFileName());
				if (file.exists())
					obj = metaSql.loadFromFile();

				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			} else if (obj instanceof MetaTable) {

				MetaTable metaTable = (MetaTable) obj;
				metaTable.loadFromDB();
				String objectName = metaTable.getTable().getName();
				String schema = metaTable.getTable().getSchema();
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String tableName = getFullDbName(schema, objectName);

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
					// TODO MSSQL backup MetaTable script
					ddl = "create table " + tableName + " as (select * from " + schema + "." + objectName + ")" +
							(metaTable.getTable().getOptions().getChildren().containsKey("tablespace") ?
									" tablespace " + metaTable.getTable().getOptions().get("tablespace").getData() : "") +";\n";
					ddl += "alter table "+ tableName + " owner to "+ metaTable.getTable().getOptions().get("owner").getData()+";\n";
				} else {

					ddl ="create table " + tableName + "() " +
							(metaTable.getTable().getOptions().getChildren().containsKey("tablespace") ?
									" tablespace " + metaTable.getTable().getOptions().get("tablespace").getData() : "") +";\n";
					ddl += "alter table "+ tableName + " owner to "+ metaTable.getTable().getOptions().get("tableowner").getData()+";\n";


					for (DBTableField field : metaTable.getFields().values()) {
						ddl += "alter table " + tableName + " add " + field.getName() + " " + field.getTypeSQL() + ";\n";
					}

				}

				for (DBConstraint constraint : metaTable.getConstraints().values()) {
					ddl += "alter table "+ tableName +" add constraint " + PREFIX + constraint.getName() + " " + constraint.getSql() + ";\n";
				}

				for (DBIndex index : metaTable.getIndexes().values()) {
					String indexDdl = index.getSql() + (metaTable.getTable().getOptions().getChildren().containsKey("tablespace") ?
							" tablespace "+index.getOptions().get("tablespace").getData() : "") + ";\n";
					indexDdl = indexDdl.replace(index.getName(), PREFIX + index.getName());
					if (indexDdl.length() > 3)
						ddl += indexDdl;
				}
				stLog.execute(ddl);

				File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
				if (file.exists())
					obj = metaTable.loadFromFile();
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			} else if (obj instanceof MetaSequence) {
				MetaSequence metaSequence = (MetaSequence) obj;
				metaSequence.loadFromDB();

				String objectName = metaSequence.getSequence().getName();
				String schema = metaSequence.getSequence().getSchema();

				if (isSaveToSchema()) {
					createSchema(stLog, schema);
				}

				String sequenceName = getFullDbName(schema, objectName);

				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);

				// TODO MSSQL Backup MetaSequence script
				String ddl = "create sequence " + sequenceName + "\n"
						+ (metaSequence.getSequence().getOptions().get("cycle_option").toString().equals("YES") ? "CYCLE\n" : "")
						+ " INCREMENT " + metaSequence.getSequence().getOptions().get("increment").toString() + "\n"
						+ " START " + metaSequence.getSequence().getOptions().get("start_value").toString() + "\n"
						+ " MINVALUE " + metaSequence.getSequence().getOptions().get("minimum_value").toString() + "\n"
						+ " MAXVALUE " + metaSequence.getSequence().getOptions().get("maximum_value").toString() + ";\n";

				ddl += "alter sequence "+ sequenceName + " owner to "+ metaSequence.getSequence().getOptions().get("owner").getData()+";\n";

				dropIfExists(isSaveToSchema() ? PREFIX + schema : schema,
						isSaveToSchema() ? objectName : PREFIX + objectName, stLog);

				stLog.execute(ddl);

				File file = new File(DBGitPath.getFullPath() + metaSequence.getFileName());
				if (file.exists())
					obj = metaSequence.loadFromFile();
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}

		} catch (SQLException e1) {
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "objectRestoreError").
					withParams(obj.getName() + ": " + e1.getLocalizedMessage()));
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			connection.rollback();
			throw new ExceptionDBGitRestore(lang.getValue("errors", "backup", "backupError").withParams(obj.getName()), e);
		} finally {
			connection.commit();
			stLog.close();
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
		Statement st = 	adapter.getConnection().createStatement();

		// TODO MSSQL dropIfExists script
		ResultSet rs = st.executeQuery("select * from (\r\n" +
				"	SELECT 'TABLE' tp, table_name obj_name, table_schema sch FROM information_schema.tables \r\n" +
				"	union select 'VIEW' tp, table_name obj_name, table_schema sch from information_schema.views\r\n" +
				"	union select 'SEQUENCE' tp, sequence_name obj_name, sequence_schema sch from information_schema.sequences\r\n" +
				"	union select 'TRIGGER' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers\r\n" +
				"	union select 'FUNCTION' tp, routine_name obj_name, routine_schema sch from information_schema.routines\r\n" +
				") all_objects\r\n" +
				"where sch = '" + owner.toLowerCase() + "' and obj_name = '" + objectName.toLowerCase() + "'");

		while (rs.next()) {
			stLog.execute("drop " + rs.getString("tp") + " " + owner + "." + objectName);
		}

		rs.close();
		st.close();
	}

	@Override
	public boolean isExists(String owner, String objectName) throws Exception {
		Statement st = 	adapter.getConnection().createStatement();
		// TODO MSSQL isExists script
		ResultSet rs = st.executeQuery("select count(*) cnt from (\r\n" +
				"	SELECT 'TABLE' tp, table_name obj_name, table_schema sch FROM information_schema.tables \r\n" +
				"	union select 'VIEW' tp, table_name obj_name, table_schema sch from information_schema.views\r\n" +
				"	union select 'SEQUENCE' tp, sequence_name obj_name, sequence_schema sch from information_schema.sequences\r\n" +
				"	union select 'TRIGGER' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers\r\n" +
				"	union select 'FUNCTION' tp, routine_name obj_name, routine_schema sch from information_schema.routines\r\n" +
				") all_objects\r\n" +
				"where lower(sch) = '" + owner.toLowerCase() + "' and lower(obj_name) = '" + objectName.toLowerCase() + "'");

		rs.next();
		return rs.getInt("cnt") > 0;
	}

	@Override
	public boolean createSchema(StatementLogging stLog, String schema) {
		try {
			Statement st = 	adapter.getConnection().createStatement();

			// TODO MSSQL createSchema script
			ResultSet rs = st.executeQuery("select count(*) cnt from information_schema.schemata where upper(schema_name) = '" +
					PREFIX + schema.toUpperCase() + "'");

			rs.next();
			if (rs.getInt("cnt") == 0) {
				ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "creatingSchema").withParams(PREFIX + schema));
				stLog.execute("create schema " + PREFIX + schema);
			}

			rs.close();
			st.close();

			return true;
		} catch (SQLException e) {
			ConsoleWriter.println(lang.getValue("errors", "backup", "cannotCreateSchema").withParams(e.getLocalizedMessage()));
			return false;
		}
	}

}
