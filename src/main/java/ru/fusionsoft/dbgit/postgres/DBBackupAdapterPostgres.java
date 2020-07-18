package ru.fusionsoft.dbgit.postgres;

import java.io.File;
import java.lang.reflect.Type;
import java.sql.*;
import java.text.MessageFormat;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.core.*;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBBackupAdapterPostgres extends DBBackupAdapter {

	@Override
	public IMetaObject backupDBObject(IMetaObject obj) throws SQLException, ExceptionDBGit {
		
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
				String tableName = metaTable.getTable().getName();
				String schema = metaTable.getTable().getSchema();
				schema = (SchemaSynonym.getInstance().getSchema(schema) == null) ? schema : SchemaSynonym.getInstance().getSchema(schema);
				String backupTableSam = getFullDbName(schema, tableName);
				String backupTableSamRe = Matcher.quoteReplacement(backupTableSam);
				String tableSamRe = schema + "\\.\\\"?" + Pattern.quote(tableName) + "\\\"?";

				if(!isExists(schema, tableName)) {
					File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
					if (file.exists())
						obj = metaTable.loadFromFile();
					return obj;
				}
				
				if (isSaveToSchema()) {
					createSchema(stLog, schema);
				}

				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(tableName, getFullDbName(schema, tableName)), 1);
				
				dropIfExists(
					isSaveToSchema() ? PREFIX + schema : schema,
					isSaveToSchema() ? tableName : PREFIX + tableName, stLog
				);
				
				StringBuilder tableDdlSb = new StringBuilder(MessageFormat.format(
					"create table {0} as (select * from {1}.{2} where 1={3}) {4};\n alter table {0} owner to {5};\n"
					, backupTableSam
					, DBAdapterPostgres.escapeNameIfNeeded(schema)
					, DBAdapterPostgres.escapeNameIfNeeded(tableName)
					, isToSaveData() ? "1" : "0"
					, metaTable.getTable().getOptions().getChildren().containsKey("tablespace")
							? " tablespace " + metaTable.getTable().getOptions().get("tablespace").getData()
							: ""
					, metaTable.getTable().getOptions().get("owner").getData()
				));


				Map<String, String> fkRefReplaces = new HashMap<>();
				for (String fk : metaTable.getTable().getDependencies()){
					String fkname = fk.substring(fk.indexOf('/')+1,fk.lastIndexOf('.')) ;
					String fkschema = fk.substring(0,fk.indexOf('/')) ;

					String nameDb = "("+fkschema+ "\\.)?" + "\\\"?" + Pattern.quote(DBAdapterPostgres.escapeNameIfNeeded(fkname)) + "\\\"?(?=\\()";
					String nameReplacement = isSaveToSchema()
						? Matcher.quoteReplacement(DBAdapterPostgres.escapeNameIfNeeded(PREFIX+fkschema) + "." + fkname)
						: Matcher.quoteReplacement(fkschema + "." + DBAdapterPostgres.escapeNameIfNeeded(PREFIX+fkname));
					fkRefReplaces.put(nameDb, nameReplacement);
				}


				for (DBIndex index : metaTable.getIndexes().values()) {
					String indexName = index.getName();
					String indexNameRe = "\\\"?" + Pattern.quote(indexName) + "\\\"?";
					String backupIndexNameRe = Matcher.quoteReplacement(DBAdapterPostgres.escapeNameIfNeeded(
						PREFIX + indexName
						+ ((metaTable.getConstraints().containsKey(index.getName())) ? "_idx" : "")
					));

					String indexSql = index.getSql().replaceAll(indexNameRe, backupIndexNameRe).replaceAll(tableSamRe, backupTableSamRe);

					String indexDdl = MessageFormat.format(
						"{0} {1};\n"
						, indexSql
						, metaTable.getTable().getOptions().getChildren().containsKey("tablespace")
							?  " tablespace "+index.getOptions().get("tablespace").getData()
							: ""
					);
					if (indexDdl.length() > 3) { tableDdlSb.append(indexDdl); }
				}

				for (DBConstraint constraint : metaTable.getConstraints().values().stream().sorted(Comparator.comparing(x->!x.getName().toLowerCase().contains("pk"))).collect(Collectors.toList())) {
					String name = DBAdapterPostgres.escapeNameIfNeeded(PREFIX + constraint.getName());
					String constrName = constraint.getName();
					String constrNameRe = "\\\"?" + Pattern.quote(constrName) + "\\\"?";
					String backupConstrNameRe = Matcher.quoteReplacement(DBAdapterPostgres.escapeNameIfNeeded(PREFIX + constrName));
					String constrDef = constraint.getSql().replaceAll(constrNameRe, backupConstrNameRe);
					for(String reference : fkRefReplaces.keySet()){ constrDef = constrDef.replaceAll(reference, fkRefReplaces.get(reference)); }


					String constrDdl = MessageFormat.format(
						"alter table {0} add {1};\n"
						,backupTableSam
						,metaTable.getIndexes().containsKey(constraint.getName())
						&& constraint.getOptions().get("constraint_type").getData().equals("p")
							? "primary key using index " + name
							: "constraint " + name + " " + constrDef
					);
					if (constrDdl.length() > 3) { tableDdlSb.append(constrDdl); }
				}


				stLog.execute(tableDdlSb.toString());
				
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
				
				String ddl = "create sequence " + sequenceName + "\n"
						+ (metaSequence.getSequence().getOptions().get("cycle_option").toString().equals("YES") ? "CYCLE\n" : "")
						+ " INCREMENT " + metaSequence.getSequence().getOptions().get("increment").toString() + "\n"
						+ " START " + metaSequence.getSequence().getOptions().get("start_value").toString() + "\n"
						+ " MINVALUE " + metaSequence.getSequence().getOptions().get("minimum_value").toString() + "\n"
						+ " MAXVALUE " + metaSequence.getSequence().getOptions().get("maximum_value").toString() + ";\n";
				
				ddl += "alter sequence "+ sequenceName + " owner to "+ metaSequence.getSequence().getOptions().get("owner").getData()+";\n";
				
				dropIfExists(
					isSaveToSchema() ? PREFIX + schema : schema,
					isSaveToSchema() ? objectName : PREFIX + objectName, stLog
				);
				
				stLog.execute(ddl);
				
				File file = new File(DBGitPath.getFullPath() + metaSequence.getFileName());				
				if (file.exists())
					obj = metaSequence.loadFromFile();
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}			
			
		} catch (SQLException e1) {
			throw new ExceptionDBGit(lang.getValue("errors", "backup", "backupError").
					withParams(obj.getName() + ": " + e1.getLocalizedMessage()));
		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			connection.rollback();
			throw new ExceptionDBGitRestore(lang.getValue("errors", "backup", "backupError").withParams(obj.getName()), e);
		} finally {
			stLog.close();
		}	
		return obj;
	}

	@Override
	public void restoreDBObject(IMetaObject obj) throws Exception {
		// TODO Auto-generated method stub

	}
	
	private String getFullDbName(String schema, String objectName) {
		if (isSaveToSchema()){
			return DBAdapterPostgres.escapeNameIfNeeded(PREFIX + schema) + "." + DBAdapterPostgres.escapeNameIfNeeded(objectName);
		}
		else {
			return DBAdapterPostgres.escapeNameIfNeeded(schema) + "." + DBAdapterPostgres.escapeNameIfNeeded(PREFIX + objectName);
		}

	}
	
	public void dropIfExists(String owner, String objectName, StatementLogging stLog) throws SQLException {
		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery("select * from (\r\n" + 
				"	SELECT 'TABLE' tp, table_name obj_name, table_schema sch FROM information_schema.tables \r\n" + 
				"	union select 'VIEW' tp, table_name obj_name, table_schema sch from information_schema.views\r\n" + 
				"	union select 'SEQUENCE' tp, sequence_name obj_name, sequence_schema sch from information_schema.sequences\r\n" + 
				"	union select 'TRIGGER' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers\r\n" + 
				"	union select 'FUNCTION' tp, routine_name obj_name, routine_schema sch from information_schema.routines\r\n" + 
				") all_objects\r\n" + 
				"where sch = '" + owner.toLowerCase() + "' and obj_name = '"+objectName+"'");
		
		while (rs.next()) {
			stLog.execute("drop " + rs.getString("tp") + " " + DBAdapterPostgres.escapeNameIfNeeded(owner) + "." + DBAdapterPostgres.escapeNameIfNeeded(objectName));
		}
		
		rs.close();
		st.close();
	}
	public void dropIfExists(IMetaObject imo, StatementLogging stLog) throws Exception {
		NameMeta nm = new NameMeta(imo);
		DBGitMetaType type = (DBGitMetaType) nm.getType();

		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery(MessageFormat.format("select * from (\r\n" +
				"	SELECT ''TABLE'' tp, table_name obj_name, table_schema sch FROM information_schema.tables WHERE 1={0}\r\n" +
				"	union select ''VIEW'' tp, table_name obj_name, table_schema sch from information_schema.views WHERE 1={1}\r\n" +
				"	union select ''SEQUENCE'' tp, sequence_name obj_name, sequence_schema sch from information_schema.sequences WHERE 1={2}\r\n" +
				"	union select ''TRIGGER'' tp, trigger_name obj_name, trigger_schema sch from information_schema.triggers WHERE 1={3}\r\n" +
				"	union select ''FUNCTION'' tp, routine_name obj_name, routine_schema sch from information_schema.routines WHERE 1={4}\r\n" +
				") all_objects\r\n" +
				"where sch = ''{5}'' and obj_name = ''{6}''",
				type.equals(DBGitMetaType.DBGitTable) ? "1" : "0",
				type.equals(DBGitMetaType.DbGitView) ? "1" : "0",
				type.equals(DBGitMetaType.DBGitSequence) ? "1" : "0",
				type.equals(DBGitMetaType.DbGitTrigger) ? "1" : "0",
				type.equals(DBGitMetaType.DbGitFunction) ? "1" : "0",
				nm.getSchema(), nm.getName()
		));

		while (rs.next()) {
			stLog.execute(MessageFormat.format("DROP {0} {1}.{2}",
				rs.getString("tp"),
				DBAdapterPostgres.escapeNameIfNeeded(nm.getSchema()),
				DBAdapterPostgres.escapeNameIfNeeded(nm.getName())
			));
		}

		rs.close();
		st.close();
	}

	@Override
	public boolean isExists(String owner, String objectName) throws SQLException {
		Statement st = 	adapter.getConnection().createStatement();
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
		if(createdSchemas.contains(schema)) return true;
		try {
			Statement st = 	adapter.getConnection().createStatement();
			String query = MessageFormat.format(
				"select count(*) cnt from information_schema.schemata where schema_name = ''{0}{1}''",
				PREFIX, schema
			);
			ResultSet rs = st.executeQuery(query);
			
			rs.next();
			if (rs.getInt("cnt") == 0) {
				ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "creatingSchema").withParams(PREFIX + schema));
				stLog.execute("create schema " + DBAdapterPostgres.escapeNameIfNeeded(PREFIX + schema));
			}
			
			rs.close();
			st.close();
			createdSchemas.add(schema);

			return true;
		} catch (SQLException e) {
			ConsoleWriter.println(lang.getValue("errors", "backup", "cannotCreateSchema").withParams(e.getLocalizedMessage()));
			return false;
		}
	}

	private Set<String> createdSchemas = new HashSet();

}
