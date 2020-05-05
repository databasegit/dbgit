package ru.fusionsoft.dbgit.oracle;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.meta.*;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBBackupAdapterOracle extends DBBackupAdapter {
	
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
				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);
				
				ddl = replaceNames(ddl, schema, objectName, stLog);
				
				int index = ddl.toUpperCase().indexOf("CREATE OR REPLACE PACKAGE BODY");
				if (index == -1) {
					index = ddl.toUpperCase().indexOf("ALTER TRIGGER \"");
				}
				
				if (index > 0) {
					String ddlHead = ddl.substring(0, index);								
					String ddlBody = ddl.substring(index);
					
					stLog.execute(ddlHead, "/");
					stLog.execute(ddlBody, "/");
				} else {
					stLog.execute(ddl, "/");
				}
				
				File file = new File(DBGitPath.getFullPath() + metaSql.getFileName());
				
				if (file.exists())
					obj = metaSql.loadFromFile();
		
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			} else if (obj instanceof MetaTable) {
				MetaTable metaTable = (MetaTable) obj;
				metaTable.loadFromDB();
				String objectName = metaTable.getTable().getName();
				String schema = metaTable.getTable().getSchema();				
				
				if(!isExists(schema, objectName))
					return obj;
				
				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);
				
				dropIfExists(isSaveToSchema() ? PREFIX + schema : schema, 
						isSaveToSchema() ? objectName : PREFIX + objectName, stLog);
				
				if (isToSaveData()) {					
					String ddl = "create table " +  (isSaveToSchema() ? "" : schema + ".") + 
							getFullDbName(schema, objectName) + " as (select * from " + schema + "." + objectName + ")";
					stLog.execute(ddl);
					
					for (DBConstraint constraint : metaTable.getConstraints().values()) {
						String constraintDdl = constraint.getSql();
						
						if (isSaveToSchema())
							constraintDdl = constraintDdl.replace("\"" + schema + "\"", "\"" + PREFIX + schema + "\"");
						else
							constraintDdl = constraintDdl.replace("\"" + constraint.getName() + "\"", "\"" +  getFullDbName(schema, constraint.getName()) + "\"")
								.replace(".\"" + objectName + "\"", ".\"" +  getFullDbName(schema, objectName) + "\"");
						
						stLog.execute(constraintDdl);
					}
				} else {					
					String ddl = metaTable.getTable().getOptions().get("ddl").toString();
					
					if (isSaveToSchema()) 
						ddl = ddl.replace("\"" + schema + "\"", "\"" + PREFIX + schema + "\"");
					else
						ddl = ddl.replace("\"" + objectName + "\"", "\"" +  getFullDbName(schema, objectName) + "\"");
					
					stLog.execute(ddl);
				}
				
				File file = new File(DBGitPath.getFullPath() + metaTable.getFileName());
				if (file.exists())
					obj = metaTable.loadFromFile();
				
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			} else if (obj instanceof MetaSequence) {
				MetaSequence metaSequence = (MetaSequence) obj;
				metaSequence.loadFromDB();

				String objectName = metaSequence.getSequence().getName();
				String schema = metaSequence.getSequence().getSchema();

				String ddl = metaSequence.getSequence().getOptions().get("ddl").toString();

				ConsoleWriter.detailsPrint(lang.getValue("general", "backup", "tryingToCopy").withParams(objectName, getFullDbName(schema, objectName)), 1);

				ddl = replaceNames(ddl, schema, objectName, stLog);

				stLog.execute(ddl);
				File file = new File(DBGitPath.getFullPath() + metaSequence.getFileName());
				
				if (file.exists())
					obj = metaSequence.loadFromFile();
				
				ConsoleWriter.detailsPrintlnGreen(lang.getValue("general", "ok"));
			}

		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed(lang.getValue("errors", "meta", "fail"));
			throw new ExceptionDBGitRestore(lang.getValue("errors", "backup", "backupError").withParams(obj.getName()), e);
		} finally {
			stLog.close();
		}	
		return obj;
	}

	@Override
	public void restoreDBObject(IMetaObject obj) throws Exception {
		//ConsoleWriter.detailsPrintLn("Restoring from backup!");
		
		/**
		 *TODO:
		 * 1) get object ddl
		 * 2) removing "BACKUP$" from names
		 * 3) execute ddl
		 */

	}
	
	public void dropIfExists(String owner, String objectName, StatementLogging stLog) throws SQLException {
		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery("select * from all_objects where owner = '" + owner + "' and object_name = '" + objectName + "' and OBJECT_TYPE not like 'PACKAGE BODY'");
		
		while (rs.next()) {
			//ConsoleWriter.detailsPrintLn("Dropping " + owner + "." + objectName);
			stLog.execute("drop " + rs.getString("OBJECT_TYPE") + " " + owner + "." + objectName);
		}
		
		rs.close();
		st.close();
	}

	@Override
	public void dropIfExists(IMetaObject imo, StatementLogging stLog) throws SQLException, Exception {
		NameMeta nm = new NameMeta(imo);
		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery("select * from all_objects where owner = '" + nm.getSchema() + "' and object_name = '" + nm.getName() + "' and OBJECT_TYPE not like 'PACKAGE BODY'");

		while (rs.next()) {
			//ConsoleWriter.detailsPrintLn("Dropping " + owner + "." + objectName);
			stLog.execute("drop " + rs.getString("OBJECT_TYPE") + " " + nm.getSchema() + "." + nm.getName());
		}

		rs.close();
		st.close();
	}

	@Override
	public boolean isExists(String owner, String objectName) throws SQLException {
		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery("select count(*) cnt from all_objects where owner = '" + owner + "' and object_name = '" + objectName + "' and OBJECT_TYPE not like 'PACKAGE BODY'");
		
		rs.next();
		return rs.getInt("cnt") > 0;
	}
	
	private String replaceNames(String ddl, String schema, String objectName, StatementLogging stLog) throws Exception {
		
		dropIfExists(isSaveToSchema() ? PREFIX + schema : schema, 
				isSaveToSchema() ? objectName : PREFIX + objectName, stLog);
		
		ddl = isSaveToSchema() ? ddl.replace("\"" + schema + "\"", "\"" + PREFIX + schema + "\"") : ddl.replace("\"" + objectName + "\"", "\"" + PREFIX + objectName + "\"");
		
		return ddl;
	}
	
	private String getFullDbName(String schema, String objectName) {
		if (isSaveToSchema())
			return PREFIX + schema + "." + objectName;
		else
			return schema + "." + PREFIX + objectName;
	}

	@Override
	public boolean createSchema(StatementLogging stLog, String schema){
		
		try {
			Statement st = 	adapter.getConnection().createStatement();
			ResultSet rs = st.executeQuery("select count(*) cnt from all_users where USERNAME = '" + PREFIX + schema + "'");
			rs.next();
			if (rs.getInt("cnt") == 0) {
				ConsoleWriter.detailsPrintLn(lang.getValue("general", "backup", "creatingSchema").withParams(PREFIX + schema));
				stLog.execute("create USER \"" + PREFIX + schema + "\"\r\n" + 
						"IDENTIFIED BY \"" + PREFIX + schema + "\"\r\n" + 
						"DEFAULT TABLESPACE \"SYSTEM\"\r\n" + 
						"TEMPORARY TABLESPACE \"TEMP\"\r\n" + 
						"ACCOUNT UNLOCK");
				
				stLog.execute("ALTER USER \"" + PREFIX + schema + "\" QUOTA UNLIMITED ON SYSTEM");
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
