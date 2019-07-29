package ru.fusionsoft.dbgit.oracle;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import ru.fusionsoft.dbgit.adapters.DBBackupAdapter;
import ru.fusionsoft.dbgit.core.DBGitPath;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.meta.MetaSql;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.statement.StatementLogging;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;

public class DBBackupAdapterOracle extends DBBackupAdapter {
	
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
				ConsoleWriter.detailsPrint("Trying to copy " + objectName + " to " + getFullDbName(schema, objectName) + "...", 1);
				
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
		
				ConsoleWriter.detailsPrintlnGreen("OK");
			} else if (obj instanceof MetaTable) {
				MetaTable metaTable = (MetaTable) obj;
				metaTable.loadFromDB();
				String objectName = metaTable.getTable().getName();
				String schema = metaTable.getTable().getSchema();				
				
				ConsoleWriter.detailsPrint("Trying to copy " + objectName + " to " +  getFullDbName(schema, objectName) + "...", 1);
				
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
				
				ConsoleWriter.detailsPrintlnGreen("OK");
			} else if (obj instanceof MetaSequence) {
				MetaSequence metaSequence = (MetaSequence) obj;
				metaSequence.loadFromDB();

				String objectName = metaSequence.getSequence().getName();
				String schema = metaSequence.getSequence().getSchema();

				String ddl = metaSequence.getSequence().getOptions().get("ddl").toString();
				ConsoleWriter.detailsPrint("Trying to copy " + objectName + " to " +  getFullDbName(schema, objectName) + "...", 1);
				
				ddl = replaceNames(ddl, schema, objectName, stLog);

				stLog.execute(ddl);
				File file = new File(DBGitPath.getFullPath() + metaSequence.getFileName());
				
				if (file.exists())
					obj = metaSequence.loadFromFile();
				
				ConsoleWriter.detailsPrintlnGreen("OK");
			}

		} catch (Exception e) {
			ConsoleWriter.detailsPrintlnRed("FAIL");
			throw new ExceptionDBGitRestore("Error on backup " + obj.getName(), e);
		} finally {
			stLog.close();
		}	
		return obj;
	}

	@Override
	public void restoreDBObject(IMetaObject obj) throws Exception {
		ConsoleWriter.detailsPrintLn("Restoring from backup!");
		
		/**
		 *TODO:
		 * 1) get object ddl
		 * 2) removing "BACKUP$" from names
		 * 3) execute ddl
		 */

	}
	
	private void dropIfExists(String owner, String objectName, StatementLogging stLog) throws Exception {		
		Statement st = 	adapter.getConnection().createStatement();
		ResultSet rs = st.executeQuery("select * from all_objects where owner = '" + owner + "' and object_name = '" + objectName + "' and OBJECT_TYPE not like 'PACKAGE BODY'");
		
		while (rs.next()) {
			//ConsoleWriter.detailsPrintLn("Dropping " + owner + "." + objectName);
			stLog.execute("drop " + rs.getString("OBJECT_TYPE") + " " + owner + "." + objectName);
		}
		
		rs.close();
		st.close();
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
				ConsoleWriter.detailsPrintLn("Creating schema " + PREFIX + schema);
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
			ConsoleWriter.println("Cannot create schema: " + e.getLocalizedMessage());
			return false;
		}
		
		
		
	}

}
