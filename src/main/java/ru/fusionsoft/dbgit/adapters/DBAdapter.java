package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

import com.axiomalaska.jdbc.NamedParameterPreparedStatement;

import ru.fusionsoft.dbgit.core.DBGitConfig;
import ru.fusionsoft.dbgit.core.DBGitLang;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRestore;
import ru.fusionsoft.dbgit.core.ExceptionDBGitRunTime;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.meta.DBGitMetaType;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.meta.IMetaObject;
import ru.fusionsoft.dbgit.meta.MetaSequence;
import ru.fusionsoft.dbgit.meta.MetaSql;
import ru.fusionsoft.dbgit.meta.MetaTable;
import ru.fusionsoft.dbgit.meta.MetaTableData;
import ru.fusionsoft.dbgit.meta.TreeMapMetaObject;
import ru.fusionsoft.dbgit.utils.ConsoleWriter;
import ru.fusionsoft.dbgit.utils.StringProperties;

/**
 * <div class="en">The base adapter adapter class. Contains general solutions independent of a particular database</div>
 * <div class="ru">Базовый класс адаптера БД. Содержит общие решения, независимые от конкретной БД</div>
 * 
 * @author mikle
 *
 */
public abstract class DBAdapter implements IDBAdapter {
	protected Connection connect;
	protected Boolean isExec = true;
	protected OutputStream streamSql = null;	
	protected DBGitLang lang = DBGitLang.getInstance();
	
	@Override
	public void setConnection(Connection conn) {
		connect = conn;
	}
	
	@Override
	public Connection getConnection() {
		return connect;
	} 
	
	@Override
	public void setDumpSqlCommand(OutputStream stream, Boolean isExec) {
		this.streamSql = stream;
		this.isExec = isExec;
	}
	
	@Override
	public OutputStream getStreamOutputSqlCommand() {
		return streamSql;
	}
	
	@Override
	public Boolean isExecSql() {
		return isExec;
	}
	
	@Override
	public void restoreDataBase(IMapMetaObject updateObjs) throws Exception {
		Connection connect = getConnection();
		IMapMetaObject currStep = updateObjs;
		
		DBGitLang lang = DBGitLang.getInstance();
		
		try {
			List<MetaTable> tables = new ArrayList<MetaTable>();			
			List<MetaTableData> tablesData = new ArrayList<MetaTableData>();
			
			List<String> createdSchemas = new ArrayList<String>();
			List<String> createdRoles = new ArrayList<String>();
			
			Comparator<IMetaObject> comparator = new Comparator<IMetaObject>() {
				public int compare(IMetaObject o1, IMetaObject o2) {
				    if (o1 instanceof MetaTable) 
				    	return -1;
				    else if (o1 instanceof MetaTableData)
				    	return 1;
				    else
				    	return 0;
				}
			};
			
			for (IMetaObject obj : updateObjs.values().stream().sorted(comparator).collect(Collectors.toList())) {
				Integer step = 0;
				
				if (step == 0 && DBGitConfig.getInstance().getBoolean("core", "TO_MAKE_BACKUP", true)) {
					obj = getBackupAdapterFactory().getBackupAdapter(this).backupDBObject(obj);
				}
				
				String schemaName = getSchemaName(obj);
				if (schemaName != null)
					schemaName = (SchemaSynonym.getInstance().getSchema(schemaName) == null) ? schemaName : SchemaSynonym.getInstance().getSchema(schemaName);
				
				boolean res = false;
				Timestamp timestampBefore = new Timestamp(System.currentTimeMillis());

				if (step == 0) {
					IDBConvertAdapter convertAdapter = getConvertAdapterFactory().getConvertAdapter(obj.getType().getValue());
					
					boolean isContainsNative = false;
					if (obj instanceof MetaTable) {						
						MetaTable table = (MetaTable) obj;		
						
						for (DBTableField field : table.getFields().values()) {
							if (field.getTypeUniversal().equals("native")) {
								isContainsNative = true;
								break;
							}
						}
					}
					
					if (isContainsNative) {
						ConsoleWriter.println("Table " + obj.getName() + " contains unsupported types, it will be skipped");
						continue;
					}
					
					if (convertAdapter != null) {
						if (!createdSchemas.contains(schemaName) && schemaName != null) {
							createSchemaIfNeed(schemaName);
							createdSchemas.add(schemaName);
						}
						
						String ownerName = getOwnerName(obj);
						if (!getRoles().containsKey(ownerName) && !createdRoles.contains(ownerName) && ownerName != null) {
							createRoleIfNeed(ownerName);
							createdRoles.add(ownerName);
						}					

						obj = convertAdapter.convert(getDbType(), getDbVersion(), obj);							
					}
				}
				
				while (!res) {						
					if (obj.getDbType() == null) {
						ConsoleWriter.println(lang.getValue("errors", "emptyDbType"));
						break;
					}
					
					if (getFactoryRestore().getAdapterRestore(obj.getType(), this) == null ||
							!obj.getDbType().equals(getDbType()))
						break;					
					
					if (!createdSchemas.contains(schemaName) && schemaName != null) {
						createSchemaIfNeed(schemaName);
						createdSchemas.add(schemaName);
					}
					
					String ownerName = getOwnerName(obj);
					if (!getRoles().containsKey(ownerName) && !createdRoles.contains(ownerName) && ownerName != null) {
						createRoleIfNeed(ownerName);
						createdRoles.add(ownerName);
					}	
					
					if (obj instanceof MetaTable) {
						MetaTable table = (MetaTable) obj;
						if (!tables.contains(table))
							tables.add(table);
					}
					
					if (obj instanceof MetaTableData) {
						MetaTableData tableData = (MetaTableData) obj;
						if (!tables.contains(tableData))
							tablesData.add(tableData);
					}
					
					res = getFactoryRestore().getAdapterRestore(obj.getType(), this).restoreMetaObject(obj, step);
					step++;

					if (step > 100) {
						throw new Exception(lang.getValue("errors", "restore", "restoreErrorDidNotReturnTrue").toString());
					}
				}
    			Timestamp timestampAfter = new Timestamp(System.currentTimeMillis());
    			Long diff = timestampAfter.getTime() - timestampBefore.getTime();
    			ConsoleWriter.println("(" + diff + " " + lang.getValue("general", "add", "ms") +")");
			}
			
			for (MetaTable table : tables) {
				getFactoryRestore().getAdapterRestore(DBGitMetaType.DBGitTable, this).restoreMetaObject(table, -1);
			}
/*
			for (MetaTableData tableData : tablesData) {
				getFactoryRestore().getAdapterRestore(DBGitMetaType.DbGitTableData, this).restoreMetaObject(tableData, -2);
			}
*/
			connect.commit();
		} catch (Exception e) {
			connect.rollback();
			throw new ExceptionDBGitRestore(lang.getValue("errors", "restore", "restoreError").toString(), e);
		} finally {
			//connect.setAutoCommit(false);
		} 
		
	}
	
	@Override
	public void deleteDataBase(IMapMetaObject deleteObjs)  throws Exception {
		Connection connect = getConnection();
		try {
			//start transaction
			for (IMetaObject obj : deleteObjs.values()) {
				if (DBGitConfig.getInstance().getBoolean("core", "TO_MAKE_BACKUP", true)) 
					obj = getBackupAdapterFactory().getBackupAdapter(this).backupDBObject(obj);

				getFactoryRestore().getAdapterRestore(obj.getType(), this).removeMetaObject(obj);
			}
			connect.commit();
		} catch (Exception e) {
			connect.rollback();
			throw new ExceptionDBGitRestore(DBGitLang.getInstance().getValue("errors", "restore", "removeError").toString(), e);
		} finally {
			//connect.setAutoCommit(false);
		} 

	}
	
	public String cleanString(String str) {
		String dt = str.replace("\r\n", "\n");
		while (dt.contains(" \n")) dt = dt.replace(" \n", "\n");
		dt = dt.replace("\t", "   ").trim();
		
		return dt;
	}
	
	public void rowToProperties(ResultSet rs, StringProperties properties) {
		try {
			for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
				if (rs.getString(i) == null) continue ;			
				
				properties.addChild(rs.getMetaData().getColumnName(i).toLowerCase(), cleanString(rs.getString(i)));
			}
		} catch(Exception e) {
			throw new ExceptionDBGitRunTime(e);
		}
	}
	
	private String getSchemaName(IMetaObject obj) {
		if (obj instanceof MetaSql)
			return ((MetaSql) obj).getSqlObject().getSchema();
		else if (obj instanceof MetaTable)
			return ((MetaTable) obj).getTable().getSchema();
		else if (obj instanceof MetaSequence)
			return ((MetaSequence) obj).getSequence().getSchema();
		else return null;
	}
	
	private String getOwnerName(IMetaObject obj) {
		if (obj instanceof MetaSql)
			return ((MetaSql) obj).getSqlObject().getOwner();
		else if (obj instanceof MetaTable)
			return ((MetaTable) obj).getTable().getOptions().get("owner").getData();
		else if (obj instanceof MetaSequence)
			return ((MetaSequence) obj).getSequence().getOptions().get("owner").getData();
		else return null;		
	}
}
