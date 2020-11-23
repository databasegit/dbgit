package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableData;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.dbobjects.DBTableRow;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.dbobjects.DBUser;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;

/**
 * <div class="en">The adapter hides the differences between different databases.</div>
 * <div class="ru">Адаптер скрывающий различия между разными БД.</div>
 * 
 * @author mikle
 *
 *
 */
public interface IDBAdapter {
	public static final int MAX_ROW_COUNT_FETCH = 10000;
	public static final int LIMIT_FETCH = 1;
	public static final int NOLIMIT_FETCH = 2;
	
	public void setConnection(Connection conn);
	
	public Connection getConnection();
	
	/**
	 * Adapter must registy typing for map data table.
	 * See class FactoryCellData
	 */
	public void registryMappingTypes();
	
	/**
	 * 
	 * @return Factory Adapter for create restore adapter
	 */
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore();
	
	/**
	 * 
	 * @param stream Stream for output sql command
	 * @param isExecSql - Execute action sql command or only write command to stream
	 */
	public void setDumpSqlCommand(OutputStream stream, Boolean isExecSql);
	
	/**
	 * Get stream for output sql command for logging
	 * @return
	 */
	public OutputStream getStreamOutputSqlCommand();
	
	/**
	 * Get state execute sql command for restore DB
	 * @return
	 */
	public Boolean isExecSql();
	
	/**
	 * Run before start update databse
	 */
	public void startUpdateDB();
	
	/**
	 * Restore Meta Objects to DataBase
	 * @param updateObjs
	 */
	public void restoreDataBase(IMapMetaObject updateObjs) throws Exception;
	
	/**
	 * delete map DB objects
	 * @param updateObjs
	 */
	public void deleteDataBase(IMapMetaObject updateObjs) throws Exception;

	/**
	 * delete map DB objects and removes from dbindex if specified
	 * @param updateObjs
	 * @param isDeleteFromIndex
	 */
	public void deleteDataBase(IMapMetaObject updateObjs, boolean isDeleteFromIndex) throws Exception;
	
	/**
	 * Run after end restore database
	 */
	public void endUpdateDB();
	
	/**
	 * Load from database custom objects
	 * @return custom meta objects
	 */
	public IMapMetaObject loadCustomMetaObjects();
	
	public Map<String, DBSchema> getSchemes();
	
	public Map<String, DBTableSpace> getTableSpaces();
	
	public Map<String, DBSequence> getSequences(String schema);
	public DBSequence getSequence(String schema, String name);
	
	public Map<String, DBTable> getTables(String schema);
	public DBTable getTable(String schema, String name);
	
	public Map<String, DBTableField> getTableFields(String schema, String nameTable);	
	public Map<String, DBIndex> getIndexes(String schema, String nameTable);	
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable);
	
	public Map<String, DBView> getViews(String schema);
	public DBView getView(String schema, String name);
	
	public Map<String, DBPackage> getPackages(String schema);
	public DBPackage getPackage(String schema, String name);
	
	public Map<String, DBProcedure> getProcedures(String schema);
	public DBProcedure getProcedure(String schema, String name);
	
	public Map<String, DBFunction> getFunctions(String schema);
	public DBFunction getFunction(String schema, String name);
	
	public Map<String, DBTrigger> getTriggers(String schema);
	public DBTrigger getTrigger(String schema, String name);
	
	public DBTableData getTableData(String schema, String nameTable);
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber);
	//public DBTableRow getTableRow(DBTable tbl, Object id); //TODO multi id
	
	public Map<String, DBUser> getUsers();
	public Map<String, DBRole> getRoles();
	
	public boolean userHasRightsToGetDdlOfOtherUsers();

	public IFactoryDBBackupAdapter getBackupAdapterFactory();
	public IFactoryDBConvertAdapter getConvertAdapterFactory();
	
	public DbType getDbType();
	public String getDbVersion();
	public Double getDbVersionNumber();
	
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit;
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit;
	
	public String getDefaultScheme() throws ExceptionDBGit;

	boolean isReservedWord(String word);

	public String escapeNameIfNeeded(String name);

	/*Если будет нужно - сюда можно добавить подписчиков на события*/
}
