package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBFuntion;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBPakage;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableData;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.dbobjects.DBTableRow;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
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
	public void setConnection(Connection conn);
	public Connection getConnection();
	
	/**
	 * 
	 * @return Factory Adapter for create restore adapter
	 */
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore();
	
	/**
	 * 
	 * @param stream Stream for output sql command
	 * @param isExec - Execute action sql command or only write command to stream
	 */
	public void setDumpSqlCommand(OutputStream stream, Boolean isExec);
	
	/**
	 * Run before start update databse
	 */
	public void startUpdateDB();
	
	/**
	 * Restore Meta Objects to DataBase
	 * @param updateObjs
	 */
	public void restoreDataBase(IMapMetaObject updateObjs);
	
	/**
	 * Run after end restore database
	 */
	public void endUpdateDB();
	
	/**
	 * Load from database custom objects
	 * @return custom meta objects
	 */
	public Map<String, IMapMetaObject> loadCustomMetaObjects();
	
	public Map<String, DBSchema> getSchemes();
	
	public Map<String, DBTableSpace> getTableSpaces();
	
	public Map<String, DBSequence> getSequences(DBSchema schema);
	public DBSequence getSequence(DBSchema schema, String name);
	
	public Map<String, DBTable> getTables(DBSchema schema);
	public DBTable getTable(DBSchema schema, String name);
	
	public Map<String, DBTableField> getTableFields(DBTable tbl);	
	public Map<String, DBIndex> getIndexes(DBTable tbl);	
	public Map<String, DBConstraint> getConstraints(DBTable tbl);
	
	public Map<String, DBView> getViews(DBSchema schema);
	public DBView getView(DBSchema schema, String name);
	
	public Map<String, DBPakage> getPackages(DBSchema schema);
	public DBPakage getPackage(DBSchema schema, String name);
	
	public Map<String, DBProcedure> getProcedures(DBSchema schema);
	public DBProcedure getProcedure(DBSchema schema, String name);
	
	public Map<String, DBFuntion> getFunctions(DBSchema schema);
	public DBFuntion getFunction(DBSchema schema, String name);
	
	public DBTableData getTableData(DBTable tbl);
	public DBTableRow getTableRow(DBTable tbl, Object id); //TODO multi id
	
	public Map<String, DBUser> getUsers();
	public Map<String, DBRole> getRoles();
	
	/*Если будет нужно - сюда можно добавить подписчиков на события*/
}
