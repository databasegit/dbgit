package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Map;

import javax.xml.validation.meta.IMapMetaObject;

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
import ru.fusionsoft.dbgit.dbobjects.DBUser;
import ru.fusionsoft.dbgit.dbobjects.DBView;

public interface IDBAdapter {
	public void setConnection(Connection conn);
	public Connection getConnection();
	
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore();
	
	public void setDumpSqlCommand(OutputStream stream, Boolean isExec);
	
	public void startUpdateDB();
	public void restoreDataBase(IMapMetaObject updateObjs);
	public void endUpdateDB();
	
	public Map<String, DBSchema> getSchemes();
	
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
