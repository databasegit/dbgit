package ru.fusionsoft.dbgit.adapters;

import java.io.OutputStream;
import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.core.SchemaSynonym;
import ru.fusionsoft.dbgit.core.db.DbType;
import ru.fusionsoft.dbgit.dbobjects.DBConstraint;
import ru.fusionsoft.dbgit.dbobjects.DBDomain;
import ru.fusionsoft.dbgit.dbobjects.DBEnum;
import ru.fusionsoft.dbgit.dbobjects.DBFunction;
import ru.fusionsoft.dbgit.dbobjects.DBIndex;
import ru.fusionsoft.dbgit.dbobjects.DBPackage;
import ru.fusionsoft.dbgit.dbobjects.DBProcedure;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;
import ru.fusionsoft.dbgit.dbobjects.DBSchemaObject;
import ru.fusionsoft.dbgit.dbobjects.DBSequence;
import ru.fusionsoft.dbgit.dbobjects.DBTable;
import ru.fusionsoft.dbgit.dbobjects.DBTableData;
import ru.fusionsoft.dbgit.dbobjects.DBTableField;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;
import ru.fusionsoft.dbgit.dbobjects.DBTrigger;
import ru.fusionsoft.dbgit.dbobjects.DBUser;
import ru.fusionsoft.dbgit.dbobjects.DBUserDefinedType;
import ru.fusionsoft.dbgit.dbobjects.DBView;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;

public class DBAdapterProxy implements IDBAdapter {
	private IDBAdapter adapter;
	private SchemaSynonym ss;	
	

	public DBAdapterProxy(IDBAdapter adapter) throws ExceptionDBGit {
		super();
		this.adapter = adapter;
		
		ss = SchemaSynonym.getInstance();
	}

	public void setConnection(Connection conn) {
		adapter.setConnection(conn);
	}

	public Connection getConnection() {
		return adapter.getConnection();
	}

	public void registryMappingTypes() {
		adapter.registryMappingTypes();
	}

	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		return adapter.getFactoryRestore();
	}

	public IFactoryDBBackupAdapter getBackupFactory() {
		return adapter.getBackupAdapterFactory();
	}

	public void setDumpSqlCommand(OutputStream stream, Boolean isExecSql) {
		adapter.setDumpSqlCommand(stream, isExecSql);
	}

	public OutputStream getStreamOutputSqlCommand() {
		return adapter.getStreamOutputSqlCommand();
	}

	public Boolean isExecSql() {
		return adapter.isExecSql();
	}

	public void startUpdateDB() {
		adapter.startUpdateDB();
	}

	public void restoreDataBase(IMapMetaObject updateObjs) throws Exception {
		//TODO replace IMapMetaObject
		adapter.restoreDataBase(updateObjs);
	}

	public void deleteDataBase(IMapMetaObject delObjs) throws Exception {
		deleteDataBase(delObjs, false);
	}

	public void deleteDataBase(IMapMetaObject delObjs, boolean isDeleteFromIndex) throws Exception {
		//TODO replace IMapMetaObject
		adapter.deleteDataBase(delObjs, isDeleteFromIndex);
	}
	/*
	protected IMapMetaObject mapSynonymSchema(IMapMetaObject objs) {
		
	}
	 */
	public void endUpdateDB() {
		adapter.endUpdateDB();
	}

	public IMapMetaObject loadCustomMetaObjects() {
		return adapter.loadCustomMetaObjects();
	}

	public Map<String, DBSchema> getSchemes() {
		return adapter.getSchemes();
	}

	public Map<String, DBTableSpace> getTableSpaces() {
		return adapter.getTableSpaces();
	}

	public Map<String, DBSequence> getSequences(String schema) {
		return schemaSynonymMap(adapter.getSequences(getSchemaMap(schema)));
	}

	public DBSequence getSequence(String schema, String name) {
		return schemaSynonymMap(adapter.getSequence(getSchemaMap(schema), name));
	}

	public Map<String, DBTable> getTables(String schema) {
		return schemaSynonymMap(adapter.getTables(getSchemaMap(schema)));
	}

	public DBTable getTable(String schema, String name) {
		return schemaSynonymMap(adapter.getTable(getSchemaMap(schema), name));
	}

	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		return adapter.getTableFields(getSchemaMap(schema), nameTable);
	}

	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		return schemaSynonymMap(adapter.getIndexes(getSchemaMap(schema), nameTable));
	}

	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		return schemaSynonymMap(adapter.getConstraints(getSchemaMap(schema), nameTable));
	}

	public Map<String, DBView> getViews(String schema) {
		return schemaSynonymMap(adapter.getViews(getSchemaMap(schema)));
	}

	public DBView getView(String schema, String name) {
		return schemaSynonymMap(adapter.getView(getSchemaMap(schema), name));
	}

	public Map<String, DBPackage> getPackages(String schema) {
		return schemaSynonymMap(adapter.getPackages(getSchemaMap(schema)));
	}

	public DBPackage getPackage(String schema, String name) {
		return schemaSynonymMap(adapter.getPackage(getSchemaMap(schema), name));
	}

	public Map<String, DBProcedure> getProcedures(String schema) {
		return schemaSynonymMap(adapter.getProcedures(getSchemaMap(schema)));
	}

	public DBProcedure getProcedure(String schema, String name) {
		return schemaSynonymMap(adapter.getProcedure(getSchemaMap(schema), name));
	}

	public Map<String, DBFunction> getFunctions(String schema) {
		return schemaSynonymMap(adapter.getFunctions(getSchemaMap(schema)));
	}

	public DBFunction getFunction(String schema, String name) {
		return schemaSynonymMap(adapter.getFunction(getSchemaMap(schema), name));
	}

	public Map<String, DBTrigger> getTriggers(String schema) {
		return schemaSynonymMap(adapter.getTriggers(getSchemaMap(schema)));
	}

	public DBTrigger getTrigger(String schema, String name) {
		return schemaSynonymMap(adapter.getTrigger(getSchemaMap(schema), name));
	}

	public DBTableData getTableData(String schema, String nameTable) {
		return adapter.getTableData(getSchemaMap(schema), nameTable);
	}

	public Map<String, DBUser> getUsers() {
		return adapter.getUsers();
	}

	public Map<String, DBRole> getRoles() {
		return adapter.getRoles();
	}

	@Override
	public Map<String, DBUserDefinedType> getUDTs(String schema) {
		return adapter.getUDTs(schema);
	}

	@Override
	public Map<String, DBDomain> getDomains(String schema) {
		return adapter.getDomains(schema);
	}

	@Override
	public Map<String, DBEnum> getEnums(String schema) {
		return adapter.getEnums(schema);
	}

	@Override
	public DBUserDefinedType getUDT(String schema, String name) {
		return adapter.getUDT(schema, name);
	}

	@Override
	public DBDomain getDomain(String schema, String name) {
		return adapter.getDomain(schema, name);
	}

	@Override
	public DBEnum getEnum(String schema, String name) {
		return adapter.getEnum(schema, name);
	}

	public <T extends DBSchemaObject> T schemaSynonymMap(T object) {
		if (object == null) 
			return null;
		object.setSchema(ss.getSynonymNvl(object.getSchema()));
		return object;
	} 
	
	public <T extends DBSchemaObject> Map<String, T> schemaSynonymMap(Map<String, T> objects) {
		if (objects == null) 
			return null;
		for (DBSchemaObject obj : objects.values()) {
			schemaSynonymMap(obj);
		} 		
		return objects;
	} 
	
	protected String getSchemaMap(String schema) {
		return ss.getSchemaNvl(schema);
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() {
		return false;
	}

	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		return adapter.getBackupAdapterFactory();
	}

	@Override
	public DbType getDbType() {
		return adapter.getDbType();
	}
	
	@Override
	public String getDbVersion() {
		return adapter.getDbVersion();
	}

	@Override
	public Double getDbVersionNumber() {
		return adapter.getDbVersionNumber();
	}

	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		return adapter.getConvertAdapterFactory();
	}

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		adapter.createSchemaIfNeed(schemaName);
		
	}

	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		adapter.createRoleIfNeed(roleName);
	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		return adapter.getDefaultScheme();
	}

	@Override
	public boolean isReservedWord(String word) {
		return adapter.isReservedWord(word);
	}

	@Override
	public String escapeNameIfNeeded(String name) {
		return adapter.escapeNameIfNeeded(name);
	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		return adapter.getTableDataPortion(schema, nameTable, portionIndex, tryNumber);
	}
}
