package ru.fusionsoft.dbgit.mssql;

import org.slf4j.Logger;
import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
import ru.fusionsoft.dbgit.adapters.IFactoryDBBackupAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBConvertAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.*;
import ru.fusionsoft.dbgit.meta.IMapMetaObject;
import ru.fusionsoft.dbgit.utils.LoggerUtil;

import java.util.Map;


public class DBAdapterMssql extends DBAdapter {

	//Stubs for MSSQL adapter, marked as "TODO Auto-generated method stub"
	//And some unfinished implementations marked as "TODO MSSQL *"

	private Logger logger = LoggerUtil.getLogger(this.getClass());
	private FactoryDBAdapterRestoreMssql restoreFactory = new FactoryDBAdapterRestoreMssql();
	private FactoryDbConvertAdapterMssql convertFactory = new FactoryDbConvertAdapterMssql();
	private FactoryDBBackupAdapterMssql backupFactory = new FactoryDBBackupAdapterMssql();

	@Override
	public void registryMappingTypes() {
		// TODO Auto-generated method stub
	}

	@Override
	public IFactoryDBAdapterRestoteMetaData getFactoryRestore() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void startUpdateDB() {
		// TODO Auto-generated method stub
	}

	@Override
	public void endUpdateDB() {
		// TODO Auto-generated method stub
	}

	@Override
	public IMapMetaObject loadCustomMetaObjects() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBSchema> getSchemes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTableSpace> getTableSpaces() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBSequence> getSequences(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBSequence getSequence(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTable> getTables(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTable getTable(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTableField> getTableFields(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBIndex> getIndexes(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBView> getViews(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBView getView(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBPackage> getPackages(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBPackage getPackage(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBProcedure getProcedure(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBFunction> getFunctions(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBFunction getFunction(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTrigger> getTriggers(String schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTrigger getTrigger(String schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableData getTableData(String schema, String nameTable) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableData getTableDataPortion(String schema, String nameTable, int portionIndex, int tryNumber) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBUser> getUsers() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBRole> getRoles() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean userHasRightsToGetDdlOfOtherUsers() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public IFactoryDBBackupAdapter getBackupAdapterFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public IFactoryDBConvertAdapter getConvertAdapterFactory() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDbType() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDbVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void createSchemaIfNeed(String schemaName) throws ExceptionDBGit {
		// TODO Auto-generated method stub
	}

	@Override
	public void createRoleIfNeed(String roleName) throws ExceptionDBGit {
		// TODO Auto-generated method stub
	}

	@Override
	public String getDefaultScheme() throws ExceptionDBGit {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isReservedWord(String word) {
		// TODO Auto-generated method stub
		return false;
	}
}
