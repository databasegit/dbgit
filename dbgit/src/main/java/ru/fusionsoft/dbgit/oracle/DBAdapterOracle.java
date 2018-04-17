package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
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

public class DBAdapterOracle extends DBAdapter {

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
	public void restoreDataBase(IMapMetaObject updateObjs) {
		
	}

	@Override
	public void endUpdateDB() {
		// TODO Auto-generated method stub

	}
	
	@Override
	public IMapMetaObject loadCustomMetaObjects() {
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
	
	public Map<String, DBTrigger> getTriggers(String schema) {
		// TODO Auto-generated method stub
				return null;
	}
	
	public DBTrigger getTrigger(String schema, String name) {
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
	public DBTableData getTableData(String schema, String nameTable, int paramFetch) {
		// TODO Auto-generated method stub
		return null;
	}
/*
	@Override
	public DBTableRow getTableRow(String schema, String nameTable, Object id) {
		// TODO Auto-generated method stub
		return null;
	}
*/
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

}
