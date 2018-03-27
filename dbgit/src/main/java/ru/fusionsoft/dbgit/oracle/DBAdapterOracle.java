package ru.fusionsoft.dbgit.oracle;

import java.sql.Connection;
import java.util.Map;

import javax.xml.validation.meta.IMapMetaObject;

import ru.fusionsoft.dbgit.adapters.DBAdapter;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.adapters.IFactoryDBAdapterRestoteMetaData;
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
	public Map<String, DBSchema> getSchemes() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBSequence> getSequences(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBSequence getSequence(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTable> getTables(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTable getTable(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBTableField> getTableFields(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBIndex> getIndexes(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBConstraint> getConstraints(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBView> getViews(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBView getView(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBPakage> getPackages(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBPakage getPackage(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBProcedure> getProcedures(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBProcedure getProcedure(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Map<String, DBFuntion> getFunctions(DBSchema schema) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBFuntion getFunction(DBSchema schema, String name) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableData getTableData(DBTable tbl) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DBTableRow getTableRow(DBTable tbl, Object id) {
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

}
