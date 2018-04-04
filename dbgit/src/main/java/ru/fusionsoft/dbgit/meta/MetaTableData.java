package ru.fusionsoft.dbgit.meta;

import java.io.InputStream;
import java.io.OutputStream;

import ru.fusionsoft.dbgit.dbobjects.DBTable;

/**
 * Meta class for Table data
 * @author mikle
 *
 */public class MetaTableData extends MetaBase {
	 protected DBTable table;
	 

	 public MetaTableData() {}
	 
	public MetaTableData(DBTable table) {
		setTable(table);
	}
	
		
	public DBTable getTable() {
		return table;
	}

	public void setTable(DBTable table) {
		this.table = table;
	}

	@Override
	public DBGitMetaType getType() {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public void serialize(OutputStream stream) {
		// TODO Auto-generated method stub

	}

	@Override
	public IMetaObject deSerialize(InputStream stream) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void loadFromDB() {
		// TODO Auto-generated method stub

	}


	@Override
	public String getHash() {
		// TODO Auto-generated method stub
		return null;
	}

}
