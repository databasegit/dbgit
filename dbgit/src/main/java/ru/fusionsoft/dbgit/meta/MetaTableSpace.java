package ru.fusionsoft.dbgit.meta;

import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBTableSpace;

public class MetaTableSpace extends MetaObjOptions {
	public MetaTableSpace() {
		super();
	}
	
	public MetaTableSpace(DBTableSpace sp) {
		super(sp);
	}
	
	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitTableSpace;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		Map<String, DBTableSpace> tbs = adapter.getTableSpaces();
		
		setObjectOptionFromMap(tbs);
		return true;
	}

}
