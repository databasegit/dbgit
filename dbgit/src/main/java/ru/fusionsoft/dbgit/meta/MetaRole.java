package ru.fusionsoft.dbgit.meta;

import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBRole;
import ru.fusionsoft.dbgit.dbobjects.DBSchema;

public class MetaRole extends MetaObjOptions {
	public MetaRole() {
		super();
	}
	
	public MetaRole(DBRole role) {
		super(role);
	}
	
	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitRole;
	}
	
	@Override
	public void loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		Map<String, DBRole> roles = adapter.getRoles();
		
		setObjectOptionFromMap(roles);
	}

}
