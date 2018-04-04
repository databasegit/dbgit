package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBUser;

public class MetaUser extends MetaObjOptions {
	public MetaUser() {
		super();
	}
	
	public MetaUser(DBUser user) {
		super(user);
	}
	
	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitUser;
	}
	
	@Override
	public void loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		Map<String, DBUser> users = adapter.getUsers();
		//find by name 
		
		

	}
}
