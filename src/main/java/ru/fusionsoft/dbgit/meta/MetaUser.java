package ru.fusionsoft.dbgit.meta;

import java.io.IOException;
import java.util.Map;

import ru.fusionsoft.dbgit.adapters.AdapterFactory;
import ru.fusionsoft.dbgit.adapters.IDBAdapter;
import ru.fusionsoft.dbgit.core.ExceptionDBGit;
import ru.fusionsoft.dbgit.dbobjects.DBUser;

public class MetaUser extends MetaObjOptions {
	
	private String passwordHash;
	
	public MetaUser() {
		super();
	}
	
	public MetaUser(DBUser user) throws ExceptionDBGit {
		super(user);
	}
	
	@Override
	public DBGitMetaType getType() {		
		return DBGitMetaType.DBGitUser;
	}
	
	@Override
	public boolean loadFromDB() throws ExceptionDBGit {
		IDBAdapter adapter = AdapterFactory.createAdapter();
		Map<String, DBUser> users = adapter.getUsers();
		
		setObjectOptionFromMap(users);
		return true;
	}

	public String getPasswordHash() {
		return passwordHash;
	}

	public void setPasswordHash(String passwordHash) {
		this.passwordHash = passwordHash;
	}
		
}
