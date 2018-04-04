package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBRole;

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
	public void loadFromDB() {
		// load data shema by name

	}

}
