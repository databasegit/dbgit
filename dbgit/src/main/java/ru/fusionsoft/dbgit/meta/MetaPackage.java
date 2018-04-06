package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBPackage;

public class MetaPackage extends MetaSql {
	public MetaPackage() {
		super();
	}
	
	public MetaPackage(DBPackage pac) {
		super(pac);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitPackage;
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}

}
