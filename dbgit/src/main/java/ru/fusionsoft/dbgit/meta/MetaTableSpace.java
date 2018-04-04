package ru.fusionsoft.dbgit.meta;

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
	public void loadFromDB() {
		// load data shema by name

	}

}
