package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBProcedure;

public class MetaProcedure extends MetaSql {
	public MetaProcedure() {
		super();
	}
	
	public MetaProcedure(DBProcedure pr) {
		super(pr);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitProcedure;
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}

}
