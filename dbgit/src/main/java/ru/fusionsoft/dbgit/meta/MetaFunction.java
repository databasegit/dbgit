package ru.fusionsoft.dbgit.meta;

import ru.fusionsoft.dbgit.dbobjects.DBFunction;

public class MetaFunction extends MetaSql {
	public MetaFunction() {
		super();
	}
	
	public MetaFunction(DBFunction fun) {
		super(fun);
	}
	
	@Override
	public DBGitMetaType getType() {
		return DBGitMetaType.DbGitFunction;
	}
	
	@Override
	public void loadFromDB() {
		// load data shema by name

	}

}
